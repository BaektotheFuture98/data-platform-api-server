from airflow.sdk import task, dag
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timezone # 💡 days_ago 대신 datetime과 timezone 사용
import json

# ==========================================
# 1. Helper Functions (설정 및 스키마 생성)
# ==========================================

def _make_avro_schema(project_name: str, fields: list) -> str:
    """Avro 스키마 JSON 문자열 생성"""
    avro_fields = []
    for field_name in fields:
        field_type = "int" if "date" in field_name or "id" in field_name else "string"
        avro_fields.append({"name": field_name, "type": ["null", field_type], "default": None})

    data_schema = {
        "type": "record",
        "name": project_name,
        "namespace": "com.pipeline.dynamic",
        "fields": avro_fields
    }
    return json.dumps(data_schema, ensure_ascii=False)

def _connect_config(param: dict, chunk_index: int) -> dict:
    """청크(테이블) 별 커넥터 설정 생성"""
    schema_registry_url = Variable.get("SCHEMA_REGISTRY_URL")
    
    suffix = f"-{chunk_index}"
    topic_name = f"{param['project_name']}-topic{suffix}"
    target_table_name = f"{param['table']}_{chunk_index}" 
    connector_name = f"{param['project_name']}-SinkConnector{suffix}"

    return {
        "name": connector_name,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topic_name,
            
            "connection.url": f"jdbc:mysql://{param['host']}/{param['database']}",
            "connection.user": param["user"],
            "connection.password": param["password"],
            
            "table.name.format": target_table_name,
            "auto.create": "true",
            "auto.evolve": "true",
            
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": schema_registry_url,
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "none"
        }
    }

# ==========================================
# 2. Core Tasks
# ==========================================

@task
def plan_job(**kwargs) -> dict:
    """
    [Planning Task] API conf 파싱, ES Count 조회 및 청크 계획 수립
    """
    dag_run = kwargs.get('dag_run')
    param = dag_run.conf if dag_run else {}

    if not param:
        raise AirflowFailException("No configuration received from API Trigger.")

    # ES 연결 및 Count 조회 로직 (이전 코드와 동일)
    from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
    es_hosts = [host.strip() for host in Variable.get("ELASTICSEARCH_HOSTS").split(",")]
    es_hook = ElasticsearchPythonHook(es_hosts=es_hosts, conn_kwargs={"basic_auth": ("elastic", "elastic")})
    
    try:
        count_res = es_hook.get_conn().count(index=param["elasticsearch_index"], body=param["query"])
        total_count = count_res["count"]
    except Exception as e:
        raise AirflowFailException(f"Failed to query Elasticsearch: {e}")

    chunk_size = 100000
    num_chunks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)
    if num_chunks == 0: num_chunks = 1 

    print(f"✂️ Plan: Need {num_chunks} tables/topics for {total_count} records. Service: {param.get('service')}")

    return {
        "base_param": param,
        "chunks": list(range(num_chunks)),
        "chunk_size": chunk_size,
        "schema_str": _make_avro_schema(param["project_name"], param["fields"])
    }

@task.branch(task_id="service_router")
def router(plan_info: dict) -> str:
    """
    [Branch Task] service 필드 값에 따라 실행 경로 분기
    """
    service = plan_info["base_param"].get("service", "mysql")
    
    if service == "mysql":
        return "mysql_start_flow"
    elif service == "elasticsearch":
        return "elasticsearch_start_flow"
    elif service == "excel":
        return "excel_export_start_flow"
    else:
        raise AirflowFailException(f"Unsupported service type: {service}")

@task(task_id="register_schema_mapped")
def register_schema_mapped(chunk_index: int, base_param: dict, schema_str: str):
    """[Mapped Task] 각 청크(Topic)별로 동일한 스키마를 등록"""
    from schema_registry.client import SchemaRegistryClient, schema
    schema_registry_url = Variable.get("SCHEMA_REGISTRY_URL")
    client = SchemaRegistryClient(url=schema_registry_url)
    topic_name = f"{base_param['project_name']}-topic-{chunk_index}"
    subject = f"{topic_name}-value"
    avro_schema = schema.AvroSchema(schema_str)
    schema_id = client.register(subject, avro_schema)
    print(f"✅ Schema registered for subject: {subject} (ID: {schema_id})")
    return schema_id

@task(task_id="create_connector_mapped")
def create_connector_mapped(chunk_index: int, base_param: dict) -> str:
    """[Mapped Task] 각 청크(Table)별로 Sink Connector 생성"""
    from kafka_connect import KafkaConnect 
    client = KafkaConnect(Variable.get("CONNECT_BOOTSTRAP_SERVERS"))
    config = _connect_config(base_param, chunk_index)
    
    try:
        response = client.create_connector(config)
        if response.status_code >= 400 and response.status_code != 409:
             raise AirflowFailException(f"Connector creation failed: {response.text}")
    except Exception as e:
        raise AirflowFailException(f"Connector error: {e}")
        
    print(f"✅ Connector {config['name']} created/verified.")
    return config['name']

@task(task_id="ingest_data_router")
def ingest_data_router(plan_info: dict):
    """[Single Task] 데이터를 읽어서 건수에 따라 알맞은 토픽으로 라우팅하며 전송"""
    # ... (Confluent Kafka Producer 및 ES Scroll 로직 구현 - 생략) ...
    print(f"🚀 Data ingestion completed for {len(plan_info['chunks'])} chunks.")
    return plan_info

@task(trigger_rule="all_done", task_id="delete_connectors_mapped")
def delete_connectors_mapped(connector_name: str):
    """[Mapped Task] 생성했던 커넥터 삭제"""
    from kafka_connect import KafkaConnect
    if not connector_name: return
    client = KafkaConnect(Variable.get("CONNECT_BOOTSTRAP_SERVERS"))
    response = client.delete_connector(connector_name)
    if 200 <= response.status_code < 300 or response.status_code == 404:
        print(f"🗑️ Connector {connector_name} deleted.")
    else:
        print(f"⚠️ Failed to delete connector {connector_name}: {response.text}")

# --- Service Specific Dummy Start Tasks ---

@task(task_id="mysql_start_flow")
def mysql_start_flow(plan_info: dict) -> dict:
    """MySQL Sink Flow의 시작 지점"""
    print("➡️ Starting MySQL/Kafka Sink flow.")
    return plan_info

@task(task_id="elasticsearch_start_flow")
def elasticsearch_start_flow(plan_info: dict):
    """Elasticsearch 인덱싱 Flow 시작 지점"""
    raise NotImplementedError("Elasticsearch indexing flow not implemented yet.")

@task(task_id="excel_export_start_flow")
def excel_export_start_flow(plan_info: dict):
    """Excel 파일 Export Flow 시작 지점"""
    raise NotImplementedError("Excel export flow not implemented yet.")


# ==========================================
# 3. DAG Definition
# ==========================================

@dag(
    dag_id="es_to_dynamic_sink_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), 
    schedule=None,
    catchup=False
)
def migration_pipeline():
    
    # 1. 계획 수립 및 서비스 라우팅
    plan = plan_job()
    router_task = router(plan)

    # 2. MySQL 경로 시작 (대용량 분할 로직)
    mysql_starter = mysql_start_flow(plan)

    # 2-1. 스키마 및 커넥터 인프라 구축 (Dynamic Mapping)
    schema_strs = register_schema_mapped.partial(
        base_param=plan["base_param"], 
        schema_str=plan["schema_str"]
    ).expand(chunk_index=plan["chunks"])

    connector_names = create_connector_mapped.partial(
        base_param=plan["base_param"]
    ).expand(chunk_index=plan["chunks"])

    # 2-2. 데이터 전송
    ingestion = ingest_data_router(plan_info=plan)

    # 2-3. 커넥터 삭제 (클린업)
    clean_up = delete_connectors_mapped.expand(connector_name=connector_names)

    # 의존성 연결 
    router_task >> mysql_starter
    mysql_starter >> [schema_strs, connector_names]
    connector_names >> ingestion
    ingestion >> clean_up
    
    # 3. 기타 경로 연결
    router_task >> [
        elasticsearch_start_flow(plan),
        excel_export_start_flow(plan)
    ]

migration_pipeline()