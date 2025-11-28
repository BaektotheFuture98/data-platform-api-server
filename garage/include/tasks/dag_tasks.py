from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Variable
import json 

from app.schemas.service import Schema

# Elasticsearch Hook
def use_elasticsearch_hook(query: str, index: str) -> dict: 
    es_hosts = ["http://192.168.125.62:9200", "http://192.168.125.63:9200", "http://192.168.125.64:9200"]
    es_hook = ElasticsearchPythonHook(es_hosts=es_hosts, conn_kwargs={"basic_auth":("elastic", "elastic")})
    result = es_hook.search(query=query, index=index)
    return result

# MySQL Hook
def use_mysql_hook(query : str, service_name: str) -> None:
    hook = MySqlHook(mysql_conn_id=service_name)
    records = hook.get_records(sql=query)  
    return records

# Kafka Connect에 Sink Connector 등록/삭제 명령어 생성
def register_connector() -> str: 
    CONNECT_BOOTSTRAP_SERVERS=Variable.get("CONNECT_BOOTSTRAP_SERVERS")
    return "curl -X PUT -H 'Content-Type: application/json' \
            --data-binary @${connectorFile} \
            ${CONNECT_BOOTSTRAP_SERVERS}/connectors/${service}-SinkConnector/config" 

def delete_connector() -> str: 
    return "curl -X DELETE ${CONNECT_BOOTSTRAP_SERVERS}/connectors/${service}-SinkConnector"



# Schema Registry에 Avro 스키마 등록
def register_schema(schema : Schema) -> int : 
    from schema_registry.client import SchemaRegistryClient, schema

    SCHEMA_REGISTRY_URL=Variable.get("SCHEMA_REGISTRY_URL")

    client = SchemaRegistryClient(url=SCHEMA_REGISTRY_URL)
    json_schema = _make_avro_schema(schema.project_name, schema.fields)
    
    avro_schema = schema.AvroSchema(json_schema)
    
    return client.register(schema.project_name + "-topic-value", avro_schema)

def _make_avro_schema(project_name : str, fields : list) -> json: 
    data_schema = {}
    data_schema["type"] = "record"
    data_schema["name"] = project_name

    fields = []
    for field_name in fields : 
        if field_name == "in_date" : 
            field_type = "int"
        else : 
            field_type = "string"  # field type은 하드코딩으로 name에 맞춰서 지정 필요
        field_schema = {}
        field_schema["name"] = field_name
        field_schema["type"] = field_type
        fields.append(field_schema)

    data_schema["fields"] = fields

    return json.dumps(data_schema, ensure_ascii=False, indent=4) 