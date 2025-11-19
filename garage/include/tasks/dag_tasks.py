from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def use_elasticsearch_hook(query: str, index: str) -> dict: 
    es_hosts = ["http://192.168.125.62:9200", "http://192.168.125.63:9200", "http://192.168.125.64:9200"]
    es_hook = ElasticsearchPythonHook(es_hosts=es_hosts, conn_kwargs={"basic_auth":("elastic", "elastic")})
    result = es_hook.search(query=query, index=index)
    return result

def use_mysql_hook(query : str, service_name: str) -> None:
    hook = MySqlHook(mysql_conn_id=service_name)
    records = hook.get_records(sql=query)  
    return records

def register_connector() -> str: 
    return "curl -X PUT -H 'Content-Type: application/json' \
            --data-binary @${connectorFile} \
            ${CONNECT_BOOTSTRAP_SERVERS}/connectors/${service}-SinkConnector/config" 