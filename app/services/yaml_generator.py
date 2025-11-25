from app.schemas.service import Schema 
from datetime import datetime 

def generate_yaml_file(schema : dict) -> dict : 
    dag_id = schema["project_name"]
    # 시작 날짜는 항상 어제로 설정
    start_date = datetime.date.today() - datetime.timedelta(days=1)
    return {
        dag_id: {
            "default_args" : {
                "owner" : dag_id,
                "start_date" : start_date
            },
            "description" : f"{dag_id} pipeline", 
            "schedule" : None,
        },
        "task_groups" : {
            "group_name" : "service task group",
            "tooltip" : "Tasks for handling service operations"
        },

        "tasks" : {
            "register_schema" : {
                "decorator" : "airflow.sdk.task",
                "python_callable" : "include.tasks.schema_generator.register_avro_schema",
                "es_to_db_schema" : schema
            },

            "classify_processing" : {
                "decorator" : "airflow.sdk.task",
                "python_callable" : "include.tasks.classify_processing.classify_id",
                "dependencies" : ["register_schema"],
                "branch" : True
            },

            "mysql_sink_connector" : {
                "decorator" : "airflow.sdk.task",
                "python_callable" : "include.tasks.mysql_connect.*",
                "dependencies" : ["classify_processing"],
                "task_group_name": "service task group"
            },

            "elasticsearch_sink_connector" : {
                "decorator" : "airflow.sdk.task",
                "python_callable" : "include.tasks.elasticsearch_connect.*",
                "dependencies" : ["classify_processing"],
                "task_group_name": "service task group"
            },

            "excel_sink_connector" : {
                "decorator" : "airflow.sdk.task", 
                "python_callable" : "include.tasks.excel_to_db.*",
                "dependencies" : ["classify_processing"],
                "task_group_name": "service task group"
            },

            "data_extraction_from_elasticsearch" : {
                "decorator" : "airflow.sdk.task",
                "python_callable" : "include.tasks.es_to_db.*",
                "dependencies" : ["elasticsearch_sink_connector", "mysql_sink_connector"],
                "trigger_rule" : "none_failed_min_one_success"
            },

            "data_extraction_to_excel" : {
                "decorator" : "airflow.sdk.task", 
                "python_callable" : "include.tasks.es_to_excel.*",
                "dependencies" : ["excel_sink_connector"]
            },

            "excel_to_api" : {
                "decorator" : "airflow.sdk.task", 
                "python_callable" : "include.tasks.excel_to_api.*",
                "dependencies" : ["data_extraction_to_excel"]
            },

            "post_processing" : {
                "decorator" : "airflow.sdk.task", 
                "python_callable" : "include.tasks.post_processing.*",
                "dependencies" : ["data_extraction_from_elasticsearch", "excel_to_api"] 
            }
        }
    }