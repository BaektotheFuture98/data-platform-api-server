from pydantic import BaseModel, field_validator
import json

schema_field = ["an_title" ,"in_date", "kw_docid", "an_content"]
services = ["mysql", "elasticsearch", "excel"]

class Schema(BaseModel) : 
    service : str = "mysql"
    project_name : str
    query : dict
    host : str
    database : str
    table : str
    user : str        
    password : str
    elasticsearch_index : str
    fields : list[str]


    @field_validator("service")
    def validate_service(cls, value) : 
        if value not in services : 
            raise ValueError(f"Invalid service: {value}. Must be one of {services}")
        return value

    @field_validator("fields")
    def validate_fields(cls, value):
        if not isinstance(value, (list, tuple)) : 
            raise TypeError("'fields' must be a list of field names")
        invalid = [item for item in value if item not in schema_field]
        if invalid : 
            raise ValueError(f"Invalid fields: {invalid}. Allowed: {schema_field}")
        return list(value)
    
    @field_validator("query")
    def validate_query(cls, value):
        if not isinstance(value, dict):
            raise ValueError(f"'query' must be a dict, got {type(value).__name__}")
        try:
            json.dumps(value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"'query' must be JSON serializable: {e}")
        return value
    
    def to_dict(self):
        return {
            "project_name": self.project_name,
            "query": self.query,
            "host": self.host,
            "database": self.database,
            "table": self.table,
            "user": self.user,
            "password": self.password,
            "elasticsearch_index": self.elasticsearch_index,
            "fields": self.fields
        }