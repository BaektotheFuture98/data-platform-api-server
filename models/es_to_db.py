from pydantic import BaseModel, field_validator
import json

schema_field = ["an_title" ,"in_date", "kw_docid", "an_content"]
class Schema(BaseModel) : 
    project_name : str
    query : dict
    host : str
    database : str
    table : str
    user : str        
    password : str
    elasticsearch_index : str
    fields : list[str]

    @field_validator("fields")
    def validate_fields(cls, value):
        if not all(item in schema_field for item in value):
            raise ValueError(f"Invalid fields: {value}. Must be a subset of {schema_field}")
        return value 
    
    @field_validator("query")
    def validate_query(cls, value):
        if not isinstance(value, dict):
            raise ValueError(f"'query' must be a dict, got {type(value).__name__}")
        try:
            # JSON으로 직렬화 가능한지 체크
            json.dumps(value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"'query' must be JSON serializable: {e}")
        return value