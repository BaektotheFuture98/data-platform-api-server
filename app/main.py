# from fastapi import FastAPI, HTTPException
# from app.entities.schema import Schema
# from app.utils.file_util import make_file, get_unique_project_name, generate_yaml_file
# import os, yaml

# app = FastAPI()

# @app.post("service/es_to_db/register")
# def register_schema(schema: Schema):
#     try:
#         yaml_path = "garage/dags"

#         # 고유 project_name 생성
#         counter = get_unique_project_name(yaml_path, schema.project_name)
#         if counter != 1 : 
#             schema.project_name = f"{schema.project_name}_{str(counter).zfill(3)}"
#             schema.table =  f"{schema.table}_{str(counter).zfill(3)}"

#         # 서비스 디렉터리 생성
#         dir_path = f"{yaml_path}/{schema.project_name}"
#         os.makedirs(dir_path, exist_ok=True)

#         # 데이터 스키마 설정파일 구현 / 저장
#         schema_data = generate_yaml_file(schema) 
#         yaml.dump 
#         make_file(f"{dir_path}/schema.json", yaml.dump(schema_data))

#         return {"message": "Schema and metadata files created successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))