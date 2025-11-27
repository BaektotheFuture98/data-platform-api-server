import os
import json
import requests
from app.schemas.service import Schema

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator, Field
from typing import List, Dict, Any
import uvicorn
# ==========================================
# 1. Configuration and Constants
# ==========================================

# Airflow 설정 (환경 변수에서 가져오는 것을 권장합니다.)
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://192.168.125.62:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "25z4hDZB3kYz9HYg")
AIRFLOW_DAG_ID = "es_to_dynamic_sink_pipeline"

# ==========================================
# 3. API Endpoint Definition
# ==========================================
app = FastAPI()

@app.post("/api/trigger-etl/")
def trigger_airflow_dag(data: Schema):
    """
    클라이언트로부터 데이터를 받아 유효성 검증 후 Airflow DAG를 실행합니다.
    """
    # 1. 유효성 검증 (Pydantic이 자동으로 처리함)
    
    # 2. Airflow Trigger Payload 구성
    # Pydantic 객체를 딕셔너리로 변환하여 Airflow conf에 넣습니다.
    airflow_conf = data.model_dump()
    
    payload = {
        "conf": airflow_conf 
    }
    
    # 3. Airflow REST API 호출
    api_url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
    
    try:
        response = requests.post(
            api_url,
            json=payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"}
        )
        
        # 4. 응답 처리
        if response.status_code == 200:
            return {
                "message": "Airflow DAG triggered successfully.",
                "dag_id": AIRFLOW_DAG_ID,
                "dag_run_id": response.json().get("dag_run_id"),
                "status": response.status_code
            }
        elif response.status_code == 404:
             raise HTTPException(status_code=404, detail=f"DAG ID '{AIRFLOW_DAG_ID}' not found or Airflow API URL incorrect.")
        elif response.status_code == 401:
             raise HTTPException(status_code=401, detail="Airflow Authentication Failed. Check AIRFLOW_USER/PASSWORD.")
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Airflow API Error: {response.text}")
            
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Could not connect to Airflow URL: {AIRFLOW_URL}. Error: {e}")