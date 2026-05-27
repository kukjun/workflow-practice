"""여러 태스크 + 의존성(DAG) + 데이터 전달 + 분기 — Kestra data_pipeline.yaml의 Airflow판."""
import random
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["demo"])
def data_pipeline_demo():
    @task
    def extract() -> dict:
        numbers = [random.randint(1, 10) for _ in range(5)]
        return {"numbers": numbers, "total": sum(numbers)}

    @task
    def decide(data: dict) -> str:
        level = "HIGH" if data["total"] > 20 else "LOW"
        print(f"total={data['total']} → level={level}")
        return level

    @task
    def summary(data: dict, level: str) -> None:
        print(f"numbers={data['numbers']} / level={level}")

    data = extract()
    level = decide(data)
    summary(data, level)


data_pipeline_demo()
