"""가장 단순한 Airflow 3 DAG — git에서 직접 읽혀 목록에 뜨는지 확인용."""
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["demo"])
def hello_from_git():
    @task
    def say_hello():
        print("Hello from Git! (v1)")

    say_hello()


hello_from_git()
