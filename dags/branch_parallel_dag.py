"""Multi-source ETL — 병렬 + 분기 + 재시도 종합 예제.

구조:
  [병렬 추출 3개] → merge(fan-in) → route(@task.branch, 3갈래)
      ├─ total==0      → handle_empty
      ├─ total<50      → transform_light
      └─ total>=50     → transform_heavy
  → load(합류, trigger_rule로 skip된 가지 허용) → [notify ∥ archive] (병렬)

데모 포인트:
- extract_api 는 ~40% 확률로 실패 → retries=2 로 자동 재시도되는 걸 보여줌
- extract_* 3개는 의존성이 없어 LocalExecutor가 동시 실행(병렬)
- route 가 고른 한 갈래만 실행, 나머지는 skip
- load 는 일부 upstream이 skip돼도 도는 trigger_rule 사용
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="multi_source_branch_etl",
    schedule=None,                      # 수동 Trigger로 돌려보기 좋음
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "demo", "branch", "parallel"],
    doc_md=__doc__,
)
def multi_source_branch_etl():

    # ---------- 1) 병렬 추출 (서로 의존 없음 → 동시에 실행) ----------
    @task
    def extract_api() -> dict:
        # 외부 API 흉내 — 가끔 실패해 retries=2 재시도를 유발
        if random.random() < 0.4:
            raise RuntimeError("API timeout (simulated) → 재시도될 것")
        time.sleep(random.uniform(1, 3))
        return {"source": "api", "rows": random.randint(0, 60)}

    @task
    def extract_db() -> dict:
        time.sleep(random.uniform(1, 3))
        return {"source": "db", "rows": random.randint(0, 60)}

    @task
    def extract_files() -> dict:
        time.sleep(random.uniform(1, 3))
        return {"source": "files", "rows": random.randint(0, 60)}

    # ---------- 2) 병합 (fan-in) ----------
    @task
    def merge(api: dict, db: dict, files: dict) -> dict:
        by_source = {s["source"]: s["rows"] for s in (api, db, files)}
        total = sum(by_source.values())
        print(f"[merge] by_source={by_source} total={total}")
        return {"total": total, "by_source": by_source}

    # ---------- 3) 분기 결정 ----------
    @task.branch
    def route(merged: dict) -> str:
        total = merged["total"]
        if total == 0:
            choice = "handle_empty"
        elif total < 50:
            choice = "transform_light"
        else:
            choice = "transform_heavy"
        print(f"[route] total={total} → {choice}")
        return choice

    @task
    def transform_heavy(merged: dict) -> str:
        print(f"[heavy] 무거운 변환 {merged['total']} rows")
        time.sleep(2)
        return "heavy_done"

    @task
    def transform_light(merged: dict) -> str:
        print(f"[light] 가벼운 변환 {merged['total']} rows")
        return "light_done"

    @task
    def handle_empty() -> str:
        print("[empty] 데이터 없음 → 격리/스킵")
        return "empty"

    # ---------- 4) 합류 (어느 갈래든 끝나면; skip된 가지가 있어도 실행) ----------
    @task(trigger_rule="none_failed_min_one_success")
    def load(merged: dict) -> str:
        print(f"[load] {merged['total']} rows 적재")
        return "loaded"

    # ---------- 5) 병렬 후처리 ----------
    @task
    def notify() -> None:
        print("[notify] 완료 알림 전송")

    @task
    def archive() -> None:
        print("[archive] 원본 보관")

    # ---------- 의존성 와이어링 ----------
    api, db, files = extract_api(), extract_db(), extract_files()
    merged = merge(api, db, files)

    branch = route(merged)
    heavy = transform_heavy(merged)
    light = transform_light(merged)
    empty = handle_empty()
    branch >> [heavy, light, empty]          # 분기: 셋 중 하나만 실행

    loaded = load(merged)
    [heavy, light, empty] >> loaded          # 합류

    loaded >> [notify(), archive()]          # 병렬 후처리


multi_source_branch_etl()
