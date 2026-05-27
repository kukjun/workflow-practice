"""Sales Daily ETL — 실전 데이터 파이프라인 예제.

흐름: extract(원천 수집) → transform(집계) → quality_check(데이터 품질 게이트) → load(적재).
- TaskFlow API(@task)로 태스크 간 데이터(XCom) 전달
- 품질 검증 실패 시 예외 → DAG 실패 → 재시도(default_args.retries)
- 외부 의존성 없이(stdlib) 동작하도록 데이터는 합성 생성

실제로 바꿀 때:
- extract: 합성 대신 DB/API/S3에서 읽기 (예: pandas, requests). 무거운 의존성은
  @task.virtualenv 또는 requirements로 태스크별 격리 권장.
- load: /tmp 파일 대신 DW/테이블/버킷에 적재.
"""
from __future__ import annotations

import json
import random
import statistics
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

CATEGORIES = ["electronics", "grocery", "clothing", "books", "toys"]
NUM_RECORDS = 500
OUTPUT_DIR = Path("/tmp/airflow/output")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="sales_daily_etl",
    schedule="0 6 * * *",            # 매일 06:00 (트리거 있는 = 자동 실행 DAG)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "demo", "sales"],
    doc_md=__doc__,
)
def sales_daily_etl():

    @task
    def extract() -> list[dict]:
        """원천에서 거래 레코드 수집 (여기선 합성 생성)."""
        rng = random.Random()
        rows = [
            {
                "category": rng.choice(CATEGORIES),
                "amount": round(rng.uniform(1, 500), 2),
                "qty": rng.randint(1, 5),
            }
            for _ in range(NUM_RECORDS)
        ]
        print(f"[extract] {len(rows)} rows")
        return rows

    @task
    def transform(rows: list[dict]) -> dict:
        """카테고리별 매출/판매량/객단가 집계."""
        agg: dict[str, dict] = {}
        for r in rows:
            bucket = agg.setdefault(r["category"], {"revenue": 0.0, "units": 0, "amounts": []})
            bucket["revenue"] += r["amount"] * r["qty"]
            bucket["units"] += r["qty"]
            bucket["amounts"].append(r["amount"])

        summary = {
            cat: {
                "revenue": round(v["revenue"], 2),
                "units": v["units"],
                "avg_ticket": round(statistics.mean(v["amounts"]), 2),
            }
            for cat, v in agg.items()
        }
        print(f"[transform] {len(summary)} categories")
        return summary

    @task
    def quality_check(summary: dict) -> dict:
        """데이터 품질 게이트 — 실패하면 예외로 DAG를 실패시켜 재시도를 유발."""
        assert summary, "집계 결과가 비어있음"
        for cat, v in summary.items():
            assert v["revenue"] >= 0, f"{cat}: 매출 음수"
            assert v["units"] > 0, f"{cat}: 판매수량 0"
        total = round(sum(v["revenue"] for v in summary.values()), 2)
        print(f"[quality_check] OK — total_revenue={total}")
        return {"summary": summary, "total_revenue": total}

    @task
    def load(result: dict) -> str:
        """적재 — 여기선 JSON 파일. 실제론 DW/버킷/테이블."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        out_path = OUTPUT_DIR / f"sales_{stamp}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

        top_cat = max(result["summary"].items(), key=lambda kv: kv[1]["revenue"])[0]
        print(f"[load] → {out_path} | total={result['total_revenue']} | top={top_cat}")
        return str(out_path)

    # DAG 의존성: extract → transform → quality_check → load
    load(quality_check(transform(extract())))


sales_daily_etl()
