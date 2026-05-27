"""Daily Quant Research Pipeline — 종합 고도화 예제 (노드 35+).

여러 Airflow 패턴을 한 DAG에 의도적으로 섞은 "쇼케이스"입니다.

전체 흐름(스테이지):
  0) preflight   : init_workspace(setup) → check_trading_day(@task.branch)
                     ├─ 휴장일 → market_closed → finalize 로 바로 합류
                     └─ 개장일 → open_session → 1단계로
  1) ingest      : 4개 원천 병렬 수집(fan-out), 각 원천마다 validate 순차 연결
                     - ingest_prices 는 flaky → retries + 지수 백오프(재시작 데모)
  2) consolidate : 4갈래 합류(fan-in, trigger_rule)
  3) route_quality(@task.branch) : 데이터 완전성으로 3갈래
                     ├─ FULL     → proceed_full      (정식 피처/모델 경로)
                     ├─ DEGRADED → proceed_degraded  (경량 신호만)
                     └─ ABORT    → abort_low_quality (종료)
  4) features    : TaskGroup 안에서 4개 피처 병렬 계산 → assemble_features(fan-in)
  5) universe    : 동적 태스크 매핑(.expand) — 섹터 N개를 동시 채점 → rank_sectors
  6) model       : train → [backtest ∥ risk_check] 병렬 → validate_model(gate)
                     - train_model 도 가끔 실패 → 재시도 데모
  7) publish     : [publish_signals ∥ update_dashboard ∥ archive ∥ report] 병렬
  8) finalize    : 모든 경로 합류(trigger_rule) → close_session(teardown)

데모 포인트 요약:
- 병렬(fan-out)·순차(chain)·합류(fan-in) 혼합
- @task.branch 2회(휴장 분기 + 품질 분기) → 선택 안 된 가지 skip
- 동적 태스크 매핑(.expand)으로 런타임에 태스크 수 결정
- @task_group 으로 논리 묶음/네임스페이스
- setup/teardown( .as_setup() / .as_teardown() )로 워크스페이스 보장·정리
- retries + 지수 백오프로 자동 재시작(restart) 시연
- trigger_rule 로 일부 upstream이 skip/되어도 도는 합류점
- 모든 데이터는 stdlib 합성 — 외부 의존성 0
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group

# 원천/섹터 등 상수 — 실제론 Variable/Connection/Config 에서 주입
SOURCES = ["prices", "fundamentals", "news", "fx"]
SECTORS = ["tech", "financials", "energy", "healthcare", "consumer", "industrials"]

default_args = {
    "owner": "quant-research",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}


@dag(
    dag_id="daily_quant_research_pipeline",
    schedule=None,                       # 수동 Trigger로 데모하기 좋음 (스케줄 붙이려면 "0 7 * * 1-5")
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=8,                  # 병렬 동시 실행 상한 — fan-out 체감용
    tags=["quant", "demo", "showcase", "branch", "parallel", "mapping", "taskgroup"],
    doc_md=__doc__,
)
def daily_quant_research_pipeline():

    # ============================================================
    # 0) PREFLIGHT — setup + 개장일 분기
    # ============================================================
    @task
    def init_workspace() -> str:
        """작업 디렉터리/세션 준비 (setup). teardown과 짝을 이뤄 항상 정리 보장."""
        run_id = datetime.now().strftime("run-%Y%m%dT%H%M%S")
        print(f"[setup] 워크스페이스 초기화 → {run_id}")
        return run_id

    @task.branch
    def check_trading_day() -> str:
        """개장일이면 세션 오픈, 아니면 휴장 처리로 분기."""
        # 데모: 85% 확률로 개장일
        is_open = random.random() < 0.85
        choice = "open_session" if is_open else "market_closed"
        print(f"[preflight] trading_day={is_open} → {choice}")
        return choice

    @task
    def market_closed() -> str:
        print("[preflight] 휴장일 — 파이프라인 스킵, finalize로 직행")
        return "closed"

    @task
    def open_session(run_id: str) -> dict:
        print(f"[preflight] 세션 오픈 (run_id={run_id})")
        return {"run_id": run_id, "opened_at": time.time()}

    # ============================================================
    # 1) INGEST — 원천 4개 병렬 수집 + 원천별 검증 순차
    # ============================================================
    @task(retries=3, retry_delay=timedelta(seconds=10),
          retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=2))
    def ingest_prices(session: dict) -> dict:
        """가격 원천 — 외부 API 흉내, ~45% 실패 → retries=3 + 지수 백오프로 재시작."""
        if random.random() < 0.45:
            raise RuntimeError("price feed timeout (simulated) → 재시도(backoff)")
        time.sleep(random.uniform(1, 2))
        return {"source": "prices", "rows": random.randint(800, 1200)}

    @task
    def ingest_fundamentals(session: dict) -> dict:
        time.sleep(random.uniform(1, 2))
        return {"source": "fundamentals", "rows": random.randint(200, 400)}

    @task
    def ingest_news(session: dict) -> dict:
        time.sleep(random.uniform(1, 2))
        return {"source": "news", "rows": random.randint(0, 300)}  # 가끔 0 → 품질 분기 자극

    @task
    def ingest_fx(session: dict) -> dict:
        time.sleep(random.uniform(1, 2))
        return {"source": "fx", "rows": random.randint(50, 120)}

    @task
    def validate(raw: dict) -> dict:
        """원천별 스키마/건수 검증 — 각 ingest 바로 뒤에 순차로 붙음."""
        ok = raw["rows"] > 0
        print(f"[validate:{raw['source']}] rows={raw['rows']} ok={ok}")
        return {**raw, "ok": ok}

    # ============================================================
    # 2) CONSOLIDATE — 4갈래 합류 (fan-in)
    # ============================================================
    @task(trigger_rule="all_success")
    def consolidate(prices: dict, fundamentals: dict, news: dict, fx: dict) -> dict:
        parts = [prices, fundamentals, news, fx]
        ok_sources = [p["source"] for p in parts if p["ok"]]
        total = sum(p["rows"] for p in parts)
        completeness = len(ok_sources) / len(parts)
        print(f"[consolidate] total_rows={total} ok={ok_sources} completeness={completeness:.0%}")
        return {"total": total, "completeness": completeness, "ok_sources": ok_sources}

    # ============================================================
    # 3) ROUTE_QUALITY — 데이터 완전성으로 3갈래 분기
    # ============================================================
    @task.branch
    def route_quality(c: dict) -> str:
        comp = c["completeness"]
        if comp >= 0.99:
            choice = "proceed_full"
        elif comp >= 0.5:
            choice = "proceed_degraded"
        else:
            choice = "abort_low_quality"
        print(f"[route_quality] completeness={comp:.0%} → {choice}")
        return choice

    @task
    def proceed_full(c: dict) -> dict:
        print("[route] FULL 경로 — 정식 피처+모델 파이프라인 진행")
        return c

    @task
    def proceed_degraded(c: dict) -> dict:
        print("[route] DEGRADED 경로 — 경량 신호만 생성")
        return c

    @task
    def abort_low_quality() -> str:
        print("[route] ABORT — 데이터 품질 미달, 파이프라인 중단")
        return "aborted"

    # ---- DEGRADED 경로의 경량 신호 ----
    @task
    def quick_signal(c: dict) -> dict:
        sig = round(random.uniform(-1, 1), 3)
        print(f"[degraded] quick_signal={sig} (rows={c['total']})")
        return {"signal": sig, "mode": "degraded"}

    # ============================================================
    # 4) FEATURES — TaskGroup 안에서 4개 병렬 → assemble (fan-in)
    # ============================================================
    @task_group(group_id="features")
    def feature_group(c: dict) -> dict:
        @task
        def compute_returns(c: dict) -> dict:
            time.sleep(random.uniform(0.5, 1.5))
            return {"returns": round(random.uniform(-0.05, 0.05), 4)}

        @task
        def compute_volatility(c: dict) -> dict:
            time.sleep(random.uniform(0.5, 1.5))
            return {"vol": round(random.uniform(0.1, 0.4), 4)}

        @task
        def compute_momentum(c: dict) -> dict:
            time.sleep(random.uniform(0.5, 1.5))
            return {"mom": round(random.uniform(-1, 1), 4)}

        @task
        def compute_liquidity(c: dict) -> dict:
            time.sleep(random.uniform(0.5, 1.5))
            return {"liq": round(random.uniform(0, 1), 4)}

        @task
        def assemble_features(r: dict, v: dict, m: dict, l: dict) -> dict:
            feats = {**r, **v, **m, **l}
            print(f"[features] assembled={feats}")
            return feats

        # 4개 병렬 계산 후 합류
        return assemble_features(
            compute_returns(c), compute_volatility(c),
            compute_momentum(c), compute_liquidity(c),
        )

    # ============================================================
    # 5) UNIVERSE — 동적 태스크 매핑: 섹터 N개를 동시 채점 → rank
    # ============================================================
    @task
    def list_sectors(c: dict) -> list[str]:
        # 런타임에 유니버스 결정 → .expand 로 태스크 수가 정해짐
        print(f"[universe] {len(SECTORS)}개 섹터 채점 예정")
        return SECTORS

    @task
    def score_sector(sector: str) -> dict:
        """동적 매핑 대상 — 섹터마다 별도 태스크 인스턴스로 병렬 실행."""
        time.sleep(random.uniform(0.3, 1.0))
        score = round(random.uniform(0, 100), 1)
        print(f"[score:{sector}] {score}")
        return {"sector": sector, "score": score}

    @task
    def rank_sectors(scored: list[dict]) -> dict:
        """매핑된 인스턴스들의 출력을 리스트로 받아 합류(fan-in)."""
        ranked = sorted(scored, key=lambda d: d["score"], reverse=True)
        top = ranked[0]["sector"] if ranked else None
        print(f"[universe] ranked={[d['sector'] for d in ranked]} top={top}")
        return {"top_sector": top, "ranked": ranked}

    # ============================================================
    # 6) MODEL — train → [backtest ∥ risk_check] → validate(gate)
    # ============================================================
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def train_model(feats: dict, universe: dict) -> dict:
        """모델 학습 — ~30% 실패 → 재시도(restart) 데모."""
        if random.random() < 0.30:
            raise RuntimeError("training diverged (simulated) → 재시도")
        time.sleep(random.uniform(1, 2))
        model = {"top_sector": universe["top_sector"], "ic": round(random.uniform(0, 0.2), 3)}
        print(f"[model] trained {model}")
        return model

    @task
    def backtest(model: dict) -> dict:
        time.sleep(random.uniform(1, 2))
        sharpe = round(random.uniform(-0.5, 2.5), 2)
        print(f"[backtest] sharpe={sharpe}")
        return {"sharpe": sharpe}

    @task
    def risk_check(model: dict) -> dict:
        time.sleep(random.uniform(0.5, 1.5))
        max_dd = round(random.uniform(0.05, 0.4), 3)
        print(f"[risk] max_drawdown={max_dd}")
        return {"max_dd": max_dd}

    @task
    def validate_model(bt: dict, risk: dict) -> dict:
        """승인 게이트 — 샤프/낙폭 기준 통과 여부."""
        passed = bt["sharpe"] > 0.5 and risk["max_dd"] < 0.3
        print(f"[gate] sharpe={bt['sharpe']} max_dd={risk['max_dd']} → passed={passed}")
        return {"passed": passed, **bt, **risk}

    # ============================================================
    # 7) PUBLISH — 결과 병렬 후처리 (FULL/DEGRADED 모두 합류 가능)
    # ============================================================
    @task(trigger_rule="none_failed_min_one_success")
    def publish_signals(verdict: dict) -> str:
        print(f"[publish] 신호 배포: {verdict}")
        return "published"

    @task
    def update_dashboard() -> None:
        print("[publish] 대시보드 갱신")

    @task
    def archive_artifacts(run_id: str) -> None:
        print(f"[publish] 산출물 아카이브 (run_id={run_id})")

    @task
    def send_report() -> None:
        print("[publish] 리포트 발송")

    # ============================================================
    # 8) FINALIZE — 모든 경로 합류 + teardown
    # ============================================================
    @task(trigger_rule="none_failed")
    def finalize() -> str:
        """개장/휴장/abort/full/degraded 어느 경로든 여기로 합류."""
        print("[finalize] 파이프라인 마감")
        return "done"

    @task
    def close_session(run_id: str) -> None:
        """세션/리소스 정리 (teardown) — upstream 실패해도 항상 실행 보장."""
        print(f"[teardown] 세션 종료 및 정리 (run_id={run_id})")

    # ============================================================
    # 의존성 와이어링
    # ============================================================
    # --- 0) preflight ---
    run_id = init_workspace()
    setup = run_id.as_setup()              # setup 으로 표시

    trading = check_trading_day()
    closed = market_closed()
    session = open_session(run_id)
    setup >> trading
    trading >> [session, closed]           # 개장 분기

    # --- 1) ingest (병렬) + 원천별 validate (순차) ---
    raw_prices = ingest_prices(session)
    raw_fund = ingest_fundamentals(session)
    raw_news = ingest_news(session)
    raw_fx = ingest_fx(session)

    v_prices = validate(raw_prices)
    v_fund = validate(raw_fund)
    v_news = validate(raw_news)
    v_fx = validate(raw_fx)

    # --- 2) consolidate (fan-in) ---
    consolidated = consolidate(v_prices, v_fund, v_news, v_fx)

    # --- 3) route_quality (분기) ---
    route = route_quality(consolidated)
    full = proceed_full(consolidated)
    degraded = proceed_degraded(consolidated)
    aborted = abort_low_quality()
    route >> [full, degraded, aborted]

    # --- 4) features (FULL 경로) ---
    feats = feature_group(full)

    # --- 5) universe (동적 매핑, FULL 경로) ---
    sectors = list_sectors(full)
    scored = score_sector.expand(sector=sectors)   # ← N개 인스턴스로 펼침
    universe = rank_sectors(scored)

    # --- 6) model (FULL 경로) ---
    model = train_model(feats, universe)
    bt = backtest(model)
    risk = risk_check(model)
    model >> [bt, risk]                    # 병렬
    verdict = validate_model(bt, risk)     # 합류(gate)

    # --- DEGRADED 경로 ---
    quick = quick_signal(degraded)

    # --- 7) publish (FULL=verdict / DEGRADED=quick 모두 가능) ---
    published = publish_signals(verdict)
    [verdict, quick] >> published          # 어느 경로든 하나 성공이면 진행
    dash = update_dashboard()
    arch = archive_artifacts(run_id)
    report = send_report()
    published >> [dash, arch, report]      # 병렬 후처리

    # --- 8) finalize (모든 경로 합류) + teardown ---
    done = finalize()
    [dash, arch, report, closed, aborted] >> done

    teardown = close_session(run_id)
    teardown.as_teardown(setups=setup)     # setup↔teardown 짝
    done >> teardown


daily_quant_research_pipeline()
