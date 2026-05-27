"""외부로 뺀 일반 Python 파일. Kestra YAML과 무관하게 단독 실행/테스트 가능."""
import argparse
import json
import random
from datetime import datetime

from dateutil.relativedelta import relativedelta  # requirements.txt 로 설치되는 외부 의존성


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5)
    args = parser.parse_args()

    numbers = [random.randint(1, 100) for _ in range(args.rows)]
    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "next_month": (datetime.now() + relativedelta(months=1)).strftime("%Y-%m"),
        "rows": args.rows,
        "numbers": numbers,
        "total": sum(numbers),
    }

    # stdout → Kestra 로그로 잡힘
    print("[report] " + json.dumps(result, ensure_ascii=False))

    # 파일 산출물 → flow의 outputFiles 로 캡처됨
    with open("result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
