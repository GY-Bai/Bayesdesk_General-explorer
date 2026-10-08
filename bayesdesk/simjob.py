"""Development-only deterministic workload; demonstrates suspended Worker execution."""
import argparse
import json
import time
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int, required=True)
    p.add_argument("--result", choices=["pass", "fail"], required=True)
    args = p.parse_args()
    print(json.dumps({"phase": "start", "duration": args.duration}), flush=True)
    time.sleep(args.duration)  # Only inside the *compute job*, never inside an LLM session.
    print(json.dumps({"phase": "finish", "result": args.result}), flush=True)
    return 0 if args.result == "pass" else 17


if __name__ == "__main__":
    raise SystemExit(main())
