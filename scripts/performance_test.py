import argparse
import csv
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


SIMULATOR_URL = "http://localhost:5001"
RESULTS_FILE = Path("performance_results.csv")

SCENARIOS = {
    "smoke": {
        "sensor_count": 3,
        "interval_ms": 1000,
        "duration_seconds": 30,
        "scenario": "normal",
        "description": "Basic health check",
    },
    "low-load": {
        "sensor_count": 10,
        "interval_ms": 2000,
        "duration_seconds": 60,
        "scenario": "normal",
        "description": "Light realistic traffic",
    },
    "medium-load": {
        "sensor_count": 100,
        "interval_ms": 1000,
        "duration_seconds": 60,
        "scenario": "normal",
        "description": "Higher city-level sample",
    },
    "sustained-stress": {
        "sensor_count": 500,
        "interval_ms": 100,
        "duration_seconds": 60,
        "scenario": "normal",
        "description": "Sustained high load around 5,000 measurements/sec",
    },
    "stress": {
        "sensor_count": 1000,
        "interval_ms": 100,
        "duration_seconds": 60,
        "scenario": "normal",
        "description": "Stress target around 10,000 measurements/sec",
    },
    "alarm-spike": {
        "sensor_count": 10,
        "interval_ms": 1000,
        "duration_seconds": 60,
        "scenario": "pollution_spike",
        "description": "Alarm and anomaly path under pollution spike",
    },
}


def request_json(path, method="GET", payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        f"{SIMULATOR_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def run_scenario(name, config):
    print(f"Starting scenario '{name}': {config['description']}")

    request_json(
        "/api/start",
        method="POST",
        payload={
            "sensor_count": config["sensor_count"],
            "interval_ms": config["interval_ms"],
            "scenario": config["scenario"],
        },
    )

    start_status = request_json("/api/status")
    start_published = int(start_status.get("total_published") or 0)
    samples = []
    started_at = time.time()
    deadline = started_at + config["duration_seconds"]

    while time.time() < deadline:
        status = request_json("/api/status")
        samples.append(status)
        print(
            "  "
            f"published={status.get('total_published')} "
            f"target={status.get('target_rate')}/sec "
            f"batch_ms={status.get('last_batch_duration_ms')}"
        )
        time.sleep(5)

    end_status = request_json("/api/status")
    request_json("/api/stop", method="POST", payload={})
    end_published = int(end_status.get("total_published") or 0)
    duration = max(time.time() - started_at, 1)
    published_delta = max(end_published - start_published, 0)
    target_rate = float(end_status.get("target_rate") or 0)
    actual_rate = published_delta / duration
    batch_durations = [
        float(item.get("last_batch_duration_ms") or 0)
        for item in samples
        if item.get("last_batch_duration_ms") is not None
    ]
    max_batch_ms = max(batch_durations) if batch_durations else 0.0
    avg_batch_ms = sum(batch_durations) / len(batch_durations) if batch_durations else 0.0

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scenario_name": name,
        "description": config["description"],
        "sensor_count": config["sensor_count"],
        "interval_ms": config["interval_ms"],
        "duration_seconds": round(duration, 2),
        "target_rate_per_sec": round(target_rate, 2),
        "actual_publish_rate_per_sec": round(actual_rate, 2),
        "published_messages": published_delta,
        "avg_batch_duration_ms": round(avg_batch_ms, 2),
        "max_batch_duration_ms": round(max_batch_ms, 2),
        "scenario": config["scenario"],
    }
    append_result(result)
    print("\nResult:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    return result


def append_result(result):
    exists = RESULTS_FILE.exists()
    with RESULTS_FILE.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(result)


def main():
    parser = argparse.ArgumentParser(description="Run AirWatch Prishtina performance tests.")
    parser.add_argument(
        "scenario",
        choices=sorted(SCENARIOS),
        help="Performance scenario to run.",
    )
    parser.add_argument("--duration", type=int, help="Override duration in seconds.")
    args = parser.parse_args()

    config = dict(SCENARIOS[args.scenario])
    if args.duration:
        config["duration_seconds"] = args.duration

    try:
        run_scenario(args.scenario, config)
    except urllib.error.URLError as exc:
        raise SystemExit(f"Simulator is not reachable at {SIMULATOR_URL}: {exc}") from exc


if __name__ == "__main__":
    main()
