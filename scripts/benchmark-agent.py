#!/usr/bin/env python3
"""Reproducible local release-mode HTTP and RSS probe for Conductor."""

import argparse
import hashlib
import os
import json
import platform
import signal
import socket
import statistics
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional


def rss_kib(pid: int) -> Optional[int]:
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=False
    )
    try:
        return int(result.stdout.strip()) if result.returncode == 0 else None
    except ValueError:
        return None


def request(base: str, method: str, route: str, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if data is None else {"Content-Type": "application/json"}
    req = urllib.request.Request(base + route, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=3) as response:
        return response.status, json.loads(response.read())


def sample(base: str, method: str, route: str, payload=None):
    started = time.perf_counter_ns()
    status, body = request(base, method, route, payload)
    return (time.perf_counter_ns() - started) / 1_000_000, status, body


def percentiles(values):
    ordered = sorted(values)
    at = lambda fraction: round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))], 3)
    return {
        "samples": len(values),
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": at(0.95),
        "p99_ms": at(0.99),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/conductor"))
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file() or args.iterations < 1 or args.iterations > 500:
        parser.error("binary must exist and iterations must be between 1 and 500")

    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    rustc = subprocess.run(["rustc", "-V"], capture_output=True, text=True, check=False)
    with tempfile.TemporaryDirectory(prefix="conductor-benchmark-") as temporary:
        root = Path(temporary)
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        config = {
            "role": "all",
            "listen": f"127.0.0.1:{port}",
            "manager_data_dir": str(root / "manager"),
            "worker_data_dir": str(root / "worker"),
            "worker_id": "benchmark-worker",
            "worker_concurrency": 4,
            "request_timeout_ms": 10000,
        }
        config_path = root / "agent.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        process = subprocess.Popen(
            [str(binary), "agent", "--config", str(config_path)],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        base = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("agent exited before readiness")
                try:
                    request(base, "GET", "/readyz")
                    break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.02)
            else:
                raise TimeoutError("agent did not become ready within 10 seconds")

            rss_before = rss_kib(process.pid)
            health = []
            workers = []
            inspections = []
            starts = []
            for index in range(args.iterations):
                health.append(sample(base, "GET", "/healthz")[0])
                workers.append(sample(base, "GET", "/v1/workers/benchmark-worker")[0])
                job_id = f"bench-{index}"
                sample(
                    base,
                    "POST",
                    "/v1/jobs",
                    {"id": job_id, "command": ["/bin/true"], "cwd": "/tmp"},
                )
                elapsed, status, accepted = sample(base, "POST", f"/v1/jobs/{job_id}/runs")
                if status != 202:
                    raise RuntimeError(f"run start returned HTTP {status}")
                starts.append(elapsed)
                run_id = accepted["runId"]
                run_deadline = time.monotonic() + 3
                while time.monotonic() < run_deadline:
                    _, _, observed = sample(base, "GET", f"/v1/runs/{run_id}")
                    if observed["state"] in ("succeeded", "failed", "cancelled", "timed_out"):
                        inspections.append(sample(base, "GET", f"/v1/runs/{run_id}")[0])
                        break
                    time.sleep(0.005)
                else:
                    raise TimeoutError(f"run {run_id} did not finish within 3 seconds")
            rss_after = rss_kib(process.pid)
            print(
                json.dumps(
                    {
                        "platform": platform.platform(),
                        "cpu": platform.processor() or platform.machine(),
                        "logical_cpus": os.cpu_count(),
                        "rustc": rustc.stdout.strip() if rustc.returncode == 0 else "unavailable",
                        "binary_sha256": digest,
                        "iterations": args.iterations,
                        "rss_before_kib": rss_before,
                        "rss_after_kib": rss_after,
                        "rss_delta_kib": None if rss_before is None or rss_after is None else rss_after - rss_before,
                        "latency": {
                            "healthz": percentiles(health),
                            "worker_inspection": percentiles(workers),
                            "run_start": percentiles(starts),
                            "terminal_run_inspection": percentiles(inspections),
                        },
                    },
                    indent=2,
                )
            )
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if process.stderr:
                process.stderr.close()


if __name__ == "__main__":
    main()
