#!/usr/bin/env python3
"""Measure Conductor's release-mode Linux run capacity under process and output load."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import platform
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


TARGET_RUNS = 64
MAX_RSS_KIB = 512 * 1024
MAX_THREADS = 256
MAX_FILE_DESCRIPTORS = 1024
MAX_HEALTH_P95_MS = 250.0
MAX_START_P95_MS = 2_000.0
MAX_STOP_P95_MS = 5_000.0
MAX_STOP_COMPLETION_SECONDS = 30.0


def request(base, method, route, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if data is None else {"Content-Type": "application/json"}
    req = urllib.request.Request(base + route, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.status, json.loads(response.read())


def sample(base, method, route, payload=None):
    started = time.perf_counter_ns()
    status, body = request(base, method, route, payload)
    return (time.perf_counter_ns() - started) / 1_000_000, status, body


def percentile(values, fraction):
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))], 3)


def distribution(values):
    return {
        "samples": len(values),
        "p50_ms": round(statistics.median(values), 3),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
    }


def process_metrics(pid):
    status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    fields = {}
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "Threads"):
            fields[key] = int(value.split()[0])
    fields["file_descriptors"] = len(os.listdir(f"/proc/{pid}/fd"))
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
    fields["cpu_ticks"] = int(stat[11]) + int(stat[12])
    return {
        "rss_kib": fields["VmRSS"],
        "threads": fields["Threads"],
        "file_descriptors": fields["file_descriptors"],
        "cpu_ticks": fields["cpu_ticks"],
    }


def p95(values):
    return percentile(values, 0.95)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/conductor"))
    parser.add_argument("--runs", type=int, default=TARGET_RUNS)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("capacity qualification requires Linux and /proc")
    if not 1 <= args.runs <= TARGET_RUNS:
        parser.error(f"runs must be between 1 and {TARGET_RUNS}")
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("release binary does not exist")

    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="conductor-linux-capacity-") as temporary:
        root = Path(temporary)
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        config = {
            "role": "all",
            "listen": f"127.0.0.1:{port}",
            "manager_data_dir": str(root / "manager"),
            "worker_data_dir": str(root / "worker"),
            "worker_id": "capacity-worker",
            "worker_concurrency": args.runs,
            "request_timeout_ms": 10_000,
            "log_max_bytes_per_stream": 65_536,
            "log_retention_runs": args.runs,
        }
        config_path = root / "agent.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        process = subprocess.Popen(
            [str(binary), "agent", "--config", str(config_path)],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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

            jobs = []
            for index in range(args.runs):
                job_id = f"capacity-{index:03}"
                command = ["/bin/sleep", "60"] if index < args.runs * 3 // 4 else ["/usr/bin/yes"]
                status, _ = request(
                    base,
                    "POST",
                    "/v1/jobs",
                    {"id": job_id, "command": command, "cwd": "/tmp"},
                )
                if status != 201:
                    raise RuntimeError(f"job registration returned HTTP {status}")
                jobs.append(job_id)

            def start(job_id):
                elapsed, status, observation = sample(base, "POST", f"/v1/jobs/{job_id}/runs")
                if status != 202:
                    raise RuntimeError(f"run start returned HTTP {status}")
                return job_id, observation["runId"], elapsed

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.runs, 32)) as pool:
                started = list(pool.map(start, jobs))

            health = []
            peaks = {"rss_kib": 0, "threads": 0, "file_descriptors": 0}
            cpu_before = process_metrics(process.pid)["cpu_ticks"]
            cpu_started = time.monotonic()
            sample_deadline = cpu_started + 5.0
            while time.monotonic() < sample_deadline:
                health.append(sample(base, "GET", "/healthz")[0])
                current = process_metrics(process.pid)
                for key in peaks:
                    value = current[key]
                    peaks[key] = max(peaks[key], value)
                time.sleep(0.02)
            cpu_after = process_metrics(process.pid)["cpu_ticks"]
            cpu_percent = round(
                (cpu_after - cpu_before)
                * 100
                / os.sysconf("SC_CLK_TCK")
                / (time.monotonic() - cpu_started),
                2,
            )

            def stop(run):
                job_id, run_id, _ = run
                sent_at = time.perf_counter()
                elapsed, status, _ = sample(base, "POST", f"/v1/runs/{run_id}/stop")
                if status not in (200, 202):
                    raise RuntimeError(f"stop for {job_id} returned HTTP {status}")
                return run_id, sent_at, elapsed

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                stops = list(pool.map(stop, started))
            stop_latencies = []
            pending = {run_id: sent_at for run_id, sent_at, _ in stops}
            stop_responses = [elapsed for _, _, elapsed in stops]
            pending_states = {}
            deadline = time.monotonic() + MAX_STOP_COMPLETION_SECONDS
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(args.runs, 32)
            ) as poller:
                while pending and time.monotonic() < deadline:
                    futures = {
                        poller.submit(request, base, "GET", f"/v1/runs/{run_id}"): (
                            run_id,
                            sent_at,
                        )
                        for run_id, sent_at in pending.items()
                    }
                    for future in concurrent.futures.as_completed(futures):
                        run_id, sent_at = futures[future]
                        _, observation = future.result()
                        state = observation["state"]
                        if state in ("succeeded", "failed", "cancelled", "timed_out"):
                            stop_latencies.append((time.perf_counter() - sent_at) * 1000)
                            del pending[run_id]
                            pending_states.pop(run_id, None)
                        else:
                            pending_states[run_id] = state
                    if pending:
                        time.sleep(0.02)
            if pending:
                raise TimeoutError(
                    f"{len(pending)} runs did not stop within "
                    f"{MAX_STOP_COMPLETION_SECONDS:g} seconds; states={pending_states}"
                )

            checks = {
                "peak_rss_within_512_mib": peaks["rss_kib"] <= MAX_RSS_KIB,
                "peak_threads_within_256": peaks["threads"] <= MAX_THREADS,
                "peak_fds_within_1024": peaks["file_descriptors"] <= MAX_FILE_DESCRIPTORS,
                "health_p95_within_250_ms": p95(health) <= MAX_HEALTH_P95_MS,
                "start_p95_within_2000_ms": p95([elapsed for _, _, elapsed in started]) <= MAX_START_P95_MS,
                "stop_p95_within_5000_ms": p95(stop_latencies) <= MAX_STOP_P95_MS,
            }
            report = {
                "platform": platform.platform(),
                "architecture": platform.machine(),
                "logical_cpus": os.cpu_count(),
                "rustc": subprocess.run(
                    ["rustc", "-V"], capture_output=True, text=True, check=False
                ).stdout.strip(),
                "binary_sha256": digest,
                "target_concurrent_runs": args.runs,
                "workload": {
                    "long_running_processes": args.runs * 3 // 4,
                    "continuous_output_processes": args.runs - args.runs * 3 // 4,
                    "log_cap_bytes_per_stream": 65_536,
                    "cancellation": "all runs stopped concurrently through 8 request workers",
                    "storage": "temporary local filesystem; no storage latency injection",
                },
                "peak_agent_metrics": peaks,
                "agent_cpu_percent_of_one_core": cpu_percent,
                "latency": {
                    "healthz_under_load": distribution(health),
                    "run_start": distribution([elapsed for _, _, elapsed in started]),
                    "stop_response": distribution(stop_responses),
                    "stop_to_terminal": distribution(stop_latencies),
                },
                "acceptance": checks,
                "passed": all(checks.values()),
            }
            print(json.dumps(report, indent=2), flush=True)
            if not report["passed"]:
                raise SystemExit("Linux capacity qualification failed")
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
