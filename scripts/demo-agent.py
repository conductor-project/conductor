#!/usr/bin/env python3
"""Run a complete, isolated Conductor MVP1 lifecycle demonstration."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any


class Demo:
    def __init__(self, binary: Path, root: Path, name: str, capacity: int = 2):
        self.binary = binary
        self.root = root
        self.name = name
        self.directory = root / name
        self.directory.mkdir()
        self.config = self.directory / "agent.json"
        self.manager = self.directory / "manager"
        self.worker = self.directory / "worker"
        self.log_cap = 4096
        self.config.write_text(
            json.dumps(
                {
                    "role": "all",
                    "listen": "127.0.0.1:0",
                    "manager_data_dir": str(self.manager),
                    "worker_data_dir": str(self.worker),
                    "worker_id": f"demo-{name}",
                    "worker_concurrency": capacity,
                    "api_access": {"mode": "local"},
                    "log_max_bytes_per_stream": self.log_cap,
                    "log_retention_runs": 100,
                }
            ),
            encoding="utf-8",
        )
        self.child: subprocess.Popen[str] | None = None
        self.address: tuple[str, int] | None = None
        self.run_ids: list[str] = []
        self.daemon_output = ""

    def start(self) -> None:
        self.child = subprocess.Popen(
            [str(self.binary), "agent", "--config", str(self.config)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.child.stderr is not None
        line = self.child.stderr.readline()
        if not line:
            raise RuntimeError(f"{self.name}: agent exited before startup: {self.child.poll()}")
        event = json.loads(line)
        if event.get("event") != "agent_started":
            raise RuntimeError(f"{self.name}: unexpected startup event: {event}")
        host, port = event["address"].rsplit(":", 1)
        self.address = (host.strip("[]"), int(port))
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                status, body, _ = self.request("GET", "/readyz")
                if status == 200 and json.loads(body).get("status") == "ready":
                    return
            except (OSError, http.client.HTTPException):
                pass
            if self.child.poll() is not None:
                raise RuntimeError(f"{self.name}: agent exited during readiness: {self.child.returncode}")
            time.sleep(0.02)
        raise TimeoutError(f"{self.name}: readiness deadline expired")

    def request(
        self, method: str, path: str, payload: Any | None = None, headers: dict[str, str] | None = None
    ) -> tuple[int, str, dict[str, str]]:
        if self.address is None:
            raise RuntimeError("agent is not started")
        body = None if payload is None else json.dumps(payload, separators=(",", ":"))
        connection = http.client.HTTPConnection(*self.address, timeout=3)
        try:
            request_headers = dict(headers or {})
            if body is not None:
                request_headers["Content-Type"] = "application/json"
            connection.request(method, path, body=body, headers=request_headers)
            response = connection.getresponse()
            return response.status, response.read().decode("utf-8"), dict(response.getheaders())
        finally:
            connection.close()

    def checked(self, method: str, path: str, payload: Any | None = None, headers: dict[str, str] | None = None):
        status, body, response_headers = self.request(method, path, payload, headers)
        if not 200 <= status < 300:
            raise RuntimeError(f"{method} {path} returned {status}: {body}")
        return json.loads(body) if body else None, response_headers

    def register(self, job_id: str, command: list[str], *, timeout_ms: int | None = None, grace_ms: int = 100):
        payload = {
            "id": job_id,
            "command": command,
            "cwd": str(self.directory),
            "timeoutMs": timeout_ms,
            "terminationGraceMs": grace_ms,
        }
        status, body, _ = self.request("POST", "/v1/jobs", payload)
        if status != 201:
            raise RuntimeError(f"job {job_id} registration returned {status}: {body}")
        return json.loads(body)

    def start_run(self, job_id: str, idempotency_key: str | None = None) -> str:
        headers = {} if idempotency_key is None else {"Idempotency-Key": idempotency_key}
        status, body, _ = self.request("POST", f"/v1/jobs/{job_id}/runs", headers=headers)
        if status != 202:
            raise RuntimeError(f"start {job_id} returned {status}: {body}")
        run_id = json.loads(body)["runId"]
        self.run_ids.append(run_id)
        return run_id

    def wait_state(self, run_id: str, expected: set[str], timeout: float = 8) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status, body, _ = self.request("GET", f"/v1/runs/{run_id}")
            if status != 200:
                raise RuntimeError(f"inspect {run_id} returned {status}: {body}")
            last = json.loads(body)
            if last.get("state") in expected:
                return last
            time.sleep(0.02)
        raise TimeoutError(f"run {run_id} did not reach {sorted(expected)}: {last}")

    def logs(self, run_id: str, stream: str) -> bytes:
        payload, _ = self.checked("GET", f"/v1/runs/{run_id}/logs?stream={stream}&limit=65536")
        return base64.b64decode(payload["data"], validate=True)

    def log_metadata(self, run_id: str, stream: str) -> dict[str, Any]:
        payload, _ = self.checked("GET", f"/v1/runs/{run_id}/logs?stream={stream}&limit=65536")
        return payload

    def stop(self) -> None:
        child, self.child = self.child, None
        if child is None:
            return
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)
                raise TimeoutError(f"{self.name}: graceful daemon shutdown exceeded 10 seconds")
        if child.returncode != 0:
            raise RuntimeError(f"{self.name}: daemon exited with {child.returncode}")
        if child.stderr is not None:
            self.daemon_output = (child.stderr.read() or "")[-8192:]
        self.address = None

    def abrupt_stop(self) -> None:
        child, self.child = self.child, None
        if child is not None and child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        if child is not None and child.stderr is not None:
            self.daemon_output = (child.stderr.read() or "")[-8192:]
        self.address = None

    def report_failure(self) -> None:
        print(f"Recent daemon diagnostics ({self.name}): {self.daemon_output[-4096:]}", file=sys.stderr)
        for run_id in self.run_ids[-10:]:
            try:
                status, body, _ = self.request("GET", f"/v1/runs/{run_id}")
                print(f"run {run_id}: HTTP {status} {body[:1024]}", file=sys.stderr)
            except Exception as error:  # diagnostics must not hide the original failure
                print(f"run {run_id}: inspection unavailable ({error})", file=sys.stderr)


def python_job(source: str, *args: str) -> list[str]:
    return [sys.executable, "-c", source, *args]


def process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        fields = stat.read_text(encoding="utf-8").split()
        if len(fields) > 2 and fields[2] == "Z":
            return False
    return True


def run_demo(binary: Path, root: Path) -> None:
    demo = Demo(binary, root, "lifecycle")
    shutdown_pid: int | None = None
    try:
        demo.start()
        assert demo.checked("GET", "/healthz")[0]["status"] == "ok"

        short = python_job(
            "import sys; print('short-stdout'); print('short-stderr', file=sys.stderr)"
        )
        long = python_job("import time; print('long-started', flush=True); time.sleep(60)")
        demo.register("short", short)
        demo.register("long", long)
        short_run = demo.start_run("short")
        long_run = demo.start_run("long")
        result = demo.wait_state(short_run, {"succeeded"})
        assert result.get("exitCode") == 0, result
        assert b"short-stdout" in demo.logs(short_run, "stdout")
        assert b"short-stderr" in demo.logs(short_run, "stderr")
        demo.wait_state(long_run, {"running"})
        _, _ = demo.checked("POST", f"/v1/runs/{long_run}/stop")
        assert demo.wait_state(long_run, {"cancelled"}).get("terminationReason") == "cancelled"

        failed = python_job("import sys; print('expected-failure', file=sys.stderr); sys.exit(7)")
        demo.register("failed", failed)
        failed_run = demo.start_run("failed")
        assert demo.wait_state(failed_run, {"failed"}).get("exitCode") == 7

        missing = str(demo.directory / "does-not-exist")
        demo.register("launch-error", [missing])
        launch_run = demo.start_run("launch-error")
        launch_result = demo.wait_state(launch_run, {"failed"})
        assert launch_result.get("failureReason") == "spawn_failed", launch_result

        timeout_source = python_job("import time; time.sleep(60)")
        demo.register("timeout", timeout_source, timeout_ms=250)
        timeout_run = demo.start_run("timeout")
        assert demo.wait_state(timeout_run, {"timed_out"}).get("terminationReason") == "deadline_exceeded"

        demo.register("idempotent", python_job("import time; time.sleep(0.1)"))

        def duplicate_start(_: int) -> str:
            status, body, _ = demo.request(
                "POST",
                "/v1/jobs/idempotent/runs",
                headers={"Idempotency-Key": "demo-idempotency-1"},
            )
            if status != 202:
                raise RuntimeError(f"concurrent idempotent start returned {status}: {body}")
            run_id = json.loads(body)["runId"]
            demo.run_ids.append(run_id)
            return run_id

        with ThreadPoolExecutor(max_workers=8) as pool:
            concurrent_ids = list(pool.map(duplicate_start, range(8)))
        assert len(set(concurrent_ids)) == 1, concurrent_ids
        same = concurrent_ids[0]
        demo.wait_state(same, {"succeeded"})

        large_output = python_job(
            "import sys; sys.stdout.write('o'*16384); sys.stderr.write('e'*16384)"
        )
        demo.register("large-output", large_output)
        large_run = demo.start_run("large-output")
        demo.wait_state(large_run, {"succeeded"})
        for stream in ("stdout", "stderr"):
            chunk = demo.log_metadata(large_run, stream)
            assert len(base64.b64decode(chunk["data"], validate=True)) == demo.log_cap
            assert chunk.get("truncated") is True, chunk

        # Deterministic admission pressure: the two known-running tasks fill capacity.
        for job_id in ("capacity-a", "capacity-b"):
            demo.register(job_id, python_job("import time; time.sleep(60)"))
        capacity_runs = [demo.start_run("capacity-a"), demo.start_run("capacity-b")]
        for run_id in capacity_runs:
            demo.wait_state(run_id, {"running"})
        demo.register("capacity-extra", python_job("pass"))
        status, _, _ = demo.request("POST", "/v1/jobs/capacity-extra/runs")
        assert status == 429, f"capacity should reject excess run, got {status}"
        for run_id in capacity_runs:
            demo.checked("POST", f"/v1/runs/{run_id}/stop")
            demo.wait_state(run_id, {"cancelled"})

        shutdown_pid_file = demo.directory / "shutdown-workload.pid"
        shutdown_source = "import os,sys,time; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(60)"
        demo.register("shutdown-cleanup", python_job(shutdown_source, str(shutdown_pid_file)))
        shutdown_run = demo.start_run("shutdown-cleanup")
        demo.wait_state(shutdown_run, {"running"})
        deadline = time.monotonic() + 3
        while not shutdown_pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not shutdown_pid_file.exists():
            raise TimeoutError("shutdown fixture did not publish its own PID")
        shutdown_pid = int(shutdown_pid_file.read_text(encoding="utf-8"))
        demo.stop()
        if process_is_alive(shutdown_pid):
            raise RuntimeError(f"graceful shutdown left isolated workload PID {shutdown_pid} alive")
        demo.start()
        jobs, _ = demo.checked("GET", "/v1/jobs?limit=100")
        assert {job["id"] for job in jobs["jobs"]} >= {
            "short", "long", "failed", "launch-error", "timeout", "idempotent"
        }
        assert demo.wait_state(short_run, {"succeeded"}).get("exitCode") == 0
        assert demo.wait_state(shutdown_run, {"cancelled"}).get("terminationReason") == "shutdown"
        assert b"short-stdout" in demo.logs(short_run, "stdout")
        demo.stop()
        print("MVP1 lifecycle demo passed: success, failure, spawn error, timeout, stop, concurrent idempotency, capacity, large output, restart, results, and logs")
    except Exception:
        demo.stop()
        demo.report_failure()
        raise
    finally:
        demo.stop()
        if shutdown_pid is not None and process_is_alive(shutdown_pid):
            try:
                os.kill(shutdown_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def run_crash_demo(binary: Path, root: Path) -> None:
    demo = Demo(binary, root, "crash-recovery", capacity=1)
    pid_file = demo.directory / "workload.pid"
    workload_pid: int | None = None
    try:
        demo.start()
        source = "import os,sys,time; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(60)"
        demo.register("crash-workload", python_job(source, str(pid_file)))
        run_id = demo.start_run("crash-workload")
        demo.wait_state(run_id, {"running"})
        deadline = time.monotonic() + 3
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not pid_file.exists():
            raise TimeoutError("crash fixture did not publish its own PID")
        workload_pid = int(pid_file.read_text(encoding="utf-8"))
        demo.abrupt_stop()

        # This is the exact child PID written by this run in this temporary directory.
        try:
            os.kill(workload_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(workload_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.01)
        else:
            raise RuntimeError(f"isolated crash fixture PID {workload_pid} survived cleanup")

        demo.start()
        unknown = demo.wait_state(run_id, {"unknown"})
        assert unknown.get("state") == "unknown", unknown
        resolved, _ = demo.checked(
            "POST",
            f"/v1/runs/{run_id}/resolve",
            {"resolution": "abandon", "reason": "demo verified exact fixture PID cleanup"},
        )
        assert resolved.get("state") == "abandoned", resolved
        demo.stop()
        print("Opt-in crash demo passed: exact fixture process cleaned up, unknown run explicitly abandoned")
    except Exception:
        demo.stop()
        demo.report_failure()
        raise
    finally:
        if workload_pid is not None:
            try:
                os.kill(workload_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        demo.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/conductor"))
    parser.add_argument(
        "--include-crash",
        action="store_true",
        help="also run an isolated crash/unknown-run scenario with explicit fixture cleanup",
    )
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"conductor binary does not exist: {binary}")
    with tempfile.TemporaryDirectory(prefix="conductor-demo-") as temporary:
        root = Path(temporary)
        try:
            run_demo(binary, root)
            if args.include_crash:
                run_crash_demo(binary, root)
        except Exception as error:
            print(f"Demo failed: {error}", file=sys.stderr)
            raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
