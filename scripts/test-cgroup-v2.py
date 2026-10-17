#!/usr/bin/env python3
"""Exercise real cgroup v2 limits, launcher placement, cleanup, and crash recovery."""

import argparse
import base64
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def request(address, method, route, payload=None):
    body = None if payload is None else json.dumps(payload, separators=(",", ":"))
    # The cgroup launcher has a bounded five-second placement handshake.
    connection = http.client.HTTPConnection(*address, timeout=35)
    try:
        headers = {} if body is None else {"Content-Type": "application/json"}
        connection.request(method, route, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        decoded = json.loads(raw) if raw else None
        return response.status, decoded
    finally:
        connection.close()


def wait_agent(process, address, ready=True):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Conductor exited during cgroup integration coverage")
        try:
            status, body = request(address, "GET", "/readyz")
            if ready and status == 200 and body["status"] == "ready":
                return
            if not ready and status == 503:
                return
        except (OSError, TimeoutError, http.client.HTTPException):
            pass
        time.sleep(0.02)
    raise TimeoutError("Conductor did not become ready within 10 seconds")


def wait_run(address, run_id, expected, timeout=10):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status, last = request(address, "GET", f"/v1/runs/{run_id}")
        if status != 200:
            raise RuntimeError(f"run inspection returned HTTP {status}")
        if last["state"] in expected:
            return last
        time.sleep(0.02)
    raise TimeoutError(f"run {run_id} did not reach {sorted(expected)}: {last}")


def wait_worker_slot(address, timeout=10):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        status, workers = request(address, "GET", "/v1/workers")
        if status != 200 or len(workers) != 1:
            raise RuntimeError(f"worker status returned HTTP {status}: {workers}")
        latest = workers[0]
        if latest["availableSlots"] > 0 and latest["available"]:
            return latest
        time.sleep(0.02)
    raise TimeoutError(f"worker did not release an execution slot: {latest}")


def logs(address, run_id):
    status, body = request(address, "GET", f"/v1/runs/{run_id}/logs?stream=stdout&limit=65536")
    if status != 200:
        raise RuntimeError(f"run logs returned HTTP {status}")
    return base64.b64decode(body["data"], validate=True).decode("utf-8")


def reap_owned_groups(worker_root):
    if not worker_root.exists():
        return
    for group in worker_root.iterdir():
        if not group.name.startswith("run-") or not group.is_dir():
            continue
        try:
            (group / "cgroup.kill").write_text("1", encoding="ascii")
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                events = (group / "cgroup.events").read_text(encoding="ascii")
                if "populated 0" in events.splitlines():
                    group.rmdir()
                    break
                time.sleep(0.02)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/conductor"))
    parser.add_argument("--delegated-root", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("real cgroup v2 integration coverage requires Linux")
    delegated_root = args.delegated_root.resolve(strict=True)
    required = ("cgroup.controllers", "cgroup.subtree_control", "cgroup.events", "cgroup.procs", "cgroup.kill")
    if any(not (delegated_root / filename).is_file() for filename in required):
        raise RuntimeError("delegated root lacks required cgroup v2 controls; enforcement was not tested")
    for controller in ("cpu", "memory", "pids"):
        for filename in ("cgroup.controllers", "cgroup.subtree_control"):
            values = (delegated_root / filename).read_text(encoding="ascii").split()
            if controller not in values:
                raise RuntimeError(f"delegated root does not expose {controller} in {filename}")
    if not args.binary.is_file():
        parser.error("release binary does not exist")

    with tempfile.TemporaryDirectory(prefix="conductor-cgroup-v2-") as temporary:
        state = Path(temporary)
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        address = ("127.0.0.1", port)
        worker_id = "cgroup-v2-test"
        worker_hash = 0xCBF29CE484222325
        for byte in worker_id.encode():
            worker_hash ^= byte
            worker_hash = (worker_hash * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        worker_root = delegated_root / f"conductor-{worker_hash:016x}"
        config = {
            "role": "all",
            "listen": f"127.0.0.1:{port}",
            "manager_data_dir": str(state / "manager"),
            "worker_data_dir": str(state / "worker"),
            "worker_id": worker_id,
            "worker_concurrency": 2,
            "request_timeout_ms": 30000,
            "cgroup_v2": {
                "enabled": True,
                "delegatedRoot": str(delegated_root),
                "maximums": {
                    "cpuQuotaUs": 200_000,
                    "cpuPeriodUs": 100_000,
                    "memoryMaxBytes": 268_435_456,
                    "memorySwapMaxBytes": 0,
                    "pidsMax": 64,
                },
            },
        }
        config_path = state / "agent.json"
        agent_log = state / "agent.stderr.log"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        def launch():
            return subprocess.Popen(
                [str(args.binary.resolve()), "agent", "--config", str(config_path)],
                cwd=state,
                stdout=subprocess.DEVNULL,
                stderr=agent_log.open("ab"),
            )

        process = launch()
        run_ids = []
        try:
            wait_agent(process, address)
            pid_path = state / "escaped-child.pid"
            job = {
                "id": "cgroup-limits",
                "command": [
                    "/bin/sh",
                    "-c",
                    "mount=$(/usr/bin/awk '$0 ~ / - cgroup2 / {print $5; exit}' /proc/self/mountinfo); "
                    "group=$(/usr/bin/awk -F: '$1==\"0\" {print $3}' /proc/self/cgroup); "
                    "directory=\"$mount$group\"; printf 'membership=%s\\n' \"$group\"; "
                    "printf 'cpu='; /bin/cat \"$directory/cpu.max\"; "
                    "printf 'memory='; /bin/cat \"$directory/memory.max\"; "
                    "printf 'swap='; /bin/cat \"$directory/memory.swap.max\"; "
                    "printf 'pids='; /bin/cat \"$directory/pids.max\"; "
                    "/usr/bin/setsid /bin/sleep 60 & echo $! > \"$TEST_PID_FILE\"; wait",
                ],
                "cwd": "/tmp",
                "env": {"TEST_PID_FILE": str(pid_path)},
                "terminationGraceMs": 100,
                "resourceLimits": {
                    "cpuQuotaUs": 50_000,
                    "cpuPeriodUs": 100_000,
                    "memoryMaxBytes": 134_217_728,
                    "memorySwapMaxBytes": 0,
                    "pidsMax": 32,
                },
            }
            status, _ = request(address, "POST", "/v1/jobs", job)
            assert status == 201, f"job registration returned HTTP {status}"
            status, accepted = request(address, "POST", "/v1/jobs/cgroup-limits/runs")
            assert status == 202, f"run start returned HTTP {status}"
            run_id = accepted["runId"]
            run_ids.append(run_id)
            wait_run(address, run_id, {"running"})
            deadline = time.monotonic() + 5
            while not pid_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert pid_path.exists(), "escaped process-group descendant did not start"
            output = logs(address, run_id)
            assert "membership=/" in output and "conductor-" in output and "/run-" in output, output
            assert "cpu=50000 100000" in output, output
            assert "memory=134217728" in output, output
            assert "swap=0" in output, output
            assert "pids=32" in output, output

            status, _ = request(address, "POST", f"/v1/runs/{run_id}/stop")
            assert status in (200, 202), f"stop returned HTTP {status}"
            assert wait_run(address, run_id, {"cancelled"})["state"] == "cancelled"
            deadline = time.monotonic() + 6
            while list(worker_root.glob("run-*")) and time.monotonic() < deadline:
                time.sleep(0.02)
            assert not list(worker_root.glob("run-*")), "cgroup remained after stop cleanup"

            # A request above the administrator's ceiling must fail without running its command.
            marker = state / "must-not-run"
            restricted = {
                "id": "over-limit",
                "command": ["/bin/sh", "-c", f"/usr/bin/touch {marker}"],
                "cwd": "/tmp",
                "resourceLimits": {"pidsMax": 65},
            }
            status, _ = request(address, "POST", "/v1/jobs", restricted)
            assert status == 201
            status, rejected = request(address, "POST", "/v1/jobs/over-limit/runs")
            assert status == 202, f"over-limit run request returned HTTP {status}"
            rejected_id = rejected["runId"]
            run_ids.append(rejected_id)
            assert wait_run(address, rejected_id, {"failed"})["state"] == "failed"
            assert not marker.exists(), "workload ran despite administrator limit rejection"

            # A daemon crash leaves an ambiguous run; restart kills only the owned subtree.
            wait_worker_slot(address)
            crash_job = {
                "id": "crash-cleanup",
                "command": [
                    "/bin/sh",
                    "-c",
                    "/usr/bin/setsid /bin/sleep 60 & echo $! > \"$TEST_PID_FILE\"; wait",
                ],
                "cwd": "/tmp",
                "env": {"TEST_PID_FILE": str(pid_path)},
                "resourceLimits": {"pidsMax": 32},
            }
            status, _ = request(address, "POST", "/v1/jobs", crash_job)
            assert status == 201
            status, accepted = request(address, "POST", "/v1/jobs/crash-cleanup/runs")
            assert status == 202, f"crash-cleanup run start returned HTTP {status}: {accepted}"
            crash_run = accepted["runId"]
            run_ids.append(crash_run)
            wait_run(address, crash_run, {"running"})
            process.kill()
            process.wait(timeout=5)
            process = launch()
            wait_agent(process, address)
            assert wait_run(address, crash_run, {"unknown"})["state"] == "unknown"
            assert not list(worker_root.glob("run-*")), "restart left an owned cgroup behind"
            print(json.dumps({"status": "passed", "enforcement": "real cgroup v2", "runs": len(run_ids)}))
        except Exception as error:
            diagnostic = agent_log.read_text(encoding="utf-8", errors="replace")
            raise RuntimeError(f"{error}; agent stderr:\n{diagnostic}") from error
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            reap_owned_groups(worker_root)


if __name__ == "__main__":
    main()
