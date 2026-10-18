#!/usr/bin/env python3
"""Verify that a descriptor inherited by the daemon is not passed to jobs."""

from __future__ import annotations

import errno
import http.client
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time


def request(address: tuple[str, int], method: str, path: str, payload: object | None = None):
    body = None if payload is None else json.dumps(payload, separators=(",", ":"))
    connection = http.client.HTTPConnection(*address, timeout=2)
    try:
        connection.request(
            method,
            path,
            body=body,
            headers={"Content-Type": "application/json"} if body is not None else {},
        )
        response = connection.getresponse()
        return response.status, response.read().decode("utf-8")
    finally:
        connection.close()


def read_startup_event(child: subprocess.Popen[str]) -> dict[str, object]:
    if child.stderr is None:
        raise RuntimeError("daemon stderr was not captured")
    selector = selectors.DefaultSelector()
    selector.register(child.stderr, selectors.EVENT_READ)
    buffer = bytearray()
    deadline = time.monotonic() + 5
    try:
        while b"\n" not in buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("daemon startup event deadline expired")
            if not selector.select(remaining):
                continue
            chunk = os.read(child.stderr.fileno(), 4096)
            if not chunk:
                raise RuntimeError(f"daemon exited before startup: {child.poll()}")
            buffer.extend(chunk)
            if len(buffer) > 8192:
                raise RuntimeError("daemon startup event exceeded its size limit")
        line = bytes(buffer).split(b"\n", 1)[0]
        event = json.loads(line)
        if not isinstance(event, dict):
            raise RuntimeError("daemon startup event was not a JSON object")
        return event
    finally:
        selector.close()


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: test-inherited-fds.py PATH_TO_CONDUCTOR")
    binary = Path(sys.argv[1]).resolve()
    if not binary.is_file():
        raise SystemExit(f"conductor binary does not exist: {binary}")

    with tempfile.TemporaryDirectory(prefix="conductor-fd-check-") as temporary:
        root = Path(temporary)
        state = root / "state"
        state.mkdir()
        config = root / "agent.json"
        config.write_text(
            json.dumps(
                {
                    "role": "all",
                    "listen": "127.0.0.1:0",
                    "manager_data_dir": str(state / "manager"),
                    "worker_data_dir": str(state / "worker"),
                    "worker_id": "fd-check",
                    "worker_concurrency": 1,
                    "api_access": {"mode": "local"},
                }
            ),
            encoding="utf-8",
        )
        inherited = (root / "inherited-secret").open("w+")
        inherited.write("descriptor must not reach the job\n")
        inherited.flush()
        os.set_inheritable(inherited.fileno(), True)
        descriptor = inherited.fileno()
        child = subprocess.Popen(
            [str(binary), "agent", "--config", str(config)],
            pass_fds=(descriptor,),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            event = read_startup_event(child)
            if event.get("event") != "agent_started":
                raise RuntimeError(f"unexpected startup event: {event}")
            host, port = event["address"].rsplit(":", 1)
            address = (host.strip("[]"), int(port))

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    status, body = request(address, "GET", "/readyz")
                    if status == 200 and json.loads(body).get("status") == "ready":
                        break
                except (OSError, http.client.HTTPException):
                    pass
                if child.poll() is not None:
                    raise RuntimeError(f"daemon exited during readiness: {child.returncode}")
                time.sleep(0.02)
            else:
                raise TimeoutError("daemon readiness deadline expired")

            code = "\n".join(
                (
                    "import errno, os, sys",
                    "fd = int(sys.argv[1])",
                    "try:",
                    "    os.fstat(fd)",
                    "except OSError as error:",
                    "    raise SystemExit(0 if error.errno == errno.EBADF else 10)",
                    "raise SystemExit(9)",
                )
            )
            status, body = request(
                address,
                "POST",
                "/v1/jobs",
                {
                    "id": "descriptor-check",
                    "command": [sys.executable, "-c", code, str(descriptor)],
                    "cwd": str(root),
                },
            )
            if status != 201:
                raise RuntimeError(f"job registration returned {status}: {body}")
            status, body = request(address, "POST", "/v1/jobs/descriptor-check/runs")
            if status != 202:
                raise RuntimeError(f"run start returned {status}: {body}")
            run_id = json.loads(body)["runId"]

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                status, body = request(address, "GET", f"/v1/runs/{run_id}")
                if status != 200:
                    raise RuntimeError(f"run inspection returned {status}: {body}")
                observation = json.loads(body)
                if observation.get("state") in {"succeeded", "failed"}:
                    if observation["state"] != "succeeded" or observation.get("exitCode") != 0:
                        raise RuntimeError(f"inherited descriptor reached the workload: {observation}")
                    print("Inherited descriptor hygiene passed: job could not access daemon descriptor")
                    return 0
                time.sleep(0.02)
            raise TimeoutError("descriptor-check run did not finish before deadline")
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
                    raise TimeoutError("daemon cleanup exceeded its deadline")
            if child.returncode != 0:
                raise RuntimeError(f"daemon exited during cleanup with {child.returncode}")
            inherited.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Inherited descriptor hygiene failed: {error}", file=sys.stderr)
        raise
