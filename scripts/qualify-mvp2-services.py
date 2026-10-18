#!/usr/bin/env python3
"""Exercise MVP2 service discovery through real manager and worker processes.

This qualification changes the host resolver only on a disposable GitHub-hosted
runner. It restores the previous resolver configuration before returning.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


SUFFIX = "qualification.test"
SEARCH_DOMAIN = f"default.service.{SUFFIX}"
READY_TIMEOUT = 40.0


def request(base: str, method: str, route: str, payload=None, headers=None, timeout=5):
    data = None if payload is None else json.dumps(payload).encode()
    request_headers = dict(headers or {})
    if data is not None:
        request_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + route, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read()
            return response.status, response.headers, json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        body = error.read()
        try:
            decoded = json.loads(body) if body else None
        except json.JSONDecodeError:
            decoded = body.decode(errors="replace")
        return error.code, error.headers, decoded


def wait_until(description, predicate, timeout=READY_TIMEOUT, interval=0.1, timings=None):
    started = time.monotonic()
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            if timings is not None:
                timings[description] = round((time.monotonic() - started) * 1000, 1)
            return last
        time.sleep(interval)
    raise TimeoutError(f"{description} did not become true within {timeout:g}s; last={last!r}")


class Process:
    def __init__(self, command, log_path, cwd):
        self.log_path = Path(log_path)
        self.log = self.log_path.open("wb")
        self.child = subprocess.Popen(
            [str(part) for part in command],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(self, timeout=8):
        if self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGINT)
                self.child.wait(timeout=timeout)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(self.child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.child.wait(timeout=3)
        self.log.close()

    def terminate(self):
        if self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.child.wait(timeout=3)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()


class Resolver:
    """Temporarily point the runner's host resolver at the local Conductor DNS."""

    def __init__(self):
        if os.environ.get("GITHUB_ACTIONS") != "true" or os.geteuid() != 0:
            raise RuntimeError(
                "host resolver qualification is restricted to root on a GitHub-hosted runner"
            )
        self.restore = None

    def __enter__(self):
        try:
            if sys.platform == "darwin":
                self._install_macos()
            elif sys.platform.startswith("linux"):
                self._install_linux()
            else:
                raise RuntimeError(f"unsupported qualification platform: {sys.platform}")
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def _install_linux(self):
        path = Path("/etc/resolv.conf")
        st = path.lstat()
        if path.is_symlink():
            target = os.readlink(path)
            self.restore = ("symlink", target)
        else:
            self.restore = (
                "file", path.read_bytes(), st.st_mode & 0o777, st.st_uid, st.st_gid
            )
        replacement = path.with_name(f".resolv.conf.conductor-{os.getpid()}")
        replacement.write_text(
            f"nameserver 127.0.0.1\nsearch {SEARCH_DOMAIN}\noptions timeout:1 attempts:1\n",
            encoding="ascii",
        )
        os.chmod(replacement, 0o644)
        os.replace(replacement, path)
        self.path = path

    def _install_macos(self):
        resolver_dir = Path("/etc/resolver")
        self.resolver_dir_created = not resolver_dir.exists()
        resolver_dir.mkdir(mode=0o755, exist_ok=True)
        resolver_path = resolver_dir / f"service.{SUFFIX}"
        if resolver_path.is_symlink():
            raise RuntimeError(f"refusing to replace symlink resolver entry {resolver_path}")
        old_stat = resolver_path.stat() if resolver_path.exists() else None
        old = resolver_path.read_bytes() if old_stat else None
        resolver_path.write_text("nameserver 127.0.0.1\n", encoding="ascii")
        self.resolver_path = resolver_path
        self.old_resolver = old
        self.old_resolver_metadata = (
            (old_stat.st_mode & 0o777, old_stat.st_uid, old_stat.st_gid)
            if old_stat
            else None
        )
        self.old_search = []
        services = subprocess.check_output(
            ["networksetup", "-listallnetworkservices"], text=True
        ).splitlines()[1:]
        candidates = [line.removeprefix("*") for line in services if line and not line.startswith("*")]
        if not candidates:
            raise RuntimeError("no enabled macOS network service found for search-domain setup")
        self.network_service = candidates[0]
        current = subprocess.check_output(
            ["networksetup", "-getsearchdomains", self.network_service], text=True
        ).splitlines()
        self.old_search = [] if not current or current[0].startswith("There aren't any") else current
        if SEARCH_DOMAIN not in self.old_search:
            subprocess.run(
                ["networksetup", "-setsearchdomains", self.network_service, *self.old_search, SEARCH_DOMAIN],
                check=True,
                stdout=subprocess.DEVNULL,
            )

    def __exit__(self, *_):
        if self.restore:
            kind, *values = self.restore
            if kind == "symlink":
                temporary = self.path.with_name(f".resolv.conf.restore-{os.getpid()}")
                temporary.symlink_to(values[0])
                os.replace(temporary, self.path)
            else:
                content, mode, owner, group = values
                temporary = self.path.with_name(f".resolv.conf.restore-{os.getpid()}")
                temporary.write_bytes(content)
                os.chown(temporary, owner, group)
                os.chmod(temporary, mode)
                os.replace(temporary, self.path)
        if hasattr(self, "resolver_path"):
            if self.old_resolver is None:
                self.resolver_path.unlink(missing_ok=True)
            else:
                self.resolver_path.write_bytes(self.old_resolver)
                mode, owner, group = self.old_resolver_metadata
                os.chown(self.resolver_path, owner, group)
                os.chmod(self.resolver_path, mode)
            if self.resolver_dir_created and not any(self.resolver_path.parent.iterdir()):
                self.resolver_path.parent.rmdir()
            old_search = getattr(self, "old_search", [])
            if hasattr(self, "network_service") and SEARCH_DOMAIN not in old_search:
                subprocess.run(
                    ["networksetup", "-setsearchdomains", self.network_service, *(old_search or ["Empty"])],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )


def wait_manager(binary, config, log_path, cwd):
    process = Process([binary, "agent", "--config", config], log_path, cwd)
    base = None
    deadline = time.monotonic() + READY_TIMEOUT
    while time.monotonic() < deadline:
        if process.child.poll() is not None:
            detail = startup_failure_detail(log_path)
            raise RuntimeError(f"manager exited early: {detail}")
        try:
            lines = Path(log_path).read_text(errors="replace").splitlines()
            event = next((json.loads(line) for line in lines if '"event":"agent_started"' in line), None)
            if event:
                base = "http://" + event["address"]
                status, _, _ = request(base, "GET", "/healthz")
                if status == 200:
                    return process, base, event
        except (OSError, json.JSONDecodeError, KeyError, urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.05)
    process.terminate()
    detail = startup_failure_detail(log_path)
    raise TimeoutError(f"manager did not become ready within {READY_TIMEOUT:g}s: {detail}")


def startup_failure_detail(log_path):
    try:
        lines = Path(log_path).read_text(errors="replace").splitlines()
    except OSError:
        return "no process log was written"
    for line in reversed(lines):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("severity") == "error" or event.get("event") == "startup_failed":
            return json.dumps(
                {
                    "component": event.get("component"),
                    "event": event.get("event"),
                    "message": event.get("message"),
                },
                sort_keys=True,
            )
    return "no structured startup failure was logged"


def wait_workers(base, wanted, timings=None):
    latest = {}

    def observed():
        status, _, body = request(base, "GET", "/v1/workers")
        latest["status"] = status
        latest["body"] = body
        if status != 200 or not isinstance(body, list):
            return None
        workers = {item.get("workerId"): item for item in body}
        return workers if all(name in workers and workers[name].get("fresh") for name in wanted) else None

    try:
        return wait_until("both standalone workers to enroll", observed, timings=timings)
    except TimeoutError as error:
        raise TimeoutError(f"{error}; latest worker API snapshot={latest}") from error


def binary_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def port(excluded=()):
    for _ in range(16):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            candidate = listener.getsockname()[1]
        if candidate not in excluded:
            return candidate
    raise RuntimeError("could not reserve a unique loopback port")


def reserve_service_networks(excluded=()):
    for start in range(24000, 44000, 32):
        second = start + 20
        if any(start <= candidate < start + 10 or second <= candidate < second + 10 for candidate in excluded):
            continue
        probes = []
        conflict = None
        try:
            for base in (start, second):
                for offset in range(10):
                    listener = socket.socket()
                    listener.bind(("0.0.0.0", base + offset))
                    if base == start and offset == 1:
                        conflict = listener
                    else:
                        probes.append(listener)
            for listener in probes:
                listener.close()
            if conflict is not None:
                return (start, second), conflict
        except OSError:
            for listener in probes:
                listener.close()
            if conflict is not None:
                conflict.close()
    raise RuntimeError("could not reserve two isolated service-port ranges")


def host_service_address():
    """Return the runner's routable IPv4 address without sending a packet."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
        route.connect(("192.0.2.1", 1))
        address = route.getsockname()[0]
    if not address or address.startswith("127.") or address == "0.0.0.0":
        raise RuntimeError(f"could not determine a non-loopback service address: {address!r}")
    return address


def service_attempt_failed(base, service):
    status, _, response = request(base, "GET", f"/v1/jobs/{service}/service-attempts")
    if status != 200:
        return None
    return next(
        (
            attempt
            for attempt in response.get("serviceAttempts", [])
            if attempt.get("currentRun", {}).get("state") == "failed"
        ),
        None,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--profile", choices=("debug", "release"), required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("Conductor binary does not exist")
    dig = shutil.which("dig")
    if not dig:
        parser.error("dig is required for SRV-aware dynamic-port client coverage")

    started_at = datetime.now(timezone.utc).isoformat()
    started_monotonic = time.monotonic()
    result = {
        "startedAtUtc": started_at,
        "binarySha256": binary_sha256(binary),
        "profile": args.profile,
        "platform": platform.platform(),
        "system": platform.system(),
        "machine": platform.machine(),
        "dnsSuffix": SUFFIX,
        "namespace": "default",
        "topology": "one manager process and two standalone worker processes on one host",
        "serviceNameCommunication": "system resolver FQDN and search-domain short names; SRV dynamic ports",
        "timingsMs": {},
        "scenarios": [],
    }
    with tempfile.TemporaryDirectory(prefix="conductor-mvp2-services-") as temporary, ExitStack() as cleanup:
        root = Path(temporary)
        transport_port = port()
        api_port = port((transport_port,))
        service_ranges, conflict_socket = reserve_service_networks((transport_port, api_port))
        service_address = host_service_address()
        cleanup.enter_context(conflict_socket)
        conflict_port = conflict_socket.getsockname()[1]
        manager_data = root / "manager-data"
        manager_identity = root / "manager-identity"
        dns_port = 53
        manager_config = {
            "role": "manager",
            "listen": f"127.0.0.1:{api_port}",
            "worker_transport_listen": f"127.0.0.1:{transport_port}",
            "manager_data_dir": str(manager_data),
            "manager_identity_dir": str(manager_identity),
            "manager_server_name": "localhost",
            "worker_id": "mvp2-worker-a",
            "worker_transport_capacity": 4,
            "worker_resource_domain_id": "mvp2-host",
            "worker_labels": {"suite": "mvp2"},
            "worker_service_network": {
                "networkDomainId": "mvp2-loopback",
                "portRangeStart": service_ranges[0],
                "portRangeEnd": service_ranges[0] + 9,
                "advertisedAddress": service_address,
            },
            "additional_workers": [
                {
                    "worker_id": "mvp2-worker-b",
                    "capacity": 4,
                    "resource_domain_id": "mvp2-host",
                    "labels": {"suite": "mvp2"},
                    "service_network": {
                        "networkDomainId": "mvp2-loopback",
                        "portRangeStart": service_ranges[1],
                        "portRangeEnd": service_ranges[1] + 9,
                        "advertisedAddress": service_address,
                    },
                }
            ],
            "host_resource_domains": [
                {"id": "mvp2-host", "cpu_millis": 2000, "memory_bytes": 2147483648}
            ],
            "service_dns": {"listen": f"127.0.0.1:{dns_port}", "suffix": SUFFIX},
            "api_access": {"mode": "local"},
        }
        manager_path = root / "manager.json"
        manager_path.write_text(json.dumps(manager_config), encoding="utf-8")

        with Resolver():
            manager, base, started = wait_manager(binary, manager_path, root / "manager.log", root)
            processes = [manager]
            try:
                result["managerApiAddress"] = started["address"]
                result["workerTransportAddress"] = started["workerTransportAddress"]
                result["serviceDnsAddress"] = started.get("serviceDnsAddress", "127.0.0.1:53")
                result["serviceAddress"] = service_address
                ca_file = manager_identity / "worker-ca.pem"
                worker_names = ("mvp2-worker-a", "mvp2-worker-b")
                for index, worker_name in enumerate(worker_names):
                    credential = root / f"{worker_name}.credential"
                    enrollment = subprocess.run(
                        [binary, "enroll-worker", "--manager-data-dir", manager_data,
                         "--identity-dir", manager_identity, "--worker-id", worker_name,
                         "--output", credential],
                        cwd=root, capture_output=True, text=True, timeout=5,
                    )
                    if enrollment.returncode:
                        raise RuntimeError(f"worker enrollment failed: {enrollment.stderr.strip()}")
                    worker_dir = root / worker_name
                    start = service_ranges[index]
                    worker_config = {
                        "role": "worker",
                        "worker_data_dir": str(worker_dir),
                        "worker_id": worker_name,
                        "worker_concurrency": 4,
                        "manager_endpoint": f"https://localhost:{transport_port}/",
                        "worker_identity_file": str(worker_dir / "identity.pem"),
                        "worker_enrollment_credential_file": str(credential),
                        "manager_ca_file": str(ca_file),
                    }
                    config_path = root / f"{worker_name}.json"
                    config_path.write_text(json.dumps(worker_config), encoding="utf-8")
                    processes.append(Process([binary, "agent", "--config", config_path],
                                             root / f"{worker_name}.log", root))
                wait_workers(base, worker_names, timings=result["timingsMs"])
                readiness = {}

                def manager_ready():
                    status, _, body = request(base, "GET", "/readyz")
                    readiness.update(status=status, body=body)
                    return readiness if status == 200 else None

                try:
                    wait_until(
                        "manager readiness after worker enrollment",
                        manager_ready,
                        timings=result["timingsMs"],
                    )
                except TimeoutError as error:
                    raise TimeoutError(
                        f"{error}; latest manager readiness response={readiness}"
                    ) from error
                result["scenarios"].append("two enrolled standalone workers report fresh status")

                for service in ("frontend", "backend"):
                    unknown = f"{service}.default.service.{SUFFIX}."
                    if system_resolves(unknown):
                        raise RuntimeError(f"uncreated service unexpectedly resolves: {unknown}")
                result["scenarios"].append("unknown service names return through the host resolver before deployment")

                service_program = root / "service.py"
                service_program.write_text(SERVICE_PROGRAM, encoding="utf-8")
                create_service_revision(root, base, service_program, "port-conflict", "backend", 1)
                deploy_service(base, "port-conflict", 1, create=True, key="mvp2-port-conflict-deploy")
                wait_until(
                    "first service attempt to report the reserved port conflict",
                    lambda: service_attempt_failed(base, "port-conflict"),
                    timeout=8,
                    timings=result["timingsMs"],
                )
                conflict_endpoint = wait_service(
                    base, "port-conflict", timings=result["timingsMs"]
                )[0]
                if conflict_endpoint["workerId"] != worker_names[0]:
                    raise RuntimeError("port-conflict service was not placed on the reserved worker")
                if conflict_endpoint["port"] == conflict_port:
                    raise RuntimeError("port conflict retry reused the reserved host port")
                result["scenarios"].append("a real bind conflict failed its first run and retry used a different host port")

                services = ("frontend", "backend")
                for service, peer in (("frontend", "backend"), ("backend", "frontend")):
                    create_service_revision(root, base, service_program, service, peer, 1)
                    deploy_service(base, service, 1, create=True, key=f"mvp2-{service}-deploy-1")
                endpoints = {
                    name: wait_service(base, name, timings=result["timingsMs"])
                    for name in services
                }
                for service in services:
                    wait_until(
                        f"host resolver positive answer for {service}",
                        lambda service=service: system_resolves(
                            f"{service}.default.service.{SUFFIX}."
                        ),
                        timeout=8,
                        interval=0.1,
                        timings=result["timingsMs"],
                    )
                    wait_until(
                        f"host resolver search answer for {service}",
                        lambda service=service: system_resolves(service),
                        timeout=8,
                        interval=0.1,
                        timings=result["timingsMs"],
                    )
                result["scenarios"].append("host resolver negative answers converged to positive FQDN and search-name answers")
                worker_ids = {name: endpoints[name][0]["workerId"] for name in services}
                if worker_ids["frontend"] == worker_ids["backend"]:
                    raise RuntimeError(f"services did not land on distinct workers: {worker_ids}")
                result["serviceWorkers"] = worker_ids
                result["scenarios"].append("frontend and backend ready on different workers")

                for service, peer in (("frontend", "backend"), ("backend", "frontend")):
                    request_started = time.monotonic()
                    response = call_service(endpoints[service][0], peer)
                    result["timingsMs"][f"{service} to {peer} application request"] = round(
                        (time.monotonic() - request_started) * 1000, 1
                    )
                    expected = f"{peer}:v1"
                    if response != expected:
                        raise RuntimeError(f"{service} received {response!r}; expected {expected!r}")
                result["scenarios"].append(
                    "bidirectional application requests resolved FQDNs and short names through the host resolver and connected using SRV targets"
                )

                create_service_revision(
                    root, base, service_program, "unready", "backend", 1,
                    version="v1-unhealthy",
                )
                deploy_service(base, "unready", 1, create=True, key="mvp2-unready-deploy")
                wait_until(
                    "failed readiness probe to hold deployment pending",
                    lambda: deployment_pending_for_readiness(base, "unready"),
                    timeout=8,
                    timings=result["timingsMs"],
                )
                status, _, response = request(base, "GET", "/v1/jobs/unready/endpoints")
                if status != 200 or response.get("endpoints"):
                    raise RuntimeError(f"unready service published an endpoint: {response}")
                status, deployment_headers, _ = request(base, "GET", "/v1/jobs/unready/deployment")
                if status != 200:
                    raise RuntimeError(f"reading unready deployment returned {status}")
                status, _, deleted = request(
                    base, "DELETE", "/v1/jobs/unready/deployment",
                    headers={"If-Match": deployment_headers["ETag"], "Idempotency-Key": "mvp2-unready-delete"},
                )
                if status not in (200, 202, 204):
                    raise RuntimeError(f"deleting unready deployment returned {status}: {deleted}")
                result["scenarios"].append("failed readiness probes keep service endpoints unpublished")
                wait_until(
                    "unready deployment cleanup",
                    lambda: request(base, "GET", "/v1/jobs/unready/deployment")[0] == 404,
                    timeout=8,
                    timings=result["timingsMs"],
                )

                create_service_revision(
                    root, base, service_program, "pressure", "backend", 1,
                    cpu_millis=3000,
                )
                deploy_service(base, "pressure", 1, create=True, key="mvp2-pressure-deploy")
                wait_until(
                    "resource pressure to remain explicitly pending",
                    lambda: deployment_pending_for_reason(
                        base, "pressure", "shared_host_capacity_exhausted"
                    ),
                    timeout=8,
                    timings=result["timingsMs"],
                )
                result["scenarios"].append(
                    "shared-host resource pressure blocks placement without exceeding the configured budget"
                )

                old_backend = endpoints["backend"][0]
                create_service_revision(root, base, service_program, "backend", "frontend", 2)
                deploy_service(base, "backend", 2, create=False, key="mvp2-backend-deploy-2")
                new_backend = wait_service(base, "backend", revision=2, timings=result["timingsMs"])[0]
                if new_backend["port"] == old_backend["port"]:
                    raise RuntimeError("backend replacement retained the old dynamic port")
                if call_service(endpoints["frontend"][0], "backend") != "backend:v2":
                    raise RuntimeError("frontend did not discover the backend replacement")
                result["scenarios"].append("revision replacement changed endpoint and service name remained stable")

                manager.stop()
                restart_started = time.monotonic()
                dns_during_outage = subprocess.run(
                    [dig, "+time=1", "+tries=1", "A",
                     f"frontend.default.service.{SUFFIX}.", "@127.0.0.1"],
                    capture_output=True, text=True, timeout=3,
                )
                if "ANSWER SECTION" in dns_during_outage.stdout:
                    raise RuntimeError("service DNS answered while its manager process was stopped")
                result["scenarios"].append("manager outage stopped DNS answers; no manager failover is implied")
                manager, base, _ = wait_manager(
                    binary, manager_path, root / "manager-restarted.log", root
                )
                processes[0] = manager
                wait_workers(base, worker_names, timings=result["timingsMs"])
                wait_service(base, "backend", revision=2, timings=result["timingsMs"])
                result["timingsMs"]["manager restart and worker reconnect"] = round(
                    (time.monotonic() - restart_started) * 1000, 1
                )
                if call_service(endpoints["frontend"][0], "backend") != "backend:v2":
                    raise RuntimeError("service-name request failed after manager restart and worker reconnect")
                result["scenarios"].append(
                    "manager process restart recovered catalog state and workers reconnected"
                )

                drained_worker = new_backend["workerId"]
                status, _, drain = request(
                    base, "POST", f"/v1/workers/{drained_worker}/drain",
                    {"gracePeriodSeconds": 5},
                    {"Idempotency-Key": "mvp2-drain-backend-worker"},
                )
                if status not in (200, 202):
                    raise RuntimeError(f"draining {drained_worker} returned {status}: {drain}")
                moved_backend = wait_until(
                    "backend replacement on the surviving worker",
                    lambda: endpoint_if_on_other_worker(base, "backend", drained_worker),
                    timings=result["timingsMs"],
                )
                if call_service(endpoints["frontend"][0], "backend") != "backend:v2":
                    raise RuntimeError("frontend did not discover backend after worker drain")
                result["scenarios"].append(
                    f"draining {drained_worker} moved backend to {moved_backend[0]['workerId']} without changing its service name"
                )
            finally:
                for process in reversed(processes):
                    process.stop()
    result["durationMs"] = round((time.monotonic() - started_monotonic) * 1000, 1)
    report = json.dumps(result, sort_keys=True)
    print(report)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("\n## MVP2 service qualification\n\n```json\n")
            summary.write(json.dumps(result, indent=2, sort_keys=True))
            summary.write("\n```\n")


def create_service_revision(
    root, base, script, service, peer, revision, version=None, cpu_millis=100
):
    digest = binary_sha256(sys.executable)
    body = {
        "definition": {
            "id": service,
            "command": [sys.executable, "-u", str(script), service, peer, version or f"v{revision}", SUFFIX, SEARCH_DOMAIN, shutil.which("dig")],
            "cwd": str(root),
            "terminationGraceMs": 1000,
        },
        "workload": {
            "kind": "service",
            "identity": {"namespace": "default", "name": service},
            "ports": [{"name": "http", "protocol": "tcp"}],
            "readinessProbe": {
                "portName": "http", "protocol": "http", "path": "/health",
                "intervalMs": 100, "timeoutMs": 50, "startupGraceMs": 0,
                "successThreshold": 1, "failureThreshold": 3,
            },
        },
        "executableDigest": {"algorithm": "sha256", "value": digest},
        "executionRequirements": {},
        "resourceRequest": {"cpuMillis": cpu_millis, "memoryBytes": 134217728},
    }
    status, _, response = request(base, "POST", f"/v1/jobs/{service}/revisions", body,
                                  {"Idempotency-Key": f"mvp2-{service}-revision-{revision}"})
    if status != 201:
        raise RuntimeError(f"creating {service} revision {revision} returned {status}: {response}")


def deploy_service(base, service, revision, create, key):
    headers = {"Idempotency-Key": key}
    if create:
        headers["If-None-Match"] = "*"
    else:
        status, current_headers, current = request(base, "GET", f"/v1/jobs/{service}/deployment")
        if status != 200:
            raise RuntimeError(f"reading {service} deployment returned {status}: {current}")
        headers["If-Match"] = current_headers["ETag"]
    status, _, response = request(
        base, "PUT", f"/v1/jobs/{service}/deployment",
        {"revision": revision, "replicas": 1}, headers,
    )
    if status not in (200, 201, 202):
        raise RuntimeError(f"deploying {service} revision {revision} returned {status}: {response}")


def wait_service(base, service, revision=1, timings=None):
    def ready():
        status, _, deployment = request(base, "GET", f"/v1/jobs/{service}/deployment")
        if status != 200 or deployment.get("revisionId", {}).get("revision") != revision:
            return None
        if deployment.get("status") != "ready":
            return None
        status, _, response = request(base, "GET", f"/v1/jobs/{service}/endpoints")
        if status != 200 or not response.get("endpoints"):
            return None
        return response["endpoints"]

    return wait_until(f"service {service} revision {revision}", ready, timings=timings)


def endpoint_if_on_other_worker(base, service, excluded_worker):
    try:
        status, _, deployment = request(base, "GET", f"/v1/jobs/{service}/deployment")
        if status != 200 or deployment.get("status") != "ready":
            return None
        status, _, response = request(base, "GET", f"/v1/jobs/{service}/endpoints")
        if status != 200:
            return None
        endpoints = response.get("endpoints", [])
    except (urllib.error.URLError, TimeoutError, AttributeError):
        return None
    if len(endpoints) == 1 and endpoints[0]["workerId"] != excluded_worker:
        return endpoints


def deployment_pending_for_readiness(base, service):
    return deployment_pending_for_reason(base, service, "service_readiness_pending")


def deployment_pending_for_reason(base, service, reason):
    status, _, deployment = request(base, "GET", f"/v1/jobs/{service}/deployment")
    return (
        deployment
        if status == 200
        and deployment.get("status") == "pending"
        and deployment.get("pendingReason") == reason
        else None
    )
    return None


def system_resolves(name):
    try:
        return bool(socket.getaddrinfo(name, None, type=socket.SOCK_STREAM))
    except socket.gaierror:
        return False


def call_service(endpoint, peer):
    address = endpoint["advertisedAddress"]
    port_number = endpoint["port"]
    conn = http.client.HTTPConnection(address, port_number, timeout=5)
    try:
        conn.request("GET", f"/call/{peer}", headers={"Host": f"{endpoint['serviceName']}.default.service.{SUFFIX}"})
        response = conn.getresponse()
        body = response.read().decode()
        if response.status != 200:
            raise RuntimeError(f"service call returned HTTP {response.status}: {body}")
        return body
    finally:
        conn.close()


SERVICE_PROGRAM = r'''#!/usr/bin/env python3
import os
import socket
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

service, peer, version, suffix, search_domain, dig = sys.argv[1:]
port = int(os.environ["CONDUCTOR_PORT_HTTP"])
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            status = 503 if version.endswith("-unhealthy") else 200
            self.send_response(status); self.end_headers(); self.wfile.write(b"ready"); return
        if self.path == "/identity":
            self.send_response(200); self.end_headers(); self.wfile.write(f"{service}:{version}".encode()); return
        if self.path == f"/call/{peer}":
            socket.getaddrinfo(f"{peer}.default.service.{suffix}.", None, type=socket.SOCK_STREAM)
            socket.getaddrinfo(peer, None, type=socket.SOCK_STREAM)
            answer = subprocess.run([dig, "+short", "SRV", f"_http._tcp.{peer}.default.service.{suffix}.", "@127.0.0.1"],
                                    check=True, capture_output=True, text=True, timeout=3).stdout.splitlines()
            if not answer:
                self.send_error(503, "SRV record unavailable"); return
            _, _, peer_port, target = answer[0].split()
            target = target.rstrip(".")
            with socket.create_connection((target, int(peer_port)), timeout=3) as remote:
                remote.sendall(f"GET /identity HTTP/1.1\r\nHost: {peer}.default.service.{suffix}\r\nConnection: close\r\n\r\n".encode())
                chunks = []
                while True:
                    chunk = remote.recv(4096)
                    if not chunk: break
                    chunks.append(chunk)
            payload = b"".join(chunks).split(b"\r\n\r\n", 1)[-1].decode().strip()
            self.send_response(200); self.end_headers(); self.wfile.write(payload.encode()); return
        self.send_error(404)

    def log_message(self, *_):
        pass

ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
'''


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"MVP2 service qualification failed: {error}", file=sys.stderr)
        raise
