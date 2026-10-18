#!/usr/bin/env python3
"""Exercise real Landlock policy, inheritance, and filesystem operation boundaries."""

import argparse
import base64
import http.client
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


FIXTURE_SOURCE = r"""
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int denied(const char *path, int flags) {
  errno = 0;
  int fd = open(path, flags);
  if (fd >= 0) { close(fd); return 0; }
  return errno == EACCES || errno == EPERM;
}

static int no_unintended_fds(void) {
  long limit = sysconf(_SC_OPEN_MAX);
  if (limit < 0 || limit > 4096) limit = 4096;
  for (int fd = 3; fd < limit; ++fd)
    if (fcntl(fd, F_GETFD) >= 0 || errno != EBADF) return 0;
  return 1;
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "--sleep") == 0)
    for (;;) sleep(60);
  if (argc == 3 && strcmp(argv[1], "--remove-lock-and-hold") == 0) {
    errno = 0;
    int removed = unlink(argv[2]);
    int unlink_denied = removed < 0 && (errno == EACCES || errno == EPERM);
    printf("ownership_lock_unlink_denied=%d\n", unlink_denied);
    errno = 0;
    int replacement = open(argv[2], O_CREAT | O_EXCL | O_WRONLY, 0600);
    int replacement_denied = replacement < 0 && (errno == EACCES || errno == EPERM || errno == EEXIST);
    if (replacement >= 0) { close(replacement); unlink(argv[2]); }
    printf("ownership_lock_replacement_denied=%d\n", replacement_denied);
    fflush(stdout);
    if (!unlink_denied || !replacement_denied) return 30;
    for (;;) sleep(60);
  }
  if (argc == 2 && strcmp(argv[1], "--hold") == 0) {
    sleep(1);
    return 0;
  }
  if (argc == 3 && strcmp(argv[1], "--child") == 0)
    return denied(argv[2], O_RDONLY) ? 0 : 21;
  if (argc != 5) return 2;
  printf("no_unintended_fds=%d\n", no_unintended_fds());
  char buffer[64] = {0};
  int fd = open(argv[1], O_RDONLY);
  if (fd < 0 || read(fd, buffer, sizeof(buffer) - 1) <= 0) return 3;
  close(fd);
  if (strcmp(buffer, "approved-input\n") != 0) return 4;
  printf("approved_input_read=1\n");
  printf("input_truncate_denied=%d\n", denied(argv[1], O_WRONLY | O_TRUNC));
  printf("input_remove_denied=%d\n", unlink(argv[1]) < 0 && (errno == EACCES || errno == EPERM));
  printf("input_rename_denied=%d\n", rename(argv[1], argv[2]) < 0 && (errno == EACCES || errno == EPERM));
  printf("state_read_denied=%d\n", denied(argv[3], O_RDONLY));
  printf("cgroup_read_denied=%d\n", denied(argv[4], O_RDONLY));
  char output[4096];
  if (snprintf(output, sizeof(output), "%s/result.txt", argv[2]) >= (int)sizeof(output)) return 5;
  fd = open(output, O_CREAT | O_WRONLY | O_TRUNC, 0600);
  if (fd < 0 || write(fd, "approved-output\n", 16) != 16) return 6;
  close(fd);
  printf("approved_output_write=1\n");
  pid_t child = fork();
  if (child < 0) return 7;
  if (child == 0) {
    char *child_args[] = {argv[0], "--child", argv[3], NULL};
    execv(argv[0], child_args);
    _exit(22);
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) return 8;
  printf("child_exec_inherited_denial=1\n");
  return 0;
}
"""

PRELOAD_SOURCE = r"""
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <sys/types.h>
#include <unistd.h>

static void write_marker(const char *path, const char *value) {
  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0600);
  if (fd >= 0) {
    const char *cursor = value;
    size_t remaining = strlen(value);
    while (remaining > 0) {
      ssize_t written = write(fd, cursor, remaining);
      if (written <= 0) break;
      cursor += written;
      remaining -= (size_t)written;
    }
    close(fd);
  }
}

__attribute__((constructor)) static void probe_launcher_environment(void) {
  const char *role = getenv("CONDUCTOR_TEST_ROLE");
  if (!role || strcmp(role, "workload") != 0) return;
  const char *workload = getenv("CONDUCTOR_TEST_WORKLOAD");
  const char *secret = getenv("CONDUCTOR_TEST_SECRET");
  const char *output = getenv("CONDUCTOR_TEST_OUTPUT");
  const char *execfn = (const char *)getauxval(AT_EXECFN);
  if (!workload || !secret || !output) return;

  if (!execfn || strcmp(execfn, workload) != 0) {
    int fd = open(secret, O_RDONLY);
    if (fd >= 0) {
      close(fd);
      const char *marker = getenv("CONDUCTOR_TEST_EARLY_READ_MARKER");
      if (marker) write_marker(marker, "trusted-launcher-read-protected-file\n");
    }
    pid_t child = fork();
    if (child == 0) {
      sleep(4);
      _exit(0);
    }
    if (child > 0) {
      char path[4096];
      if (snprintf(path, sizeof(path), "%s/early-child.pid", output) > 0) {
        char value[32];
        int length = snprintf(value, sizeof(value), "%ld\n", (long)child);
        if (length > 0) write_marker(path, value);
      }
    }
    return;
  }

  int fd = open(secret, O_RDONLY);
  if (fd >= 0) {
    close(fd);
    const char *marker = getenv("CONDUCTOR_TEST_EARLY_READ_MARKER");
    if (marker) write_marker(marker, "workload-read-protected-file\n");
  } else {
    char path[4096];
    if (snprintf(path, sizeof(path), "%s/protected-read-denied", output) > 0)
      write_marker(path, "denied\n");
  }

  pid_t child = fork();
  if (child == 0) {
    char path[4096];
    if (snprintf(path, sizeof(path), "%s/sandbox-child.pid", output) > 0) {
      char value[32];
      int length = snprintf(value, sizeof(value), "%ld\n", (long)getpid());
      if (length > 0) write_marker(path, value);
    }
    sleep(4);
    _exit(0);
  }
}
"""


def request(address, method, route, payload=None):
    body = None if payload is None else json.dumps(payload, separators=(",", ":"))
    connection = http.client.HTTPConnection(*address, timeout=35)
    try:
        headers = {} if body is None else {"Content-Type": "application/json"}
        connection.request(method, route, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        return response.status, json.loads(raw) if raw else None
    finally:
        connection.close()


def wait_agent(process, address):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Conductor exited during Landlock integration coverage")
        try:
            status, body = request(address, "GET", "/readyz")
            if status == 200 and body["status"] == "ready":
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
    raise TimeoutError(f"run did not reach {sorted(expected)}: {last}")


def output(address, run_id, stream="stdout"):
    status, body = request(address, "GET", f"/v1/runs/{run_id}/logs?stream={stream}&limit=65536")
    if status != 200:
        raise RuntimeError(f"run logs returned HTTP {status}")
    return base64.b64decode(body["data"], validate=True).decode("utf-8")


def reserve_address():
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        return reserved.getsockname()


def wait_cgroups_empty(worker_root, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not list(worker_root.glob("run-*")):
            return
        time.sleep(0.02)
    raise TimeoutError("Landlock run left an owned cgroup behind")


def reap_owned_groups(worker_root):
    if not worker_root.exists():
        return
    for group in worker_root.glob("run-*"):
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


def worker_cgroup_root(delegated_root, worker_id):
    value = 0xCBF29CE484222325
    for byte in worker_id.encode():
        value ^= byte
        value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return delegated_root / f"conductor-{value:016x}"


def ownership_lock_file(lock_directory, delegated_root, worker_id):
    namespace_name = worker_cgroup_root(delegated_root, worker_id).name
    value = 0xCBF29CE484222325
    for byte in os.fsencode(str(delegated_root.resolve())) + b"\0" + namespace_name.encode():
        value ^= byte
        value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return lock_directory / f"conductor-cgroup-{value:016x}.lock"


def runtime_paths(binary):
    result = subprocess.run(["ldd", str(binary)], check=True, capture_output=True, text=True)
    paths = set()
    for token in re.findall(r"(?:=>\s+)?(/[^\s()]+)", result.stdout):
        path = Path(token)
        if path.exists():
            paths.add(path.resolve(strict=True))
    if not paths:
        raise RuntimeError("could not identify dynamic executable dependencies")
    return sorted(paths)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/conductor"))
    parser.add_argument("--delegated-root", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("real Landlock integration coverage requires Linux")
    if not args.binary.is_file():
        parser.error("release binary does not exist")
    delegated_root = args.delegated_root.resolve(strict=True)
    if not (delegated_root / "cgroup.procs").is_file():
        raise RuntimeError("delegated cgroup control path is unavailable")

    with tempfile.TemporaryDirectory(prefix="conductor-landlock-") as temporary, tempfile.TemporaryDirectory(
        prefix="conductor-landlock-locks-"
    ) as lock_temporary:
        root = Path(temporary)
        lock_parent = Path(lock_temporary)
        lock_runtime = lock_parent / "secure" / "runtime"
        lock_runtime.mkdir(parents=True)
        lock_target_child = lock_runtime / "child"
        lock_target_child.mkdir()
        lock_directory = lock_runtime / "locks"
        lock_directory.mkdir(mode=0o700)
        lock_child = lock_directory / "nested"
        lock_child.mkdir()
        lock_alias = root / "ownership-lock-alias"
        lock_alias.symlink_to(lock_directory, target_is_directory=True)
        lock_parent_alias_dir = root / "ownership-lock-parent-alias"
        lock_parent_alias_dir.mkdir()
        lock_parent_alias = lock_parent_alias_dir / "alias"
        lock_parent_alias.symlink_to(lock_target_child, target_is_directory=True)
        lock_parent_traversal = lock_parent_alias / "../locks"
        manager = root / "manager"
        worker = root / "worker"
        manager.mkdir(mode=0o700)
        worker.mkdir(mode=0o700)
        secret = manager / "credential-fixture"
        secret.write_text("not-for-jobs\n", encoding="utf-8")
        secret.chmod(0o644)
        source = root / "fixture.c"
        source.write_text(FIXTURE_SOURCE, encoding="utf-8")
        fixture = root / "fixture"
        subprocess.run(["cc", "-O2", "-Wall", "-Wextra", "-Werror", str(source), "-o", str(fixture)], check=True)
        preload_source = root / "preload.c"
        preload_source.write_text(PRELOAD_SOURCE, encoding="utf-8")
        preload = root / "preload.so"
        subprocess.run(
            ["cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror", str(preload_source), "-o", str(preload)],
            check=True,
        )

        input_path = root / "input.txt"
        input_path.write_text("approved-input\n", encoding="utf-8")
        input_alias = root / "input-alias"
        input_alias.symlink_to(input_path)
        output_dir = root / "output"
        output_dir.mkdir(mode=0o700)

        grants = [
            {"id": "fixture", "path": str(fixture), "read": True, "execute": True},
            {"id": "preload", "path": str(preload), "read": True},
            {"id": "input", "path": str(input_alias), "read": True},
            {"id": "output", "path": str(output_dir), "write": True},
        ]
        for index, dependency in enumerate(runtime_paths(fixture)):
            loader = "ld-linux" in dependency.name or "ld-musl" in dependency.name
            grants.append({"id": f"runtime-{index}", "path": str(dependency), "read": True, "execute": loader})

        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        address = ("127.0.0.1", port)
        config = {
            "role": "all",
            "listen": f"127.0.0.1:{port}",
            "manager_data_dir": str(manager),
            "worker_data_dir": str(worker),
            "worker_id": "landlock-test",
            "worker_concurrency": 2,
            "api_access": {"mode": "local"},
            "cgroup_v2": {
                "enabled": True,
                "delegatedRoot": str(delegated_root),
                "ownershipLockDir": str(lock_directory),
                "maximums": {"pidsMax": 32},
            },
            "landlock": {"mode": "required", "grants": grants},
        }
        config_path = root / "agent.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        agent_log = root / "agent.stderr.log"

        # Prove the daemon identity can see the protected fixtures before Landlock applies.
        secret.read_text(encoding="utf-8")
        (delegated_root / "cgroup.procs").read_text(encoding="ascii")

        def launch():
            inherited_secret_fd = os.open(secret, os.O_RDONLY)
            try:
                environment = os.environ.copy()
                environment["LD_PRELOAD"] = str(preload)
                environment["CONDUCTOR_TEST_ROLE"] = "daemon"
                return subprocess.Popen(
                    [str(args.binary.resolve()), "agent", "--config", str(config_path)],
                    cwd=root,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=agent_log.open("ab"),
                    pass_fds=(inherited_secret_fd,),
                )
            finally:
                os.close(inherited_secret_fd)

        process = launch()
        try:
            wait_agent(process, address)
            job = {
                "id": "landlock-fixture",
                "command": [str(fixture), str(input_path), str(output_dir), str(secret), str(delegated_root / "cgroup.procs")],
                "cwd": str(root),
                "filesystemPolicy": {"grants": [grant["id"] for grant in grants]},
            }
            status, _ = request(address, "POST", "/v1/jobs", job)
            assert status == 201, f"fixture registration returned HTTP {status}"
            status, accepted = request(address, "POST", "/v1/jobs/landlock-fixture/runs")
            assert status == 202, f"fixture run start returned HTTP {status}: {accepted}"
            run_id = accepted["runId"]
            outcome = wait_run(address, run_id, {"succeeded", "failed"})
            log = output(address, run_id)
            error_log = output(address, run_id, "stderr")
            assert outcome["state"] == "succeeded", f"fixture failed: {outcome}; stdout={log}; stderr={error_log}; agent={agent_log.read_text(errors='replace')}"
            for evidence in (
                "approved_input_read=1",
                "input_truncate_denied=1",
                "input_remove_denied=1",
                "input_rename_denied=1",
                "state_read_denied=1",
                "cgroup_read_denied=1",
                "approved_output_write=1",
                "child_exec_inherited_denial=1",
                "no_unintended_fds=1",
            ):
                assert evidence in log, f"missing enforcement evidence {evidence}: {log}"
            assert (output_dir / "result.txt").read_text(encoding="utf-8") == "approved-output\n"
            grant_ids = [grant["id"] for grant in grants]
            worker_root = worker_cgroup_root(delegated_root, config["worker_id"])

            # Explicit and inherited loader variables must reach only the target exec,
            # after it is already in its cgroup and under Landlock.
            for job_id, inherit_env in (("preload-explicit", False), ("preload-inherited", True)):
                preload_output = output_dir
                for name in ("sandbox-child.pid", "early-child.pid", "protected-read-denied"):
                    (preload_output / name).unlink(missing_ok=True)
                early_read = root / f"{job_id}-early-read"
                env = {
                    "CONDUCTOR_TEST_ROLE": "workload",
                    "CONDUCTOR_TEST_WORKLOAD": str(fixture),
                    "CONDUCTOR_TEST_SECRET": str(secret),
                    "CONDUCTOR_TEST_OUTPUT": str(preload_output),
                    "CONDUCTOR_TEST_EARLY_READ_MARKER": str(early_read),
                }
                if not inherit_env:
                    env["LD_PRELOAD"] = str(preload)
                loader_job = {
                    "id": job_id,
                    "command": [str(fixture), "--hold"],
                    "cwd": str(root),
                    "inheritEnv": inherit_env,
                    "env": env,
                    "filesystemPolicy": {"grants": grant_ids},
                }
                status, _ = request(address, "POST", "/v1/jobs", loader_job)
                assert status == 201, f"{job_id} registration returned HTTP {status}"
                status, accepted = request(address, "POST", f"/v1/jobs/{job_id}/runs")
                assert status == 202, f"{job_id} start returned HTTP {status}"
                run_id = accepted["runId"]
                child_marker = preload_output / "sandbox-child.pid"
                deadline = time.monotonic() + 1
                while time.monotonic() < deadline and not child_marker.exists():
                    time.sleep(0.01)
                assert child_marker.is_file(), f"{job_id} preload constructor did not run in the workload"
                assert (preload_output / "protected-read-denied").read_text(encoding="utf-8") == "denied\n"
                assert not early_read.exists(), f"{job_id} preload constructor read protected state before enforcement"
                early_child = preload_output / "early-child.pid"
                if early_child.exists():
                    os.kill(int(early_child.read_text(encoding="ascii")), signal.SIGKILL)
                    raise AssertionError(f"{job_id} preload constructor ran in the trusted launcher")
                child_pid = child_marker.read_text(encoding="ascii").strip()
                deadline = time.monotonic() + 1
                contained = False
                while time.monotonic() < deadline:
                    for group in worker_root.glob("run-*"):
                        if child_pid in (group / "cgroup.procs").read_text(encoding="ascii").splitlines():
                            contained = True
                            break
                    if contained:
                        break
                    time.sleep(0.01)
                assert contained, f"{job_id} preload descendant was not placed in the run cgroup"
                outcome = wait_run(address, run_id, {"succeeded", "failed"})
                assert outcome["state"] == "succeeded", f"{job_id} workload failed: {outcome}"
                wait_cgroups_empty(worker_root)

            lock_file = ownership_lock_file(lock_directory, delegated_root, config["worker_id"])
            lock_identity = lock_file.stat()
            lock_job = {
                "id": "landlock-lock-protection",
                "command": [str(fixture), "--remove-lock-and-hold", str(lock_file)],
                "cwd": str(root),
                "filesystemPolicy": {"grants": grant_ids},
            }
            status, _ = request(address, "POST", "/v1/jobs", lock_job)
            assert status == 201, f"ownership lock job registration returned HTTP {status}"
            status, accepted = request(address, "POST", "/v1/jobs/landlock-lock-protection/runs")
            assert status == 202, f"ownership lock run start returned HTTP {status}: {accepted}"
            lock_run_id = accepted["runId"]
            wait_run(address, lock_run_id, {"running"})
            deadline = time.monotonic() + 5
            evidence = "ownership_lock_unlink_denied=1" in output(address, lock_run_id)
            while not evidence and time.monotonic() < deadline:
                time.sleep(0.02)
                evidence = "ownership_lock_unlink_denied=1" in output(address, lock_run_id)
            lock_log = output(address, lock_run_id)
            assert "ownership_lock_unlink_denied=1" in lock_log, lock_log
            assert "ownership_lock_replacement_denied=1" in lock_log, lock_log
            wait_run(address, lock_run_id, {"running"})
            current_lock_identity = lock_file.stat()
            assert (
                current_lock_identity.st_dev,
                current_lock_identity.st_ino,
            ) == (lock_identity.st_dev, lock_identity.st_ino), "workload changed the active ownership lock inode"

            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                contender_address = reserved.getsockname()
            contender_config = dict(config)
            contender_config.update(
                {
                    "listen": f"127.0.0.1:{contender_address[1]}",
                    "manager_data_dir": str(root / "contender-manager"),
                    "worker_data_dir": str(root / "contender-worker"),
                    "landlock": {"mode": "disabled"},
                }
            )
            contender_path = root / "contender.json"
            contender_path.write_text(json.dumps(contender_config), encoding="utf-8")
            contender = subprocess.run(
                [str(args.binary.resolve()), "agent", "--config", str(contender_path)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert contender.returncode != 0, "competing daemon acquired ownership while the lock was active"
            assert "startup_failed" in contender.stderr, contender.stderr
            assert "worker cgroup namespace is already owned" in contender.stderr, contender.stderr
            assert wait_run(address, lock_run_id, {"running"})["state"] == "running"
            after_contender_identity = lock_file.stat()
            assert (
                after_contender_identity.st_dev,
                after_contender_identity.st_ino,
            ) == (lock_identity.st_dev, lock_identity.st_ino), "competing daemon replaced the active ownership lock"
            status, _ = request(address, "POST", f"/v1/runs/{lock_run_id}/stop")
            assert status in (200, 202), f"ownership lock workload stop returned HTTP {status}"
            assert wait_run(address, lock_run_id, {"cancelled"})["state"] == "cancelled"
            wait_cgroups_empty(worker_root)

            # Cancellation and deadlines still reap the cgroup while Landlock is active.
            for job_id, extra, expected in (
                ("landlock-cancel", {"terminationGraceMs": 100}, "cancelled"),
                ("landlock-deadline", {"timeoutMs": 200}, "timed_out"),
            ):
                limited = {
                    "id": job_id,
                    "command": [str(fixture), "--sleep"],
                    "cwd": str(root),
                    "filesystemPolicy": {"grants": grant_ids},
                    **extra,
                }
                status, _ = request(address, "POST", "/v1/jobs", limited)
                assert status == 201, f"{job_id} registration returned HTTP {status}"
                status, accepted = request(address, "POST", f"/v1/jobs/{job_id}/runs")
                assert status == 202, f"{job_id} start returned HTTP {status}"
                if expected == "cancelled":
                    wait_run(address, accepted["runId"], {"running"})
                    status, _ = request(address, "POST", f"/v1/runs/{accepted['runId']}/stop")
                    assert status in (200, 202), f"Landlock stop returned HTTP {status}"
                outcome = wait_run(address, accepted["runId"], {expected})
                assert outcome["state"] == expected, f"{job_id} ended in {outcome['state']}"
                wait_cgroups_empty(worker_root)

            # The named grant policy persists with its immutable job definition across restart.
            process.send_signal(signal.SIGINT)
            process.wait(timeout=8)
            process = launch()
            wait_agent(process, address)
            status, persisted = request(address, "GET", "/v1/jobs/landlock-fixture")
            assert status == 200, "Landlock job was not persisted across restart"
            assert persisted["filesystemPolicy"] == {"grants": grant_ids}

            # The required mode cannot be disabled by omitting a policy or selecting an unknown grant.
            for job_id, policy in (("missing-policy", None), ("unknown-grant", {"grants": ["not-approved"]})):
                rejected = {
                    "id": job_id,
                    "command": [str(fixture), str(input_path), str(output_dir), str(secret), str(delegated_root / "cgroup.procs")],
                    "cwd": str(root),
                }
                if policy is not None:
                    rejected["filesystemPolicy"] = policy
                status, _ = request(address, "POST", "/v1/jobs", rejected)
                assert status == 201, f"{job_id} registration returned HTTP {status}"
                status, accepted = request(address, "POST", f"/v1/jobs/{job_id}/runs")
                assert status == 202, f"{job_id} start returned HTTP {status}"
                outcome = wait_run(address, accepted["runId"], {"failed"})
                assert outcome["state"] == "failed", f"{job_id} unexpectedly ran: {outcome}"

            disabled_job = {
                "id": "disable-required-policy",
                "command": [str(fixture), str(input_path), str(output_dir), str(secret), str(delegated_root / "cgroup.procs")],
                "cwd": str(root),
                "filesystemPolicy": {"grants": ["fixture"], "enabled": False},
            }
            status, _ = request(address, "POST", "/v1/jobs", disabled_job)
            assert status == 422, "job was allowed to disable mandatory filesystem enforcement"

            # A path grant must not overlap state, cgroup controls, or a protected ancestor.
            broad_config = dict(config)
            broad_config["manager_data_dir"] = str(root / "broad-manager")
            broad_config["worker_data_dir"] = str(root / "broad-worker")
            broad_config["cgroup_v2"] = {"enabled": False}
            broad_config["landlock"] = {
                "mode": "required",
                "grants": [{"id": "broad", "path": str(root), "read": True}],
            }
            broad_path = root / "broad.json"
            broad_path.write_text(json.dumps(broad_config), encoding="utf-8")
            rejected_agent = subprocess.run(
                [str(args.binary.resolve()), "agent", "--config", str(broad_path)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert rejected_agent.returncode != 0, "broad protected ancestor grant was accepted"
            assert "overlap Conductor state or control paths" in rejected_agent.stderr, rejected_agent.stderr

            nested = output_dir / "nested"
            nested.mkdir()
            broad_config["landlock"]["grants"] = [
                {"id": "outer-read", "path": str(output_dir), "read": True},
                {"id": "nested-write", "path": str(nested), "write": True},
            ]
            nested_path = root / "nested.json"
            nested_path.write_text(json.dumps(broad_config), encoding="utf-8")
            rejected_nested = subprocess.run(
                [str(args.binary.resolve()), "agent", "--config", str(nested_path)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert rejected_nested.returncode != 0, "a nested grant widened an ancestor policy"
            assert "nested Landlock grants cannot widen" in rejected_nested.stderr

            # The configured ownership directory is protected independently of
            # manager/worker state and the delegated cgroup hierarchy.
            for index, (case, grant_path, configured_path) in enumerate(
                (
                    ("direct", lock_directory, None),
                    ("ancestor", lock_runtime, None),
                    ("descendant", lock_child, None),
                    ("grant-alias", lock_alias, None),
                    ("configured-alias", lock_directory, lock_alias),
                    ("configured-parent-traversal", lock_directory, lock_parent_traversal),
                    ("configured-parent-ancestor", lock_runtime, lock_parent_traversal),
                    ("configured-parent-descendant", lock_child, lock_parent_traversal),
                )
            ):
                protected_config = dict(config)
                worker_id = f"landlock-lock-{case}"
                protected_config.update(
                    {
                        "worker_id": worker_id,
                        "manager_data_dir": str(root / f"protected-manager-{index}"),
                        "worker_data_dir": str(root / f"protected-worker-{index}"),
                    }
                )
                protected_config["cgroup_v2"] = dict(config["cgroup_v2"])
                if configured_path is not None:
                    protected_config["cgroup_v2"]["ownershipLockDir"] = str(configured_path)
                protected_config["landlock"] = {
                    "mode": "required",
                    "grants": [{"id": "lock-write", "path": str(grant_path), "write": True}],
                }
                protected_path = root / f"protected-{case}.json"
                protected_path.write_text(json.dumps(protected_config), encoding="utf-8")
                rejected_lock = subprocess.run(
                    [str(args.binary.resolve()), "agent", "--config", str(protected_path)],
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                assert rejected_lock.returncode != 0, f"{case} ownership lock grant was accepted"
                assert "overlap Conductor state or control paths" in rejected_lock.stderr, rejected_lock.stderr
                protected_worker_root = worker_cgroup_root(delegated_root, worker_id)
                reap_owned_groups(protected_worker_root)
                if protected_worker_root.exists():
                    protected_worker_root.rmdir()

            # A cgroups-disabled agent remains compatible with Landlock grants.
            disabled_address = reserve_address()
            disabled_config = dict(config)
            disabled_config.update(
                {
                    "listen": f"{disabled_address[0]}:{disabled_address[1]}",
                    "manager_data_dir": str(root / "disabled-manager"),
                    "worker_data_dir": str(root / "disabled-worker"),
                    "worker_id": "landlock-no-cgroups",
                    "cgroup_v2": {"enabled": False},
                }
            )
            disabled_path = root / "disabled-cgroups.json"
            disabled_path.write_text(json.dumps(disabled_config), encoding="utf-8")
            disabled_agent = subprocess.Popen(
                [str(args.binary.resolve()), "agent", "--config", str(disabled_path)],
                cwd=root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                wait_agent(disabled_agent, disabled_address)
            finally:
                if disabled_agent.poll() is None:
                    disabled_agent.send_signal(signal.SIGINT)
                    disabled_agent.wait(timeout=8)
            print(json.dumps({"status": "passed", "enforcement": "real Landlock ABI >= 3", "evidence": 15}))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            reap_owned_groups(worker_cgroup_root(delegated_root, config["worker_id"]))


if __name__ == "__main__":
    main()

