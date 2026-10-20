#!/usr/bin/env python3
"""Bounded HTTP traffic probe for independent overlay hosts."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import http.server
import json
import math
import os
import secrets
import statistics
import sys
import time
from urllib.parse import quote

MAX_PAYLOAD_BYTES = 1_048_576
MAX_TOTAL_PROBE_BYTES = 32 * 1_048_576


def emit(value: dict[str, object]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subcommands = result.add_subparsers(dest="mode", required=True)
    serve = subcommands.add_parser("serve", help="serve one bounded random payload")
    serve.add_argument("--bind", required=True, help="assigned overlay IPv4 address")
    serve.add_argument("--port", type=int, default=18080)
    serve.add_argument("--bytes", type=int, default=65_536, dest="payload_bytes")
    serve.add_argument("--timeout-seconds", type=float, default=120)
    serve.add_argument("--max-requests", type=int, default=1)
    managed = subcommands.add_parser(
        "serve-managed", help="serve a bounded payload on a Conductor-allocated service port"
    )
    managed.add_argument("--bind", required=True, help="worker overlay IPv4 address")
    managed.add_argument(
        "--port-env", default="CONDUCTOR_PORT_HTTP", help="Conductor-assigned port environment key"
    )
    managed.add_argument("--bytes", type=int, default=65_536, dest="payload_bytes")
    managed.add_argument("--timeout-seconds", type=float, default=600)
    managed.add_argument("--max-requests", type=int, default=32)
    request = subcommands.add_parser("request", help="fetch and verify a server payload")
    request.add_argument("--host", required=True, help="server's assigned overlay IPv4")
    request.add_argument("--port", type=int, default=18080)
    request.add_argument("--nonce", required=True)
    request.add_argument("--sha256", required=True)
    request.add_argument("--bytes", type=int, required=True, dest="payload_bytes")
    request.add_argument("--timeout-seconds", type=float, default=30)
    request.add_argument("--iterations", type=int, default=1)
    return result


def run_server(args: argparse.Namespace) -> int:
    if not 1 <= args.payload_bytes <= MAX_PAYLOAD_BYTES:
        raise ValueError(f"--bytes must be between 1 and {MAX_PAYLOAD_BYTES}")
    if not 1 <= args.port <= 65535 or not 1 <= args.timeout_seconds <= 600:
        raise ValueError("port or timeout is outside the supported range")
    if not 1 <= args.max_requests <= 128:
        raise ValueError("--max-requests must be between 1 and 128")
    if args.payload_bytes * args.max_requests > MAX_TOTAL_PROBE_BYTES:
        raise ValueError("payload size times --max-requests exceeds the 32 MiB run limit")
    if args.payload_bytes < 32:
        raise ValueError("--bytes must be at least 32 to include the capture marker")
    marker = secrets.token_hex(16)
    payload = marker.encode("ascii") + secrets.token_bytes(args.payload_bytes - len(marker))
    nonce = secrets.token_hex(16)
    digest = hashlib.sha256(payload).hexdigest()
    expected_path = f"/probe/{nonce}"
    started = time.monotonic()
    request_count = 0

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            nonlocal request_count
            if self.path != expected_path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            request_count += 1
            emit(
                {
                    "event": "request_complete",
                    "remoteAddress": self.client_address[0],
                    "remotePort": self.client_address[1],
                    "bytesSent": len(payload),
                    "sha256": digest,
                    "elapsedMs": round((time.monotonic() - started) * 1000, 3),
                }
            )

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = http.server.HTTPServer((args.bind, args.port), Handler)
    server.timeout = args.timeout_seconds
    emit(
        {
            "event": "ready",
            "bindAddress": args.bind,
            "port": args.port,
            "nonce": nonce,
            "captureMarker": marker,
            "payloadBytes": len(payload),
            "sha256": digest,
            "timeoutSeconds": args.timeout_seconds,
            "maxRequests": args.max_requests,
        }
    )
    try:
        deadline = started + args.timeout_seconds
        while time.monotonic() < deadline and request_count < args.max_requests:
            server.timeout = min(0.5, max(0.0, deadline - time.monotonic()))
            server.handle_request()
    finally:
        server.server_close()
    return 0


def run_request(args: argparse.Namespace) -> int:
    if not 1 <= args.payload_bytes <= MAX_PAYLOAD_BYTES:
        raise ValueError(f"--bytes must be between 1 and {MAX_PAYLOAD_BYTES}")
    if not 1 <= args.port <= 65535 or not 1 <= args.timeout_seconds <= 300:
        raise ValueError("port or timeout is outside the supported range")
    if not 1 <= args.iterations <= 128:
        raise ValueError("--iterations must be between 1 and 128")
    if args.payload_bytes * args.iterations > MAX_TOTAL_PROBE_BYTES:
        raise ValueError("payload size times --iterations exceeds the 32 MiB run limit")
    if len(args.nonce) != 32 or any(c not in "0123456789abcdef" for c in args.nonce):
        raise ValueError("--nonce must be 32 lowercase hexadecimal characters")
    if len(args.sha256) != 64 or any(c not in "0123456789abcdef" for c in args.sha256):
        raise ValueError("--sha256 must be 64 lowercase hexadecimal characters")

    path = f"/probe/{quote(args.nonce, safe='')}"
    samples: list[dict[str, object]] = []
    peer = None
    local = None
    started = time.monotonic()
    for iteration in range(args.iterations):
        connection = http.client.HTTPConnection(args.host, args.port, timeout=args.timeout_seconds)
        sample_started = time.monotonic()
        try:
            connection.connect()
            if iteration == 0:
                peer = list(connection.sock.getpeername()) if connection.sock else None
                local = list(connection.sock.getsockname()) if connection.sock else None
            connection.request("GET", path)
            response = connection.getresponse()
            payload = response.read(MAX_PAYLOAD_BYTES + 1)
            elapsed_ms = max((time.monotonic() - sample_started) * 1000, 0.001)
            digest = hashlib.sha256(payload).hexdigest()
            passed = (
                response.status == 200
                and len(payload) == args.payload_bytes
                and len(payload) <= MAX_PAYLOAD_BYTES
                and digest == args.sha256
            )
            if not passed:
                emit(
                    {
                        "event": "probe_complete",
                        "result": "failed",
                        "failure": "payload_mismatch",
                        "failedIteration": iteration + 1,
                        "httpStatus": response.status,
                        "targetAddress": args.host,
                        "targetPort": args.port,
                        "bytesReceived": len(payload),
                        "sha256": digest,
                        "elapsedMs": round((time.monotonic() - started) * 1000, 3),
                    }
                )
                return 1
            samples.append(
                {
                    "iteration": iteration + 1,
                    "elapsedMs": round(elapsed_ms, 3),
                    "bytesReceived": len(payload),
                    "throughputBytesPerSecond": round(len(payload) * 1000 / elapsed_ms, 3),
                }
            )
        except (OSError, http.client.HTTPException) as error:
            emit(
                {
                    "event": "probe_complete",
                    "result": "failed",
                    "failure": "request_error",
                    "failedIteration": iteration + 1,
                    "errorType": type(error).__name__,
                    "targetAddress": args.host,
                    "targetPort": args.port,
                    "elapsedMs": round((time.monotonic() - started) * 1000, 3),
                }
            )
            return 1
        finally:
            connection.close()

    latencies = sorted(float(sample["elapsedMs"]) for sample in samples)
    throughputs = sorted(float(sample["throughputBytesPerSecond"]) for sample in samples)

    def percentile(values: list[float], quantile: float) -> float:
        return values[max(0, math.ceil(quantile * len(values)) - 1)]

    emit(
        {
            "event": "probe_complete",
            "result": "passed",
            "targetAddress": args.host,
            "targetPort": args.port,
            "localAddress": local,
            "connectedPeer": peer,
            "iterations": len(samples),
            "bytesPerIteration": args.payload_bytes,
            "sha256": args.sha256,
            "totalBytesReceived": sum(int(sample["bytesReceived"]) for sample in samples),
            "elapsedMs": round((time.monotonic() - started) * 1000, 3),
            "latencyMs": {
                "min": round(latencies[0], 3),
                "median": round(statistics.median(latencies), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "max": round(latencies[-1], 3),
            },
            "throughputBytesPerSecond": {
                "min": round(throughputs[0], 3),
                "median": round(statistics.median(throughputs), 3),
                "p95": round(percentile(throughputs, 0.95), 3),
                "max": round(throughputs[-1], 3),
            },
            "percentileMethod": "nearest_rank",
            "samples": samples,
        }
    )
    return 0


def run_managed_server(args: argparse.Namespace) -> int:
    if not 1 <= args.payload_bytes <= MAX_PAYLOAD_BYTES:
        raise ValueError(f"--bytes must be between 1 and {MAX_PAYLOAD_BYTES}")
    if not 1 <= args.timeout_seconds <= 3600:
        raise ValueError("--timeout-seconds must be between 1 and 3600")
    if not 1 <= args.max_requests <= 128:
        raise ValueError("--max-requests must be between 1 and 128")
    if args.payload_bytes * args.max_requests > MAX_TOTAL_PROBE_BYTES:
        raise ValueError("payload size times --max-requests exceeds the 32 MiB run limit")
    if args.payload_bytes < 32:
        raise ValueError("--bytes must be at least 32 to include the capture marker")
    port_name = args.port_env.removeprefix("CONDUCTOR_PORT_")
    if not port_name or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in port_name):
        raise ValueError("--port-env must name a Conductor-assigned service port")
    port_value = os.environ.get(args.port_env)
    if port_value is None or not port_value.isascii() or not port_value.isdecimal():
        raise ValueError("the configured Conductor service port is unavailable")
    port = int(port_value)
    if not 1 <= port <= 65535:
        raise ValueError("the configured Conductor service port is invalid")

    marker = secrets.token_hex(16)
    payload = marker.encode("ascii") + secrets.token_bytes(args.payload_bytes - len(marker))
    nonce = secrets.token_hex(16)
    digest = hashlib.sha256(payload).hexdigest()
    expected_path = f"/probe/{nonce}"
    started = time.monotonic()
    request_count = 0

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            nonlocal request_count
            if self.path != expected_path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            request_count += 1
            emit(
                {
                    "event": "request_complete",
                    "remoteAddress": self.client_address[0],
                    "remotePort": self.client_address[1],
                    "bytesSent": len(payload),
                    "sha256": digest,
                    "elapsedMs": round((time.monotonic() - started) * 1000, 3),
                }
            )

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = http.server.HTTPServer((args.bind, port), Handler)
    deadline = started + args.timeout_seconds
    emit(
        {
            "event": "ready",
            "bindAddress": args.bind,
            "port": port,
            "portEnvironment": args.port_env,
            "nonce": nonce,
            "captureMarker": marker,
            "payloadBytes": len(payload),
            "sha256": digest,
            "timeoutSeconds": args.timeout_seconds,
            "maxRequests": args.max_requests,
        }
    )
    try:
        while time.monotonic() < deadline and request_count < args.max_requests:
            server.timeout = min(0.5, max(0, deadline - time.monotonic()))
            server.handle_request()
    finally:
        server.server_close()
    return 0


def main() -> int:
    args = parser().parse_args()
    try:
        if args.mode == "serve":
            return run_server(args)
        if args.mode == "serve-managed":
            return run_managed_server(args)
        return run_request(args)
    except (OSError, ValueError) as error:
        print(f"overlay traffic probe: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
