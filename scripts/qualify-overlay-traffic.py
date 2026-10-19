#!/usr/bin/env python3
"""Bounded one-request HTTP smoke probe for independent overlay hosts."""

from __future__ import annotations

import argparse
import hashlib
import http.server
import http.client
import json
import secrets
import sys
import time
from urllib.parse import quote

MAX_PAYLOAD_BYTES = 1_048_576


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
    request = subcommands.add_parser("request", help="fetch and verify a server payload")
    request.add_argument("--host", required=True, help="server's assigned overlay IPv4")
    request.add_argument("--port", type=int, default=18080)
    request.add_argument("--nonce", required=True)
    request.add_argument("--sha256", required=True)
    request.add_argument("--bytes", type=int, required=True, dest="payload_bytes")
    request.add_argument("--timeout-seconds", type=float, default=30)
    return result


def run_server(args: argparse.Namespace) -> int:
    if not 1 <= args.payload_bytes <= MAX_PAYLOAD_BYTES:
        raise ValueError(f"--bytes must be between 1 and {MAX_PAYLOAD_BYTES}")
    if not 1 <= args.port <= 65535 or not 1 <= args.timeout_seconds <= 600:
        raise ValueError("port or timeout is outside the supported range")
    if args.payload_bytes < 32:
        raise ValueError("--bytes must be at least 32 to include the capture marker")
    marker = secrets.token_hex(16)
    payload = marker.encode("ascii") + secrets.token_bytes(args.payload_bytes - len(marker))
    nonce = secrets.token_hex(16)
    digest = hashlib.sha256(payload).hexdigest()
    expected_path = f"/probe/{nonce}"
    started = time.monotonic()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != expected_path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
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
        }
    )
    try:
        server.handle_request()
    finally:
        server.server_close()
    return 0


def run_request(args: argparse.Namespace) -> int:
    if not 1 <= args.payload_bytes <= MAX_PAYLOAD_BYTES:
        raise ValueError(f"--bytes must be between 1 and {MAX_PAYLOAD_BYTES}")
    if not 1 <= args.port <= 65535 or not 1 <= args.timeout_seconds <= 300:
        raise ValueError("port or timeout is outside the supported range")
    if len(args.nonce) != 32 or any(c not in "0123456789abcdef" for c in args.nonce):
        raise ValueError("--nonce must be 32 lowercase hexadecimal characters")
    if len(args.sha256) != 64 or any(c not in "0123456789abcdef" for c in args.sha256):
        raise ValueError("--sha256 must be 64 lowercase hexadecimal characters")

    path = f"/probe/{quote(args.nonce, safe='')}"
    connection = http.client.HTTPConnection(args.host, args.port, timeout=args.timeout_seconds)
    started = time.monotonic()
    try:
        connection.connect()
        peer = list(connection.sock.getpeername()) if connection.sock else None
        local = list(connection.sock.getsockname()) if connection.sock else None
        connection.request("GET", path)
        response = connection.getresponse()
        payload = response.read(MAX_PAYLOAD_BYTES + 1)
        elapsed_ms = round((time.monotonic() - started) * 1000, 3)
        digest = hashlib.sha256(payload).hexdigest()
        passed = (
            response.status == 200
            and len(payload) == args.payload_bytes
            and len(payload) <= MAX_PAYLOAD_BYTES
            and digest == args.sha256
        )
        emit(
            {
                "event": "probe_complete",
                "result": "passed" if passed else "failed",
                "httpStatus": response.status,
                "targetAddress": args.host,
                "targetPort": args.port,
                "localAddress": local,
                "connectedPeer": peer,
                "bytesReceived": len(payload),
                "sha256": digest,
                "elapsedMs": elapsed_ms,
            }
        )
        return 0 if passed else 1
    except (OSError, http.client.HTTPException) as error:
        emit(
            {
                "event": "probe_complete",
                "result": "failed",
                "targetAddress": args.host,
                "targetPort": args.port,
                "errorType": type(error).__name__,
                "elapsedMs": round((time.monotonic() - started) * 1000, 3),
            }
        )
        return 1
    finally:
        connection.close()


def main() -> int:
    args = parser().parse_args()
    try:
        if args.mode == "serve":
            return run_server(args)
        return run_request(args)
    except (OSError, ValueError) as error:
        print(f"overlay traffic probe: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
