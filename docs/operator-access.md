# Operator API access

The local combined deployment uses `api_access.mode: "local"`. This mode accepts no operator credentials and requires the HTTP listener to bind to loopback. It is intended for development and deployments where access is restricted by the host.

For remote operator access, keep Conductor's HTTP listener on loopback and put a trusted TLS-terminating reverse proxy in front of it. Create an operator token file:

```sh
conductor create-operator-token --output /etc/conductor/operator-token
```

The command creates a new file, refuses to overwrite an existing path, writes a cryptographically random 256-bit URL-safe token, and applies mode `0600` on Unix. The token is emitted only into that file. Protect the file and its parent directory as daemon credentials.

Configure the daemon with `api_access.mode` set to `operator_token_file` and point `token_file` at the token file. Relative token paths are resolved against the configuration file's directory. For example:

```json
{
  "role": "all",
  "listen": "127.0.0.1:8080",
  "manager_data_dir": "/var/lib/conductor/manager",
  "worker_data_dir": "/var/lib/conductor/worker",
  "worker_id": "local-worker",
  "worker_concurrency": 4,
  "api_access": {
    "mode": "operator_token_file",
    "token_file": "/etc/conductor/operator-token"
  }
}
```

The HTTP API accepts loopback listeners only because it does not terminate TLS itself. A reverse proxy can accept remote HTTPS and forward requests over loopback. Configure the proxy to preserve `Authorization` and restrict access to its HTTPS endpoint. Every `/v1` request reaching Conductor must include `Authorization: Bearer <token>`. This protects reads as well as writes, including worker status and run logs. Invalid or missing credentials receive `401` and a `WWW-Authenticate` challenge. `/healthz` and `/readyz` remain unauthenticated and expose only basic health and readiness.

Operator bearer credentials are independent from worker identity. Remote worker poll and report routes continue to require mutual TLS; the manager derives the worker identity from the verified certificate and rejects a request whose claimed worker ID differs. Worker credentials cannot authorize operator API calls, and operator tokens cannot authorize worker transport calls.

To rotate an operator token, create a replacement token file at a new protected path, update `token_file`, and restart the daemon. The daemon reads one token during startup, so replacing the configured file and restarting invalidates the previous token. Distribute the replacement through the existing protected operator credential channel, then securely remove the old file after clients have switched. Keep token contents out of logs, command-line arguments, and configuration files.
