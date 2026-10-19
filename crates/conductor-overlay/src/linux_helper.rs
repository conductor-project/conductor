use std::io;
use std::net::Ipv4Addr;
use std::process::Stdio;

use conductor_core::{OverlayPeerConfiguration, WorkerId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

const MAX_HELPER_RESPONSE_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct HelperResponse {
    event: String,
    worker_id: Option<WorkerId>,
    interface_name: Option<String>,
    local_address: Option<Ipv4Addr>,
    peer_generation: Option<u64>,
    network_generation: Option<u64>,
    expires_at_ms: Option<i64>,
}

#[derive(Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum HelperRequest<'a> {
    Start {
        worker_id: &'a WorkerId,
        secret_key: [u8; 32],
        configuration: &'a OverlayPeerConfiguration,
    },
    Reconfigure {
        configuration: &'a OverlayPeerConfiguration,
    },
    Shutdown,
}

/// Child process supervisor for the Linux helper carrying only network-admin
/// authority. The Conductor worker and its workload child processes stay
/// unprivileged.
pub struct LinuxOverlayHelperClient {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    interface_name: String,
    worker_id: WorkerId,
    configuration: OverlayPeerConfiguration,
}

impl LinuxOverlayHelperClient {
    pub async fn start(
        helper_path: &std::path::Path,
        worker_id: &WorkerId,
        secret_key: [u8; 32],
        configuration: &OverlayPeerConfiguration,
    ) -> io::Result<Self> {
        if &configuration.worker_id != worker_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay helper configuration has the wrong worker identity",
            ));
        }
        if configuration.peers.len() > crate::MAX_OVERLAY_PEERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer count exceeds the supported limit",
            ));
        }
        let interface_name = crate::interface_name_for_worker(worker_id);
        let mut child = Command::new(helper_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("overlay helper stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("overlay helper stdout unavailable"))?;
        let mut client = Self {
            child,
            input,
            output: BufReader::new(stdout),
            interface_name,
            worker_id: worker_id.clone(),
            configuration: configuration.clone(),
        };
        client
            .write_request(HelperRequest::Start {
                worker_id,
                secret_key,
                configuration,
            })
            .await?;
        let response = client.read_response().await?;
        client.validate_response(&response, "ready", configuration)?;
        Ok(client)
    }

    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    pub fn local_address(&self) -> Ipv4Addr {
        self.configuration.worker_address
    }

    pub async fn reconfigure(
        &mut self,
        configuration: &OverlayPeerConfiguration,
    ) -> io::Result<()> {
        if configuration.worker_id != self.worker_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay helper configuration has the wrong worker identity",
            ));
        }
        if configuration.peers.len() > crate::MAX_OVERLAY_PEERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer count exceeds the supported limit",
            ));
        }
        self.write_request(HelperRequest::Reconfigure { configuration })
            .await?;
        let response = self.read_response().await?;
        self.validate_response(&response, "applied", configuration)?;
        self.configuration = configuration.clone();
        Ok(())
    }

    pub async fn shutdown(mut self) -> io::Result<()> {
        self.write_request(HelperRequest::Shutdown).await?;
        let response = self.read_response().await?;
        self.validate_response(&response, "stopped", &self.configuration)?;
        let status = self.child.wait().await?;
        if !status.success() {
            return Err(io::Error::other("overlay helper exited unsuccessfully"));
        }
        Ok(())
    }

    async fn write_request(&mut self, request: HelperRequest<'_>) -> io::Result<()> {
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|_| io::Error::other("could not encode overlay helper request"))?;
        encoded.push(b'\n');
        self.input.write_all(&encoded).await?;
        self.input.flush().await
    }

    async fn read_response(&mut self) -> io::Result<HelperResponse> {
        read_bounded_json_line(&mut self.output, MAX_HELPER_RESPONSE_BYTES).await
    }

    fn validate_response(
        &self,
        response: &HelperResponse,
        expected_event: &str,
        configuration: &OverlayPeerConfiguration,
    ) -> io::Result<()> {
        if response.event != expected_event
            || response.worker_id.as_ref() != Some(&self.worker_id)
            || response.interface_name.as_deref() != Some(self.interface_name.as_str())
            || response.local_address != Some(configuration.worker_address)
            || response.peer_generation != Some(configuration.peer_generation)
            || response.network_generation != Some(configuration.network_generation)
            || response.expires_at_ms != Some(configuration.expires_at_ms)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "overlay helper returned an unexpected response",
            ));
        }
        Ok(())
    }
}

async fn read_bounded_json_line<R>(reader: &mut R, limit: usize) -> io::Result<HelperResponse>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "overlay helper closed its response stream",
            ));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if line.len().saturating_add(count) > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "overlay helper response exceeds the size limit",
            ));
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            return serde_json::from_slice(&line).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid helper response")
            });
        }
    }
}
