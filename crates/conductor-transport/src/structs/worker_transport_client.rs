use super::*;

fn transport_error_for_status(status: reqwest::StatusCode) -> TransportError {
    if status == reqwest::StatusCode::UPGRADE_REQUIRED {
        TransportError::IncompatibleProtocol
    } else {
        TransportError::Http
    }
}

fn is_permanent_poll_error(error: TransportError) -> bool {
    matches!(error, TransportError::IncompatibleProtocol)
}

/// Client certificate credentials and HTTPS endpoint for one worker.
pub struct WorkerTransportClient {
    pub(super) endpoint: Url,
    pub(super) client: reqwest::Client,
}

impl WorkerTransportClient {
    pub fn from_pem_identity(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
    ) -> Result<Self, TransportError> {
        Self::build(endpoint, certificate_and_private_key_pem, None)
    }

    /// Construct a worker client using the provided bootstrap CA trust anchor.
    pub fn from_pem_identity_with_root(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
        certificate_authority_pem: &[u8],
    ) -> Result<Self, TransportError> {
        let root = reqwest::Certificate::from_pem(certificate_authority_pem)
            .map_err(|_| TransportError::InvalidIdentity)?;
        Self::build(endpoint, certificate_and_private_key_pem, Some(root))
    }

    pub(super) fn build(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
        root: Option<reqwest::Certificate>,
    ) -> Result<Self, TransportError> {
        let endpoint = Url::parse(endpoint).map_err(|_| TransportError::InvalidEndpoint)?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(TransportError::InvalidEndpoint);
        }
        let identity = reqwest::Identity::from_pem(certificate_and_private_key_pem)
            .map_err(|_| TransportError::InvalidIdentity)?;
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CLIENT_REQUEST_TIMEOUT)
            .identity(identity);
        if let Some(root) = root {
            builder = builder.tls_certs_only([root]);
        }
        let client = builder
            .build()
            .map_err(|_| TransportError::InvalidIdentity)?;
        Ok(Self { endpoint, client })
    }

    pub async fn poll(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        status: &WorkerStatus,
        inventory_page: &WorkerInventoryPage,
        protocol_offer: &ProtocolOffer,
    ) -> Result<WorkerPollResponse, TransportError> {
        self.post_json(
            "v1/worker/poll",
            &WorkerPollRequest {
                worker_id: worker_id.clone(),
                worker_incarnation,
                status: status.clone(),
                inventory_page: inventory_page.clone(),
                protocol_offer: protocol_offer.clone(),
            },
        )
        .await
    }

    pub async fn register_overlay_identity(
        &self,
        domain_id: &conductor_core::NetworkDomainId,
        endpoint_id: &conductor_core::OverlayEndpointId,
    ) -> Result<conductor_core::OverlayPeerAssignment, TransportError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct EndpointRequest<'a> {
            endpoint_id: &'a str,
        }
        self.json_request(
            reqwest::Method::PUT,
            &format!("v1/worker/network-domains/{}/identity", domain_id.as_str()),
            Some(&EndpointRequest {
                endpoint_id: endpoint_id.as_str(),
            }),
        )
        .await
    }

    pub async fn overlay_configuration(
        &self,
        domain_id: &conductor_core::NetworkDomainId,
    ) -> Result<conductor_core::OverlayPeerConfiguration, TransportError> {
        self.json_request::<(), _>(
            reqwest::Method::GET,
            &format!("v1/worker/network-domains/{}", domain_id.as_str()),
            None,
        )
        .await
    }

    pub async fn advertise_overlay_candidates(
        &self,
        domain_id: &conductor_core::NetworkDomainId,
        peer_generation: u64,
        candidates: &[std::net::SocketAddr],
    ) -> Result<conductor_core::OverlayDirectCandidateAcknowledgement, TransportError> {
        self.post_json(
            &format!(
                "v1/worker/network-domains/{}/candidates",
                domain_id.as_str()
            ),
            &conductor_core::OverlayDirectCandidateAdvertisement {
                peer_generation,
                candidates: candidates.to_vec(),
            },
        )
        .await
    }

    pub async fn report_overlay_observation(
        &self,
        domain_id: &conductor_core::NetworkDomainId,
        observation: &conductor_core::OverlayPeerObservation,
    ) -> Result<(), TransportError> {
        let url = self
            .endpoint
            .join(&format!(
                "v1/worker/network-domains/{}/observation",
                domain_id.as_str()
            ))
            .map_err(|_| TransportError::InvalidEndpoint)?;
        let mut body = BoundedJsonBody(Vec::new());
        serde_json::to_writer(&mut body, observation)
            .map_err(|_| TransportError::InvalidCommand)?;
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.0)
            .send()
            .await
            .map_err(|_| TransportError::Http)?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(transport_error_for_status(response.status()));
        }
        Ok(())
    }

    pub async fn report_overlay_application_probe(
        &self,
        domain_id: &conductor_core::NetworkDomainId,
        report: &conductor_core::OverlayApplicationProbeReport,
    ) -> Result<(), TransportError> {
        let url = self
            .endpoint
            .join(&format!(
                "v1/worker/network-domains/{}/probe",
                domain_id.as_str()
            ))
            .map_err(|_| TransportError::InvalidEndpoint)?;
        let mut body = BoundedJsonBody(Vec::new());
        serde_json::to_writer(&mut body, report).map_err(|_| TransportError::InvalidCommand)?;
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.0)
            .send()
            .await
            .map_err(|_| TransportError::Http)?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(transport_error_for_status(response.status()));
        }
        Ok(())
    }

    pub(super) async fn send_report(
        &self,
        report: &WorkerReport,
    ) -> Result<ReportAcknowledgement, TransportError> {
        let acknowledgement: ReportAcknowledgement = self
            .post_json(
                "v1/worker/report",
                &WorkerReportRequest {
                    worker_id: report.worker_id.clone(),
                    worker_incarnation: report.worker_incarnation,
                    report: report.clone(),
                },
            )
            .await?;
        if acknowledgement.report_sequence != report.report_sequence {
            return Err(TransportError::MalformedResponse);
        }
        Ok(acknowledgement)
    }

    pub(super) async fn post_json<RequestBody, ResponseBody>(
        &self,
        path: &str,
        request: &RequestBody,
    ) -> Result<ResponseBody, TransportError>
    where
        RequestBody: Serialize,
        ResponseBody: for<'de> Deserialize<'de>,
    {
        self.json_request(reqwest::Method::POST, path, Some(request))
            .await
    }

    async fn json_request<RequestBody, ResponseBody>(
        &self,
        method: reqwest::Method,
        path: &str,
        request: Option<&RequestBody>,
    ) -> Result<ResponseBody, TransportError>
    where
        RequestBody: Serialize,
        ResponseBody: for<'de> Deserialize<'de>,
    {
        let url = self
            .endpoint
            .join(path)
            .map_err(|_| TransportError::InvalidEndpoint)?;
        let mut builder = self.client.request(method, url);
        if let Some(request) = request {
            let mut body = BoundedJsonBody(Vec::new());
            serde_json::to_writer(&mut body, request)
                .map_err(|_| TransportError::InvalidCommand)?;
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.0);
        }
        let mut response = builder.send().await.map_err(|_| TransportError::Http)?;
        if !response.status().is_success() {
            return Err(transport_error_for_status(response.status()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_TRANSPORT_BODY_BYTES as u64)
        {
            return Err(TransportError::MalformedResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| TransportError::Http)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_TRANSPORT_BODY_BYTES {
                return Err(TransportError::MalformedResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| TransportError::MalformedResponse)
    }

    /// Polls, executes, and reports worker commands until shutdown or until a
    /// permanent transport/configuration error occurs. Temporary network
    /// errors use bounded exponential reconnect backoff.
    pub async fn run(
        self: Arc<Self>,
        worker: Arc<WorkerService>,
        protocol_offer: ProtocolOffer,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), TransportError> {
        let descriptor = worker.descriptor();
        let worker_id = descriptor.worker_id().clone();
        let incarnation = descriptor.incarnation();
        let recovery_worker = Arc::clone(&worker);
        tokio::task::spawn_blocking(move || recover_accepted_commands(&recovery_worker))
            .await
            .map_err(|_| TransportError::Unavailable)??;
        let mut failures = 0_u32;
        let mut inventory_snapshot = None;
        let mut inventory_after_sequence = 0;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let pending = worker
                .pending_reports()
                .map_err(|_| TransportError::Unavailable)?;
            if let Some(stored) = pending.into_iter().next() {
                let report = report_from_stored(stored)?;
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        result = self.send_report(&report) => match result {
                            Ok(_) => {
                                worker.acknowledge_report(
                                    &report.command_id,
                                    report.worker_incarnation,
                                    report.report_sequence,
                                ).map_err(|_| TransportError::Unavailable)?;
                                failures = 0;
                                break;
                            }
                            Err(_) => {
                                failures = failures.saturating_add(1);
                                tokio::time::sleep(backoff(failures)).await;
                            }
                        }
                    }
                }
                continue;
            }
            let polled = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = async {
                    let status = worker.status().map_err(|_| TransportError::Unavailable)?;
                    let inventory_worker = Arc::clone(&worker);
                    let snapshot = inventory_snapshot;
                    let after = inventory_after_sequence;
                    let inventory_page = tokio::task::spawn_blocking(move || {
                        inventory_worker.inventory_page(
                            snapshot,
                            after,
                            conductor_core::MAX_WORKER_INVENTORY_PAGE_SIZE,
                        )
                    })
                    .await
                    .map_err(|_| TransportError::Unavailable)?
                    .map_err(|_| TransportError::Unavailable)?;
                    let response = self
                        .poll(&worker_id, incarnation, &status, &inventory_page, &protocol_offer)
                        .await?;
                    Ok::<_, TransportError>((response, inventory_page))
                } => result,
            };
            match polled {
                Ok((response, inventory_page)) => {
                    failures = 0;
                    let acknowledgement = response.inventory_acknowledgement;
                    if acknowledgement.restart_scan {
                        inventory_snapshot = None;
                        inventory_after_sequence = 0;
                    } else if acknowledgement.snapshot_generation
                        != inventory_page.snapshot_generation
                        || acknowledgement.next_after_sequence != inventory_page.next_after_sequence
                    {
                        return Err(TransportError::MalformedResponse);
                    } else if let Some(next_after_sequence) = inventory_page.next_after_sequence {
                        inventory_snapshot = Some(inventory_page.snapshot_generation);
                        inventory_after_sequence = next_after_sequence;
                    } else {
                        inventory_snapshot = None;
                        inventory_after_sequence = 0;
                    }
                    let Some(command) = response.command else {
                        continue;
                    };
                    if command.target_worker_id != worker_id
                        || command.target_worker_incarnation != incarnation
                    {
                        return Err(TransportError::WorkerConflict);
                    }
                    let worker_for_command = Arc::clone(&worker);
                    let report = tokio::task::spawn_blocking(move || {
                        let accepted = worker_for_command.accept_command(&command)?;
                        if let Some(stored) = accepted {
                            return Ok::<_, WorkerError>(stored);
                        }
                        let result =
                            execute_command(worker_for_command.as_ref(), command.command.clone());
                        let result_json = serde_json::to_string(&result)
                            .map_err(|_| WorkerError::StorageUnavailable)?;
                        worker_for_command.complete_command(&command, &result_json)
                    })
                    .await
                    .map_err(|_| TransportError::Unavailable)?
                    .map_err(|_| TransportError::Unavailable)?;
                    let report = report_from_stored(report)?;
                    loop {
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                            result = self.send_report(&report) => match result {
                                Ok(_) => {
                                    worker.acknowledge_report(
                                        &report.command_id,
                                        report.worker_incarnation,
                                        report.report_sequence,
                                    ).map_err(|_| TransportError::Unavailable)?;
                                    break;
                                }
                                Err(_) => {
                                    failures = failures.saturating_add(1);
                                    tokio::time::sleep(backoff(failures)).await;
                                }
                            }
                        }
                    }
                }
                Err(error) if is_permanent_poll_error(error) => return Err(error),
                Err(_) => {
                    failures = failures.saturating_add(1);
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(backoff(failures)) => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_required_response_is_protocol_incompatibility_not_network_failure() {
        assert_eq!(
            transport_error_for_status(reqwest::StatusCode::UPGRADE_REQUIRED),
            TransportError::IncompatibleProtocol
        );
        assert_eq!(
            transport_error_for_status(reqwest::StatusCode::SERVICE_UNAVAILABLE),
            TransportError::Http
        );
        assert_eq!(
            TransportError::IncompatibleProtocol.to_string(),
            "worker and manager protocol versions or required capabilities are incompatible; install matching Conductor versions"
        );
        assert!(is_permanent_poll_error(
            TransportError::IncompatibleProtocol
        ));
        assert!(!is_permanent_poll_error(TransportError::Http));
    }
}
