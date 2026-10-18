//! One-time worker enrollment and certificate identity lifecycle.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use axum::Json;
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use axum_server_mtls::{MtlsAcceptor, PeerCertificates};
use base64::Engine;
use conductor_core::WorkerId;
use conductor_manager::{SqliteJobRepository, WorkerIdentityError};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use zeroize::Zeroizing;

const CREDENTIAL_BYTES: usize = 32;
const DEFAULT_CERTIFICATE_LIFETIME: Duration = Duration::from_secs(90 * 24 * 60 * 60);
const MAX_ENROLLMENT_CREDENTIAL_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_CSR_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityError {
    InvalidCredential,
    InvalidCsr,
    InvalidCertificate,
    InvalidAuthority,
    StorageUnavailable,
    Io,
    Clock,
    Random,
}

impl From<WorkerIdentityError> for IdentityError {
    fn from(error: WorkerIdentityError) -> Self {
        match error {
            WorkerIdentityError::CredentialInvalid | WorkerIdentityError::IdentityExists => {
                Self::InvalidCredential
            }
            WorkerIdentityError::IdentityUnavailable => Self::InvalidCertificate,
            WorkerIdentityError::StorageUnavailable => Self::StorageUnavailable,
        }
    }
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidCredential => "enrollment credential is invalid, expired, or already used",
            Self::InvalidCsr => "certificate signing request is invalid",
            Self::InvalidCertificate => "worker certificate is invalid or revoked",
            Self::InvalidAuthority => "certificate authority is invalid",
            Self::StorageUnavailable => "worker identity storage is unavailable",
            Self::Io => "worker certificate authority files could not be accessed securely",
            Self::Clock => "system time is unavailable",
            Self::Random => "secure random credential generation failed",
        })
    }
}

impl std::error::Error for IdentityError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IssuedWorkerIdentity {
    pub worker_id: WorkerId,
    pub certificate_pem: String,
    pub certificate_authority_pem: String,
    pub expires_at_ms: i64,
}

/// Locally generated worker key and CSR. The private key remains with the worker.
pub struct WorkerKeyMaterial {
    private_key_pem: Zeroizing<String>,
    csr_pem: String,
}

/// Server certificate and protected in-memory private key issued by the manager CA.
pub struct IssuedServerIdentity {
    pub certificate_pem: String,
    private_key_pem: Zeroizing<String>,
}

impl IssuedServerIdentity {
    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    pub fn write_private_key_once(&self, path: impl AsRef<Path>) -> Result<(), IdentityError> {
        write_new_file(path.as_ref(), self.private_key_pem.as_bytes(), true)
    }
}

impl WorkerKeyMaterial {
    pub fn generate() -> Result<Self, IdentityError> {
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|_| IdentityError::InvalidCertificate)?;
        let csr = CertificateParams::default()
            .serialize_request(&key_pair)
            .map_err(|_| IdentityError::InvalidCsr)?;
        Ok(Self {
            private_key_pem: Zeroizing::new(key_pair.serialize_pem()),
            csr_pem: csr.pem().map_err(|_| IdentityError::InvalidCsr)?,
        })
    }

    pub fn certificate_signing_request_pem(&self) -> &str {
        &self.csr_pem
    }

    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    /// Store a newly generated worker key in a new file with owner-only Unix permissions.
    pub fn write_private_key_once(&self, path: impl AsRef<Path>) -> Result<(), IdentityError> {
        write_new_file(path.as_ref(), self.private_key_pem.as_bytes(), true)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnrollmentRequest {
    credential: String,
    csr_pem: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenewalRequest {
    csr_pem: String,
}

/// Holds the manager's CA signing key. The key is created once with mode 0600 on Unix,
/// stored outside the repository, and never sent to workers.
pub struct CertificateAuthority {
    certificate_pem: String,
    key_pem: Zeroizing<String>,
}

impl CertificateAuthority {
    pub fn open_or_create(directory: impl AsRef<Path>) -> Result<Self, IdentityError> {
        let directory = prepare_identity_directory(directory.as_ref())?;
        let certificate_path = directory.join("worker-ca.pem");
        let key_path = directory.join("worker-ca-key.pem");
        match (
            read_regular_file(&certificate_path, false),
            read_regular_file(&key_path, true),
        ) {
            (Ok(certificate_pem), Ok(key_pem)) => {
                let key_pair =
                    KeyPair::from_pem(&key_pem).map_err(|_| IdentityError::InvalidAuthority)?;
                Issuer::from_ca_cert_pem(&certificate_pem, key_pair)
                    .map_err(|_| IdentityError::InvalidAuthority)?;
                verify_ca_key(&certificate_pem, &key_pem)?;
                Ok(Self {
                    certificate_pem,
                    key_pem: Zeroizing::new(key_pem),
                })
            }
            (Err(IdentityError::Io), Err(IdentityError::Io)) => {
                Self::create_files(&certificate_path, &key_path)
            }
            _ => Err(IdentityError::InvalidAuthority),
        }
    }

    fn create_files(certificate_path: &Path, key_path: &Path) -> Result<Self, IdentityError> {
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|_| IdentityError::InvalidAuthority)?;
        let mut parameters = CertificateParams::default();
        parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        parameters.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let mut name = DistinguishedName::new();
        name.push(rcgen::DnType::CommonName, "Conductor Worker CA");
        parameters.distinguished_name = name;
        let certificate = parameters
            .self_signed(&key_pair)
            .map_err(|_| IdentityError::InvalidAuthority)?;
        let certificate_pem = certificate.pem();
        let key_pem = Zeroizing::new(key_pair.serialize_pem());

        write_new_file(certificate_path, certificate_pem.as_bytes(), false)?;
        write_new_file(key_path, key_pem.as_bytes(), true)?;
        Ok(Self {
            certificate_pem,
            key_pem,
        })
    }

    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    pub fn issue_server_identity(
        &self,
        dns_name: &str,
    ) -> Result<IssuedServerIdentity, IdentityError> {
        if dns_name.is_empty()
            || dns_name.len() > 253
            || dns_name.bytes().any(|b| b.is_ascii_control())
        {
            return Err(IdentityError::InvalidCertificate);
        }
        let issuer = self.issuer()?;
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|_| IdentityError::InvalidAuthority)?;
        let mut parameters = CertificateParams::new(vec![dns_name.to_owned()])
            .map_err(|_| IdentityError::InvalidCertificate)?;
        parameters.not_before = OffsetDateTime::now_utc() - time::Duration::minutes(5);
        parameters.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = parameters
            .signed_by(&key_pair, &issuer)
            .map_err(|_| IdentityError::InvalidCertificate)?;
        Ok(IssuedServerIdentity {
            certificate_pem: certificate.pem(),
            private_key_pem: Zeroizing::new(key_pair.serialize_pem()),
        })
    }

    fn issuer(&self) -> Result<Issuer<'static, KeyPair>, IdentityError> {
        let key_pair = KeyPair::from_pem(self.key_pem.as_str())
            .map_err(|_| IdentityError::InvalidAuthority)?;
        Issuer::from_ca_cert_pem(&self.certificate_pem, key_pair)
            .map_err(|_| IdentityError::InvalidAuthority)
    }

    pub fn server_config(
        &self,
        certificate_pem: &str,
        private_key_pem: &str,
    ) -> Result<Arc<ServerConfig>, IdentityError> {
        let certificate = parse_pem_der(certificate_pem, "CERTIFICATE")?;
        let key_pair =
            KeyPair::from_pem(private_key_pem).map_err(|_| IdentityError::InvalidCertificate)?;
        let ca = parse_pem_der(&self.certificate_pem, "CERTIFICATE")?;
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca))
            .map_err(|_| IdentityError::InvalidAuthority)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .map_err(|_| IdentityError::InvalidAuthority)?;
        let mut config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![CertificateDer::from(certificate)],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der())),
            )
            .map_err(|_| IdentityError::InvalidCertificate)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    /// Build an Axum-compatible TLS acceptor that attaches verified peer certificates.
    pub fn mtls_acceptor(
        &self,
        certificate_pem: &str,
        private_key_pem: &str,
    ) -> Result<MtlsAcceptor, IdentityError> {
        let config =
            RustlsConfig::from_config(self.server_config(certificate_pem, private_key_pem)?);
        Ok(MtlsAcceptor::new(RustlsAcceptor::new(config)))
    }
}

/// Enrollment facade backed by the durable manager repository.
pub struct WorkerEnrollmentService {
    repository: Arc<SqliteJobRepository>,
    authority: Arc<CertificateAuthority>,
    certificate_lifetime: Duration,
}

impl WorkerEnrollmentService {
    pub fn new(repository: Arc<SqliteJobRepository>, authority: Arc<CertificateAuthority>) -> Self {
        Self {
            repository,
            authority,
            certificate_lifetime: DEFAULT_CERTIFICATE_LIFETIME,
        }
    }

    pub fn with_certificate_lifetime(mut self, lifetime: Duration) -> Self {
        self.certificate_lifetime = lifetime.max(Duration::from_secs(60));
        self
    }

    /// Create a random, single-use credential and return its raw value exactly once.
    pub fn create_credential(
        &self,
        worker_id: &WorkerId,
        expires_at_ms: i64,
    ) -> Result<String, IdentityError> {
        let now = now_ms()?;
        let latest_expiry = now
            .checked_add(MAX_ENROLLMENT_CREDENTIAL_LIFETIME.as_millis() as i64)
            .ok_or(IdentityError::Clock)?;
        if expires_at_ms <= now || expires_at_ms > latest_expiry {
            return Err(IdentityError::InvalidCredential);
        }
        let mut credential = Zeroizing::new([0_u8; CREDENTIAL_BYTES]);
        getrandom::fill(&mut *credential).map_err(|_| IdentityError::Random)?;
        let digest = Sha256::digest(&credential[..]);
        let mut token_hash = [0_u8; 32];
        token_hash.copy_from_slice(&digest);
        self.repository
            .create_enrollment_credential(&token_hash, worker_id, expires_at_ms)?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&credential[..]))
    }

    pub fn create_credential_file(
        &self,
        worker_id: &WorkerId,
        expires_at_ms: i64,
        output_path: impl AsRef<Path>,
    ) -> Result<(), IdentityError> {
        let credential = Zeroizing::new(self.create_credential(worker_id, expires_at_ms)?);
        write_new_file(output_path.as_ref(), credential.as_bytes(), true)
    }

    pub fn enroll(
        &self,
        credential: &str,
        csr_pem: &str,
    ) -> Result<IssuedWorkerIdentity, IdentityError> {
        if credential.len() > 128 || csr_pem.len() > MAX_CSR_BYTES {
            return Err(IdentityError::InvalidCredential);
        }
        let credential_bytes = Zeroizing::new(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(credential.as_bytes())
                .map_err(|_| IdentityError::InvalidCredential)?,
        );
        if credential_bytes.len() != CREDENTIAL_BYTES {
            return Err(IdentityError::InvalidCredential);
        }
        let now_ms = now_ms()?;
        let token_hash = hash(&credential_bytes);
        let worker_id = self
            .repository
            .worker_for_enrollment_credential(&token_hash, now_ms)?
            .ok_or(IdentityError::InvalidCredential)?;
        self.issue_certificate(
            &worker_id,
            csr_pem,
            |fingerprint, serial, not_before_ms, expires_at_ms| {
                self.repository.enroll_worker_identity(
                    &token_hash,
                    fingerprint,
                    serial,
                    not_before_ms,
                    expires_at_ms,
                    now_ms,
                )
            },
        )
    }

    pub fn renew(
        &self,
        current_certificate_der: &[u8],
        csr_pem: &str,
    ) -> Result<IssuedWorkerIdentity, IdentityError> {
        if csr_pem.len() > MAX_CSR_BYTES {
            return Err(IdentityError::InvalidCsr);
        }
        let current_fingerprint = hash(current_certificate_der);
        let now_ms = now_ms()?;
        let worker_id = self
            .repository
            .worker_for_active_certificate(&current_fingerprint, now_ms)?
            .ok_or(IdentityError::InvalidCertificate)?;
        self.issue_certificate(
            &worker_id,
            csr_pem,
            |fingerprint, serial, not_before_ms, expires_at_ms| {
                self.repository.renew_worker_identity(
                    &current_fingerprint,
                    fingerprint,
                    serial,
                    not_before_ms,
                    expires_at_ms,
                    now_ms,
                )
            },
        )
    }

    pub fn revoke(&self, worker_id: &WorkerId) -> Result<bool, IdentityError> {
        self.repository
            .revoke_worker_identity(worker_id, now_ms()?)
            .map_err(Into::into)
    }

    pub fn authenticate_certificate(
        &self,
        certificate_der: &[u8],
    ) -> Result<Option<WorkerId>, IdentityError> {
        self.repository
            .authenticate_worker_certificate(&hash(certificate_der), now_ms()?)
            .map_err(Into::into)
    }

    pub fn authenticator(&self) -> Arc<dyn super::WorkerAuthenticator> {
        Arc::new(EnrollmentAuthenticator {
            service: self.clone_handle(),
        })
    }

    /// Routes for worker enrollment and mTLS-authenticated certificate renewal.
    pub fn routes(self: &Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/v1/worker/enroll", post(enroll_handler))
            .route("/v1/worker/renew", post(renew_handler))
            .layer(DefaultBodyLimit::max(super::MAX_TRANSPORT_BODY_BYTES))
            .with_state(Arc::clone(self))
    }

    /// Combine authenticated worker transport and enrollment routes.
    pub fn router_with_hub(self: &Arc<Self>, hub: &super::WorkerTransportHub) -> axum::Router {
        hub.router(self.authenticator()).merge(self.routes())
    }

    fn clone_handle(&self) -> Arc<WorkerEnrollmentHandle> {
        Arc::new(WorkerEnrollmentHandle {
            repository: Arc::clone(&self.repository),
        })
    }

    fn issue_certificate<F>(
        &self,
        worker_id: &WorkerId,
        csr_pem: &str,
        persist: F,
    ) -> Result<IssuedWorkerIdentity, IdentityError>
    where
        F: FnOnce(&[u8; 32], &str, i64, i64) -> Result<WorkerId, WorkerIdentityError>,
    {
        if csr_pem.len() > MAX_CSR_BYTES {
            return Err(IdentityError::InvalidCsr);
        }
        let csr = rcgen::CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|_| IdentityError::InvalidCsr)?;
        let now = OffsetDateTime::now_utc();
        let end = now
            + time::Duration::try_from(self.certificate_lifetime)
                .map_err(|_| IdentityError::Clock)?;
        let mut parameters = CertificateParams::default();
        parameters.not_before = now - time::Duration::minutes(5);
        parameters.not_after = end;
        parameters.distinguished_name = DistinguishedName::new();
        parameters
            .distinguished_name
            .push(rcgen::DnType::CommonName, worker_id.as_str());
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let mut serial_bytes = [0_u8; 16];
        getrandom::fill(&mut serial_bytes).map_err(|_| IdentityError::Random)?;
        serial_bytes[0] &= 0x7f;
        if serial_bytes.iter().all(|byte| *byte == 0) {
            serial_bytes[15] = 1;
        }
        parameters.serial_number = Some(rcgen::SerialNumber::from_slice(&serial_bytes));
        let certificate = parameters
            .signed_by(&csr.public_key, &self.authority.issuer()?)
            .map_err(|_| IdentityError::InvalidCsr)?;
        let der = certificate.der().as_ref();
        let fingerprint = hash(der);
        let serial = hex(&serial_bytes);
        let not_before_ms = millis(parameters.not_before)?;
        let expires_at_ms = millis(parameters.not_after)?;
        let persisted_worker = persist(&fingerprint, &serial, not_before_ms, expires_at_ms)?;
        if persisted_worker != *worker_id {
            return Err(IdentityError::StorageUnavailable);
        }
        Ok(IssuedWorkerIdentity {
            worker_id: worker_id.clone(),
            certificate_pem: certificate.pem(),
            certificate_authority_pem: self.authority.certificate_pem().to_owned(),
            expires_at_ms,
        })
    }
}

async fn enroll_handler(
    State(service): State<Arc<WorkerEnrollmentService>>,
    certificates: Option<Extension<PeerCertificates>>,
    Json(request): Json<EnrollmentRequest>,
) -> Response {
    if certificates.is_none() {
        return (
            StatusCode::UPGRADE_REQUIRED,
            "worker enrollment requires TLS",
        )
            .into_response();
    }
    let credential = Zeroizing::new(request.credential);
    match service.enroll(&credential, &request.csr_pem) {
        Ok(identity) => (StatusCode::CREATED, Json(identity)).into_response(),
        Err(IdentityError::StorageUnavailable) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "worker identity storage unavailable",
        )
            .into_response(),
        Err(_) => (StatusCode::UNAUTHORIZED, "worker enrollment rejected").into_response(),
    }
}

async fn renew_handler(
    State(service): State<Arc<WorkerEnrollmentService>>,
    certificates: Option<Extension<PeerCertificates>>,
    Json(request): Json<RenewalRequest>,
) -> Response {
    let Some(certificate) =
        certificates.and_then(|Extension(certificates)| certificates.leaf().cloned())
    else {
        return (StatusCode::UNAUTHORIZED, "worker certificate required").into_response();
    };
    match service.renew(certificate.as_ref(), &request.csr_pem) {
        Ok(identity) => (StatusCode::OK, Json(identity)).into_response(),
        Err(IdentityError::StorageUnavailable) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "worker identity storage unavailable",
        )
            .into_response(),
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            "worker certificate renewal rejected",
        )
            .into_response(),
    }
}

#[derive(Clone)]
struct WorkerEnrollmentHandle {
    repository: Arc<SqliteJobRepository>,
}

struct EnrollmentAuthenticator {
    service: Arc<WorkerEnrollmentHandle>,
}

impl super::WorkerAuthenticator for EnrollmentAuthenticator {
    fn authenticate(&self, request: &axum::http::Request<axum::body::Body>) -> Option<WorkerId> {
        let certificates = request.extensions().get::<PeerCertificates>()?;
        let certificate = certificates.leaf()?;
        let now_ms = now_ms().ok()?;
        self.service
            .repository
            .authenticate_worker_certificate(&hash(certificate.as_ref()), now_ms)
            .ok()?
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut result = [0_u8; 32];
    result.copy_from_slice(&digest);
    result
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn now_ms() -> Result<i64, IdentityError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IdentityError::Clock)?
        .as_millis()
        .try_into()
        .map_err(|_| IdentityError::Clock)
}

fn millis(value: OffsetDateTime) -> Result<i64, IdentityError> {
    value
        .unix_timestamp_nanos()
        .checked_div(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(IdentityError::Clock)
}

fn parse_pem_der(pem: &str, tag: &str) -> Result<Vec<u8>, IdentityError> {
    let begin = format!("-----BEGIN {tag}-----");
    let end = format!("-----END {tag}-----");
    let body = pem
        .trim()
        .strip_prefix(&begin)
        .and_then(|body| body.strip_suffix(&end))
        .ok_or(IdentityError::InvalidCertificate)?
        .lines()
        .flat_map(str::as_bytes)
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|_| IdentityError::InvalidCertificate)
}

fn verify_ca_key(certificate_pem: &str, key_pem: &str) -> Result<(), IdentityError> {
    let certificate_der = parse_pem_der(certificate_pem, "CERTIFICATE")?;
    let (_, certificate) = x509_parser::parse_x509_certificate(&certificate_der)
        .map_err(|_| IdentityError::InvalidAuthority)?;
    let key_pair = KeyPair::from_pem(key_pem).map_err(|_| IdentityError::InvalidAuthority)?;
    let can_sign_certificates = certificate
        .key_usage()
        .map_err(|_| IdentityError::InvalidAuthority)?
        .is_some_and(|usage| usage.value.key_cert_sign());
    if !certificate.is_ca()
        || !can_sign_certificates
        || certificate.public_key().subject_public_key.data.as_ref() != key_pair.public_key_raw()
    {
        return Err(IdentityError::InvalidAuthority);
    }
    Ok(())
}

fn read_regular_file(path: &Path, private: bool) -> Result<String, IdentityError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| IdentityError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(IdentityError::Io);
    }
    #[cfg(unix)]
    if private && metadata.permissions().mode() & 0o077 != 0 {
        return Err(IdentityError::Io);
    }
    let mut file = File::open(path).map_err(|_| IdentityError::Io)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|_| IdentityError::Io)?;
    Ok(contents)
}

fn write_new_file(path: &Path, bytes: &[u8], private: bool) -> Result<(), IdentityError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| IdentityError::Io)?;
    file.write_all(bytes).map_err(|_| IdentityError::Io)?;
    file.sync_all().map_err(|_| IdentityError::Io)
}

fn prepare_identity_directory(path: &Path) -> Result<PathBuf, IdentityError> {
    fs::create_dir_all(path).map_err(|_| IdentityError::Io)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| IdentityError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(IdentityError::Io);
    }
    let canonical = fs::canonicalize(path).map_err(|_| IdentityError::Io)?;
    #[cfg(unix)]
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))
        .map_err(|_| IdentityError::Io)?;
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use conductor_core::{ProtocolOffer, ProtocolVersionRange, WorkerDescriptor, WorkerStatus};
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tower::ServiceExt;

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let id = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("conductor-identity-{}-{id}", std::process::id()));
            fs::create_dir(&path).expect("create isolated identity directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

    impl<T> Drop for AbortOnDrop<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    fn csr() -> (String, String) {
        let key = WorkerKeyMaterial::generate().expect("generate local worker key");
        (
            key.certificate_signing_request_pem().to_owned(),
            key.private_key_pem().to_owned(),
        )
    }

    #[test]
    fn ca_is_persistent_and_private_key_permissions_are_restricted() {
        let directory = TempDir::new();
        let first = CertificateAuthority::open_or_create(&directory.0).expect("create CA");
        let certificate = first.certificate_pem().to_owned();
        drop(first);
        let reopened = CertificateAuthority::open_or_create(&directory.0).expect("reopen CA");
        assert_eq!(reopened.certificate_pem(), certificate);
        let worker_key = WorkerKeyMaterial::generate().expect("generate worker key locally");
        let worker_key_path = directory.0.join("worker-key.pem");
        worker_key
            .write_private_key_once(&worker_key_path)
            .expect("persist worker key securely");
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(directory.0.join("worker-ca-key.pem"))
                .expect("key metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(worker_key_path)
                .expect("worker key metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn enrollment_rejects_replay_and_renewal_requires_current_certificate() {
        let directory = TempDir::new();
        let authority = Arc::new(
            CertificateAuthority::open_or_create(directory.0.join("ca")).expect("create CA"),
        );
        let repository = Arc::new(
            SqliteJobRepository::open(directory.0.join("manager.sqlite3")).expect("manager store"),
        );
        let service = WorkerEnrollmentService::new(Arc::clone(&repository), authority);
        let worker_id = WorkerId::new("worker-a").expect("worker id");
        let credential = service
            .create_credential(&worker_id, now_ms().expect("clock") + 60_000)
            .expect("issue credential");
        let (first_csr, first_key) = csr();
        let first = service
            .enroll(&credential, &first_csr)
            .expect("enroll worker");
        assert_eq!(first.worker_id, worker_id);
        assert!(service.enroll(&credential, &first_csr).is_err());

        let first_der =
            parse_pem_der(&first.certificate_pem, "CERTIFICATE").expect("issued certificate");
        let (next_csr, _) = csr();
        let renewed = service
            .renew(&first_der, &next_csr)
            .expect("renew worker certificate");
        assert_eq!(renewed.worker_id, first.worker_id);
        assert_eq!(service.authenticate_certificate(&first_der), Ok(None));
        let renewed_der =
            parse_pem_der(&renewed.certificate_pem, "CERTIFICATE").expect("renewed certificate");
        assert_eq!(
            service.authenticate_certificate(&renewed_der),
            Ok(Some(worker_id.clone()))
        );
        assert!(service.revoke(&worker_id).expect("revoke worker"));
        assert_eq!(service.authenticate_certificate(&renewed_der), Ok(None));
        assert!(!first_key.is_empty());
    }

    #[test]
    fn server_tls_configuration_uses_the_client_ca_and_http_alpn() {
        let directory = TempDir::new();
        let authority =
            CertificateAuthority::open_or_create(directory.0.clone()).expect("create CA");
        let identity = authority
            .issue_server_identity("localhost")
            .expect("issue server cert");
        let config = authority
            .server_config(&identity.certificate_pem, identity.private_key_pem())
            .expect("mTLS server config");
        assert!(!config.alpn_protocols.is_empty());
    }

    #[tokio::test]
    async fn enrollment_route_rejects_plain_http_requests() {
        let directory = TempDir::new();
        let authority = Arc::new(
            CertificateAuthority::open_or_create(directory.0.join("ca")).expect("create CA"),
        );
        let repository = Arc::new(
            SqliteJobRepository::open(directory.0.join("manager.sqlite3")).expect("manager store"),
        );
        let service = Arc::new(WorkerEnrollmentService::new(repository, authority));
        let request_body = serde_json::to_vec(&EnrollmentRequest {
            credential: "invalid".to_owned(),
            csr_pem: "invalid".to_owned(),
        })
        .expect("encode request");
        let response = service
            .routes()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/worker/enroll")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(request_body))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
    }

    #[tokio::test]
    async fn enrollment_and_mtls_renewal_work_over_tls_and_reject_missing_identity() {
        let directory = TempDir::new();
        let authority = Arc::new(
            CertificateAuthority::open_or_create(directory.0.join("ca")).expect("create CA"),
        );
        let repository = Arc::new(
            SqliteJobRepository::open(directory.0.join("manager.sqlite3")).expect("manager store"),
        );
        let service = Arc::new(WorkerEnrollmentService::new(
            Arc::clone(&repository),
            Arc::clone(&authority),
        ));
        let worker_id = WorkerId::new("worker-live").expect("worker id");
        let descriptor = WorkerDescriptor::new(worker_id.clone(), 4, true).with_incarnation(1);
        let offer = ProtocolOffer::new(
            ProtocolVersionRange::current(),
            BTreeSet::new(),
            BTreeSet::new(),
        )
        .expect("protocol offer");
        let hub = super::super::WorkerTransportHub::default();
        hub.register_worker(descriptor.clone(), offer.clone())
            .expect("register worker");
        let server_identity = authority
            .issue_server_identity("localhost")
            .expect("server certificate");
        let acceptor = authority
            .mtls_acceptor(
                &server_identity.certificate_pem,
                server_identity.private_key_pem(),
            )
            .expect("mTLS server acceptor");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let address = listener.local_addr().expect("listener address");
        let server = axum_server::from_tcp(listener)
            .expect("wrap bound listener")
            .acceptor(acceptor)
            .serve(service.router_with_hub(&hub).into_make_service());
        let _server_task = AbortOnDrop(tokio::spawn(server));

        let credential = service
            .create_credential(&worker_id, now_ms().expect("clock") + 60_000)
            .expect("create enrollment credential");
        let (first_csr, first_key) = csr();
        let base_url = format!("https://localhost:{}/v1/worker/enroll", address.port());
        let root = reqwest::Certificate::from_pem(authority.certificate_pem().as_bytes())
            .expect("parse bootstrap CA");
        let no_identity_client = reqwest::Client::builder()
            .add_root_certificate(root.clone())
            .timeout(Duration::from_secs(3))
            .build()
            .expect("client without worker identity");
        let enroll_body = serde_json::to_vec(&EnrollmentRequest {
            credential,
            csr_pem: first_csr,
        })
        .expect("encode enrollment request");
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            no_identity_client
                .post(&base_url)
                .header("content-type", "application/json")
                .body(enroll_body)
                .send(),
        )
        .await
        .expect("bounded enrollment request")
        .expect("TLS enrollment response");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        let response_body = response.bytes().await.expect("identity response body");
        let issued: IssuedWorkerIdentity =
            serde_json::from_slice(&response_body).expect("decode identity response");

        let no_certificate_renewal = no_identity_client
            .post(format!(
                "https://localhost:{}/v1/worker/renew",
                address.port()
            ))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&RenewalRequest { csr_pem: csr().0 }).expect("encode renewal"))
            .send()
            .await
            .expect("anonymous TLS connection is allowed for enrollment routes");
        assert_eq!(
            no_certificate_renewal.status(),
            reqwest::StatusCode::UNAUTHORIZED
        );

        let mut worker_identity_pem = issued.certificate_pem.into_bytes();
        worker_identity_pem.extend_from_slice(first_key.as_bytes());
        let identity = reqwest::Identity::from_pem(&worker_identity_pem).expect("client identity");
        let worker_client = reqwest::Client::builder()
            .add_root_certificate(root.clone())
            .identity(identity)
            .timeout(Duration::from_secs(3))
            .build()
            .expect("mTLS worker client");
        let poll_url = format!("https://localhost:{}/v1/worker/poll", address.port());
        let mismatched_poll = super::super::WorkerPollRequest {
            worker_id: WorkerId::new("worker-impostor").expect("impostor id"),
            worker_incarnation: 1,
            status: WorkerStatus::new(
                descriptor.clone(),
                0,
                0,
                true,
                false,
                now_ms().expect("clock"),
            ),
            protocol_offer: offer.clone(),
        };
        let impersonation = worker_client
            .post(&poll_url)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&mismatched_poll).expect("encode mismatched worker request"))
            .send()
            .await
            .expect("present valid but mismatched identity");
        assert_eq!(impersonation.status(), reqwest::StatusCode::FORBIDDEN);
        let (renewal_csr, renewal_key) = csr();
        let renewal = worker_client
            .post(format!(
                "https://localhost:{}/v1/worker/renew",
                address.port()
            ))
            .header("content-type", "application/json")
            .body(
                serde_json::to_vec(&RenewalRequest {
                    csr_pem: renewal_csr,
                })
                .expect("encode renewal"),
            )
            .send()
            .await
            .expect("verified mTLS renewal");
        assert_eq!(renewal.status(), reqwest::StatusCode::OK);
        let renewed_body = renewal.bytes().await.expect("renewed identity body");
        let renewed_identity: IssuedWorkerIdentity =
            serde_json::from_slice(&renewed_body).expect("decode renewed identity");
        let mut renewed_identity_pem = renewed_identity.certificate_pem.into_bytes();
        renewed_identity_pem.extend_from_slice(renewal_key.as_bytes());
        let renewed_identity =
            reqwest::Identity::from_pem(&renewed_identity_pem).expect("renewed client identity");
        let renewed_client = reqwest::Client::builder()
            .add_root_certificate(root)
            .identity(renewed_identity)
            .timeout(Duration::from_secs(3))
            .build()
            .expect("renewed worker client");
        let reconnected = renewed_client
            .post(format!(
                "https://localhost:{}/v1/worker/report",
                address.port()
            ))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .expect("reconnect using renewed certificate");
        assert_eq!(reconnected.status(), reqwest::StatusCode::BAD_REQUEST);
    }
}
