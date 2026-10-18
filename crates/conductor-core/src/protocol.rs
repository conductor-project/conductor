//! Version negotiation for the manager-worker wire protocol.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU16;

use serde::{Deserialize, Deserializer, Serialize};

/// The protocol version implemented by this MVP2 release.
pub const CURRENT_PROTOCOL_VERSION: u16 = 3;

/// An inclusive range of protocol versions a peer can speak compatibly.
///
/// Versions are monotonically increasing wire-contract revisions. A peer may
/// select any version in the overlap; incompatible wire changes require a new
/// version outside the prior compatibility range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersionRange {
    min: NonZeroU16,
    max: NonZeroU16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawProtocolVersionRange {
    min: NonZeroU16,
    max: NonZeroU16,
}

impl ProtocolVersionRange {
    pub fn current() -> Self {
        let version =
            NonZeroU16::new(CURRENT_PROTOCOL_VERSION).expect("protocol version is nonzero");
        Self {
            min: version,
            max: version,
        }
    }

    pub fn new(min: u16, max: u16) -> Result<Self, ProtocolNegotiationError> {
        let min = NonZeroU16::new(min).ok_or(ProtocolNegotiationError::InvalidVersionRange)?;
        let max = NonZeroU16::new(max).ok_or(ProtocolNegotiationError::InvalidVersionRange)?;
        if min > max {
            return Err(ProtocolNegotiationError::InvalidVersionRange);
        }
        Ok(Self { min, max })
    }

    pub fn min(self) -> u16 {
        self.min.get()
    }

    pub fn max(self) -> u16 {
        self.max.get()
    }
}

impl<'de> Deserialize<'de> for ProtocolVersionRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawProtocolVersionRange::deserialize(deserializer)?;
        if raw.min > raw.max {
            return Err(serde::de::Error::custom(
                ProtocolNegotiationError::InvalidVersionRange,
            ));
        }
        Ok(Self {
            min: raw.min,
            max: raw.max,
        })
    }
}

/// Protocol extensions that can be advertised and required by a peer.
/// Unknown values fail strict JSON decoding and therefore can never be treated
/// as supported capabilities.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolCapability {
    AssignmentGenerations,
    CommandDeduplication,
    OrderedReports,
    WorkerIncarnations,
    WorkerResourceInventory,
}

/// A peer's supported protocol versions and capability requirements.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolOffer {
    versions: ProtocolVersionRange,
    supported_capabilities: BTreeSet<ProtocolCapability>,
    required_capabilities: BTreeSet<ProtocolCapability>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawProtocolOffer {
    versions: ProtocolVersionRange,
    supported_capabilities: BTreeSet<ProtocolCapability>,
    required_capabilities: BTreeSet<ProtocolCapability>,
}

impl ProtocolOffer {
    pub fn new(
        versions: ProtocolVersionRange,
        supported_capabilities: BTreeSet<ProtocolCapability>,
        required_capabilities: BTreeSet<ProtocolCapability>,
    ) -> Result<Self, ProtocolNegotiationError> {
        if !required_capabilities.is_subset(&supported_capabilities) {
            return Err(ProtocolNegotiationError::RequiredCapabilityNotOffered);
        }
        Ok(Self {
            versions,
            supported_capabilities,
            required_capabilities,
        })
    }

    pub fn versions(&self) -> ProtocolVersionRange {
        self.versions
    }

    pub fn supported_capabilities(&self) -> &BTreeSet<ProtocolCapability> {
        &self.supported_capabilities
    }

    pub fn required_capabilities(&self) -> &BTreeSet<ProtocolCapability> {
        &self.required_capabilities
    }

    /// Select the highest compatible version and the mutually supported
    /// capabilities. Either peer's unsatisfied requirement rejects the offer.
    pub fn negotiate(&self, peer: &Self) -> Result<NegotiatedProtocol, ProtocolNegotiationError> {
        let version = self.versions.max().min(peer.versions.max());
        if version < self.versions.min().max(peer.versions.min()) {
            return Err(ProtocolNegotiationError::NoCommonVersion);
        }

        let capabilities = self
            .supported_capabilities
            .intersection(&peer.supported_capabilities)
            .copied()
            .collect::<BTreeSet<_>>();
        if !self.required_capabilities.is_subset(&capabilities)
            || !peer.required_capabilities.is_subset(&capabilities)
        {
            return Err(ProtocolNegotiationError::UnsupportedRequiredCapability);
        }

        Ok(NegotiatedProtocol {
            version,
            capabilities,
        })
    }
}

impl<'de> Deserialize<'de> for ProtocolOffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawProtocolOffer::deserialize(deserializer)?;
        Self::new(
            raw.versions,
            raw.supported_capabilities,
            raw.required_capabilities,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// The protocol version and capabilities selected for one connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegotiatedProtocol {
    version: u16,
    capabilities: BTreeSet<ProtocolCapability>,
}

impl NegotiatedProtocol {
    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn capabilities(&self) -> &BTreeSet<ProtocolCapability> {
        &self.capabilities
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolNegotiationError {
    InvalidVersionRange,
    NoCommonVersion,
    RequiredCapabilityNotOffered,
    UnsupportedRequiredCapability,
}

impl fmt::Display for ProtocolNegotiationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidVersionRange => "protocol version range must be positive and ordered",
            Self::NoCommonVersion => "peers have no common protocol version",
            Self::RequiredCapabilityNotOffered => {
                "required protocol capabilities must also be offered"
            }
            Self::UnsupportedRequiredCapability => {
                "peer requires a protocol capability that is not mutually supported"
            }
        })
    }
}

impl std::error::Error for ProtocolNegotiationError {}
