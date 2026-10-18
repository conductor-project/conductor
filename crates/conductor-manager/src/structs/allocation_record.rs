use super::*;

/// Durable service-slot allocation and the shared host domain that owns its
/// reservation. Pending allocations have no worker or resource domain yet.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocationRecord {
    pub(super) allocation: Allocation,
    pub(super) resource_domain_id: Option<WorkerId>,
}

impl AllocationRecord {
    pub fn allocation(&self) -> &Allocation {
        &self.allocation
    }

    pub fn resource_domain_id(&self) -> Option<&WorkerId> {
        self.resource_domain_id.as_ref()
    }
}
