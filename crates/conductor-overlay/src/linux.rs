//! Linux TUN and route lifecycle for a separately privileged helper process.
//!
//! Call this module only from the helper service. The worker that launches user
//! workloads must not receive `CAP_NET_ADMIN`.

use futures_util::StreamExt;
use rtnetlink::packet_route::route::{RouteAddress, RouteAttribute, RouteProtocol};
use rtnetlink::{Handle, RouteMessageBuilder};
use std::collections::BTreeSet;
use std::io;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tun_rs::{AsyncDevice, DeviceBuilder};

/// Route protocol tag reserved for Conductor-owned overlay routes.
const CONDUCTOR_ROUTE_PROTOCOL: RouteProtocol = RouteProtocol::Other(240);
const MAX_INTERFACE_NAME_BYTES: usize = 15;

/// One owned Linux TUN interface and its manager-authorized peer routes.
///
/// Dropping the TUN descriptor removes the non-persistent interface. Call
/// `shutdown` first so only routes tagged as Conductor-owned are removed.
pub struct LinuxOverlayInterface {
    device: Arc<AsyncDevice>,
    route_handle: Handle,
    interface_index: u32,
    interface_name: String,
    local_address: Ipv4Addr,
}

impl LinuxOverlayInterface {
    /// Create or attach to the named TUN interface and reconcile its /32 peer
    /// routes. The caller must be the isolated helper process with
    /// `CAP_NET_ADMIN`; the ordinary worker must not call this function.
    pub async fn start(
        interface_name: &str,
        local_address: Ipv4Addr,
        peer_addresses: impl IntoIterator<Item = Ipv4Addr>,
    ) -> io::Result<Self> {
        validate_interface_name(interface_name)?;
        let peer_addresses = peer_addresses.into_iter().collect::<Vec<_>>();
        let peer_routes = peer_addresses.iter().copied().collect::<BTreeSet<_>>();
        if peer_routes.len() != peer_addresses.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer addresses must be unique",
            ));
        }
        if peer_routes.len() > crate::MAX_OVERLAY_PEERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer count exceeds the supported limit",
            ));
        }
        if peer_routes.contains(&local_address) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer address duplicates local address",
            ));
        }

        let device = DeviceBuilder::new()
            .name(interface_name)
            .mtu(u16::try_from(crate::MAX_OVERLAY_PACKET_BYTES).map_err(io::Error::other)?)
            .ipv4(local_address, 32, None)
            .build_async()?;

        let (connection, route_handle, messages) =
            rtnetlink::new_connection().map_err(io::Error::other)?;
        tokio::spawn(connection);
        drop(messages);

        let link = route_handle
            .link()
            .get()
            .match_name(interface_name)
            .execute()
            .next()
            .await
            .transpose()
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "TUN link not found"))?;
        let interface_index = link.header.index;

        let mut interface = Self {
            device: Arc::new(device),
            route_handle,
            interface_index,
            interface_name: interface_name.to_owned(),
            local_address,
        };
        if let Err(error) = interface.reconcile_routes(&peer_routes).await {
            let _ = interface.remove_owned_routes().await;
            return Err(error);
        }
        Ok(interface)
    }

    pub fn device(&self) -> Arc<AsyncDevice> {
        Arc::clone(&self.device)
    }

    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    pub fn local_address(&self) -> Ipv4Addr {
        self.local_address
    }

    /// Return the addresses of routes currently installed by this interface.
    pub async fn installed_peer_routes(&self) -> io::Result<Vec<Ipv4Addr>> {
        let mut routes = self
            .route_snapshot()
            .await?
            .into_iter()
            .filter(|route| self.owns_route(route))
            .filter_map(|route| route_destination(&route))
            .collect::<Vec<_>>();
        routes.sort_unstable();
        routes.dedup();
        Ok(routes)
    }

    /// Replace the peer route set. Existing host routes are left untouched;
    /// an address occupied by a non-Conductor route is reported as a conflict.
    pub async fn reconcile_routes(&mut self, desired: &BTreeSet<Ipv4Addr>) -> io::Result<()> {
        if desired.len() > crate::MAX_OVERLAY_PEERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer count exceeds the supported limit",
            ));
        }
        if desired.contains(&self.local_address) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlay peer address duplicates local address",
            ));
        }

        let existing = self.route_snapshot().await?;
        for address in desired {
            let route_conflict = existing
                .iter()
                .any(|route| route_covers_address(route, *address) && !self.owns_route(route));
            if route_conflict {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "overlay peer address conflicts with an existing host route",
                ));
            }
        }

        for route in existing.iter().filter(|route| self.owns_route(route)) {
            let Some(destination) = route_destination(route) else {
                continue;
            };
            if !desired.contains(&destination) {
                self.route_handle
                    .route()
                    .del(route.clone())
                    .execute()
                    .await
                    .map_err(io::Error::other)?;
            }
        }

        for address in desired {
            if existing
                .iter()
                .any(|route| route_destination(route) == Some(*address) && self.owns_route(route))
            {
                continue;
            }
            let mut route = RouteMessageBuilder::<Ipv4Addr>::new()
                .destination_prefix(*address, 32)
                .output_interface(self.interface_index)
                .build();
            route.header.protocol = CONDUCTOR_ROUTE_PROTOCOL;
            self.route_handle
                .route()
                .add(route)
                .execute()
                .await
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    pub async fn shutdown(self) -> io::Result<()> {
        self.remove_owned_routes().await
    }

    async fn route_snapshot(
        &self,
    ) -> io::Result<Vec<rtnetlink::packet_route::route::RouteMessage>> {
        let mut routes = self
            .route_handle
            .route()
            .get(RouteMessageBuilder::<Ipv4Addr>::new().build())
            .execute();
        let mut snapshot = Vec::new();
        while let Some(route) = routes.next().await {
            snapshot.push(route.map_err(io::Error::other)?);
        }
        Ok(snapshot)
    }

    fn owns_route(&self, route: &rtnetlink::packet_route::route::RouteMessage) -> bool {
        is_owned_route(route, self.interface_index)
    }

    async fn remove_owned_routes(&self) -> io::Result<()> {
        for route in self.route_snapshot().await? {
            if self.owns_route(&route) {
                self.route_handle
                    .route()
                    .del(route)
                    .execute()
                    .await
                    .map_err(io::Error::other)?;
            }
        }
        Ok(())
    }
}

fn route_destination(route: &rtnetlink::packet_route::route::RouteMessage) -> Option<Ipv4Addr> {
    route
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            RouteAttribute::Destination(RouteAddress::Inet(address)) => Some(*address),
            _ => None,
        })
}

fn route_covers_address(
    route: &rtnetlink::packet_route::route::RouteMessage,
    address: Ipv4Addr,
) -> bool {
    let prefix = route.header.destination_prefix_length;
    if prefix == 0 || prefix > 32 {
        return false;
    }
    route_destination(route).is_some_and(|destination| {
        let mask = u32::MAX << (32 - u32::from(prefix));
        u32::from(destination) & mask == u32::from(address) & mask
    })
}

fn is_owned_route(route: &rtnetlink::packet_route::route::RouteMessage, index: u32) -> bool {
    route.header.protocol == CONDUCTOR_ROUTE_PROTOCOL
        && route.attributes.iter().any(
            |attribute| matches!(attribute, RouteAttribute::Oif(route_index) if *route_index == index),
        )
}

fn validate_interface_name(name: &str) -> io::Result<()> {
    if name.len() < 4
        || name.len() > MAX_INTERFACE_NAME_BYTES
        || !name.starts_with("cnd-")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name must be a bounded cnd- name",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtnetlink::packet_route::route::{RouteAttribute, RouteMessage};

    #[test]
    fn interface_names_are_narrowly_scoped() {
        assert!(validate_interface_name("cnd-worker1").is_ok());
        assert!(validate_interface_name("eth0").is_err());
        assert!(validate_interface_name("cnd-THIS-IS-TOO-LONG").is_err());
        assert!(validate_interface_name("cnd-a;ip link").is_err());
    }

    #[test]
    fn route_ownership_requires_both_tag_and_interface() {
        let mut owned = RouteMessage::default();
        owned.header.protocol = CONDUCTOR_ROUTE_PROTOCOL;
        owned.attributes.push(RouteAttribute::Oif(7));
        assert!(is_owned_route(&owned, 7));
        owned.attributes[0] = RouteAttribute::Oif(8);
        assert!(!is_owned_route(&owned, 7));
        owned.attributes[0] = RouteAttribute::Oif(7);
        owned.header.protocol = RouteProtocol::Static;
        assert!(!is_owned_route(&owned, 7));
    }

    #[test]
    fn route_overlap_check_ignores_default_and_detects_specific_overlap() {
        let address = Ipv4Addr::new(100, 64, 12, 3);
        let mut route = RouteMessage::default();
        route
            .attributes
            .push(RouteAttribute::Destination(RouteAddress::Inet(
                Ipv4Addr::UNSPECIFIED,
            )));
        route.header.destination_prefix_length = 0;
        assert!(!route_covers_address(&route, address));
        route.attributes[0] =
            RouteAttribute::Destination(RouteAddress::Inet(Ipv4Addr::new(100, 64, 12, 0)));
        route.header.destination_prefix_length = 24;
        assert!(route_covers_address(&route, address));
        assert!(!route_covers_address(&route, Ipv4Addr::new(100, 64, 13, 3)));
    }
}
