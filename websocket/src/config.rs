use crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Clone, Deserialize, Serialize)]
pub struct Authority {
    /// Address of websocket.
    pub websocket_address: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
}

impl Committee {
    pub fn new(info: Vec<(PublicKey, SocketAddr)>) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, websocket_address)| {
                    let authority = Authority { websocket_address };
                    (name, authority)
                })
                .collect(),
        }
    }

    /// Returns the ws addresses of a specific node.
    pub fn ws_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.websocket_address)
    }
}
