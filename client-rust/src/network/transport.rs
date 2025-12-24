// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transport client for Celeborn communication.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use prost::Message as ProstMessage;

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result};
use crate::network::master_rpc::{MasterRpcClient, NettyRpcClient};
use crate::protocol::java_serialization::RpcAddress;
use crate::protocol::transport::*;

/// Transport client for communicating with Celeborn servers.
///
/// Both Master and Worker use NettyRpcEnv which requires Java serialization for RPC messages.
/// This client uses the appropriate serialization format for all communications.
pub struct TransportClient {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Master RPC client (uses Java serialization with retry logic)
    master_rpc_client: MasterRpcClient,
    /// Netty RPC client for Worker communication (uses Java serialization)
    netty_rpc_client: NettyRpcClient,
}

impl TransportClient {
    /// Create a new transport client.
    pub fn new(config: Arc<CelebornConfig>) -> Result<Self> {
        let master_endpoints: Vec<SocketAddr> = config
            .master_endpoints
            .iter()
            .filter_map(|ep| {
                ep.to_socket_addrs()
                    .ok()
                    .and_then(|mut addrs| addrs.next())
            })
            .collect();

        if master_endpoints.is_empty() {
            return Err(CelebornError::Config(
                "No valid master endpoints".to_string(),
            ));
        }

        // Create Master RPC client with Java serialization support
        let master_rpc_client = MasterRpcClient::new(
            master_endpoints,
            Some(RpcAddress::new("localhost", 0)),
            config.rpc_timeout,
            config.max_retries as usize,
            config.retry_wait,
        )?;

        // Create Netty RPC client for Worker communication
        let netty_rpc_client = NettyRpcClient::new(
            Some(RpcAddress::new("localhost", 0)),
            config.rpc_timeout,
        );

        Ok(Self {
            config,
            master_rpc_client,
            netty_rpc_client,
        })
    }

    /// Send an RPC request to the master.
    /// Uses Java serialization format required by Celeborn Master.
    pub async fn send_to_master<Req, Resp>(
        &self,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        self.master_rpc_client.send_rpc(message_type, request).await
    }

    /// Send an RPC request to a worker.
    /// Uses Java serialization format required by Celeborn Worker (NettyRpcEnv).
    pub async fn send_to_worker<Req, Resp>(
        &self,
        host: &str,
        port: i32,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        let addr_str = format!("{}:{}", host, port);
        let addr: SocketAddr = addr_str
            .to_socket_addrs()
            .map_err(|e| CelebornError::Connection(format!("Invalid address {}: {}", addr_str, e)))?
            .next()
            .ok_or_else(|| CelebornError::Connection(format!("Cannot resolve {}", addr_str)))?;

        // Worker uses "WorkerEndpoint" as the endpoint name
        self.netty_rpc_client
            .send_rpc_to_endpoint(addr, "WorkerEndpoint", message_type, request)
            .await
    }

    /// Close all connections.
    pub fn close(&self) {
        // NettyRpcClient creates new connections per request, so nothing to close
    }
}

/// Factory for creating transport clients.
pub struct TransportClientFactory {
    config: Arc<CelebornConfig>,
}

impl TransportClientFactory {
    /// Create a new factory.
    pub fn new(config: CelebornConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Create a new transport client.
    pub fn create_client(&self) -> Result<TransportClient> {
        TransportClient::new(self.config.clone())
    }

    /// Get the configuration.
    pub fn config(&self) -> &CelebornConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_client_factory() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build()
            .unwrap();

        let factory = TransportClientFactory::new(config);
        assert_eq!(factory.config().app_id, "test-app");
    }
}
