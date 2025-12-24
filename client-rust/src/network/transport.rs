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

use bytes::Bytes;
use prost::Message as ProstMessage;
use tracing::error;

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::connection::ConnectionPool;
use crate::network::codec::Frame;
use crate::protocol::message::MessageType;
use crate::protocol::transport::*;

/// Transport client for communicating with Celeborn servers.
pub struct TransportClient {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Connection pool
    connection_pool: ConnectionPool,
    /// Master endpoints
    master_endpoints: Vec<SocketAddr>,
    /// Current master index
    current_master_index: std::sync::atomic::AtomicUsize,
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

        let connection_pool = ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        );

        Ok(Self {
            config,
            connection_pool,
            master_endpoints,
            current_master_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Get the current master endpoint.
    fn current_master(&self) -> SocketAddr {
        let index = self
            .current_master_index
            .load(std::sync::atomic::Ordering::Relaxed);
        self.master_endpoints[index % self.master_endpoints.len()]
    }

    /// Switch to the next master endpoint.
    fn switch_master(&self) {
        self.current_master_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Send an RPC request to the master.
    pub async fn send_to_master<Req, Resp>(
        &self,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        let mut last_error = None;
        
        for attempt in 0..self.config.max_retries {
            let master_addr = self.current_master();
            
            match self.send_rpc(master_addr, message_type, request).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    error!(
                        "Failed to send RPC to master {} (attempt {}): {}",
                        master_addr, attempt + 1, e
                    );
                    last_error = Some(e);
                    self.switch_master();
                    
                    if attempt < self.config.max_retries - 1 {
                        tokio::time::sleep(self.config.retry_wait).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CelebornError::Connection("All master endpoints failed".to_string())
        }))
    }

    /// Send an RPC request to a specific address.
    pub async fn send_rpc<Req, Resp>(
        &self,
        addr: SocketAddr,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        let conn = self.connection_pool.get_connection(addr).await?;

        // Encode the request
        let mut payload = Vec::new();
        payload.extend_from_slice(&(message_type as i32).to_be_bytes());
        request.encode(&mut payload).map_err(|e| {
            CelebornError::Serialization(format!("Failed to encode request: {}", e))
        })?;

        // Send RPC
        let response_frame = conn
            .send_rpc(Bytes::from(payload), self.config.rpc_timeout)
            .await?;

        // Decode response
        self.decode_response(response_frame)
    }

    /// Send an RPC request to a worker.
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

        self.send_rpc(addr, message_type, request).await
    }

    /// Decode an RPC response.
    fn decode_response<Resp>(&self, frame: Frame) -> Result<Resp>
    where
        Resp: ProstMessage + Default,
    {
        match frame.message_type {
            MessageType::RpcResponse => {
                // In the new frame format:
                // - frame.message contains: request_id (8 bytes) + body_size (4 bytes)
                // - frame.body contains: the actual protobuf response
                
                // The body contains: message_type (4 bytes) + protobuf data
                if frame.body.len() < 4 {
                    return Err(CelebornError::Protocol(
                        "Response body too short".to_string(),
                    ));
                }
                
                // Skip message type (4 bytes) in the body
                let response_body = &frame.body[4..];
                
                Resp::decode(response_body).map_err(|e| {
                    CelebornError::Serialization(format!("Failed to decode response: {}", e))
                })
            }
            MessageType::RpcFailure => {
                // For RPC failure, the error message is in the message content
                // Format: request_id (8 bytes) + error_string_length (4 bytes) + error_string
                if frame.message.len() < 12 {
                    return Err(CelebornError::Protocol(
                        "RPC failure response too short".to_string(),
                    ));
                }
                
                // Skip request ID (8 bytes), read error string length (4 bytes)
                let error_len = i32::from_be_bytes([
                    frame.message[8], frame.message[9],
                    frame.message[10], frame.message[11]
                ]) as usize;
                
                let error_msg = if frame.message.len() >= 12 + error_len {
                    String::from_utf8_lossy(&frame.message[12..12 + error_len]).to_string()
                } else {
                    "Unknown error".to_string()
                };
                
                Err(CelebornError::ServerError {
                    status: StatusCode::RpcFailed,
                    message: error_msg,
                })
            }
            _ => Err(CelebornError::Protocol(format!(
                "Unexpected response type: {:?}",
                frame.message_type
            ))),
        }
    }

    /// Close all connections.
    pub fn close(&self) {
        self.connection_pool.close_all();
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
