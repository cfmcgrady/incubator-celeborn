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

//! Connection management for Celeborn client.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use crate::error::{CelebornError, Result};
use crate::network::codec::{CelebornCodec, Frame};
use crate::protocol::message::{MessageType, RpcRequest};
use crate::protocol::Encodable;

/// A single connection to a Celeborn server.
pub struct Connection {
    /// Remote address
    addr: SocketAddr,
    /// Sender for outgoing frames
    sender: mpsc::Sender<Frame>,
    /// Pending requests waiting for responses
    pending_requests: Arc<DashMap<i64, oneshot::Sender<Result<Frame>>>>,
    /// Request ID counter
    request_id_counter: AtomicU64,
    /// Whether the connection is active
    active: AtomicBool,
    /// Semaphore for limiting in-flight requests
    in_flight_semaphore: Arc<Semaphore>,
}

impl Connection {
    /// Create a new connection to the given address.
    pub async fn connect(addr: SocketAddr, max_in_flight: usize) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.map_err(|e| {
            CelebornError::Connection(format!("Failed to connect to {}: {}", addr, e))
        })?;

        // Set TCP options
        stream.set_nodelay(true).ok();

        let framed = Framed::new(stream, CelebornCodec::new());
        let (sink, stream) = framed.split();

        let pending_requests: Arc<DashMap<i64, oneshot::Sender<Result<Frame>>>> =
            Arc::new(DashMap::new());
        let (sender, receiver) = mpsc::channel(max_in_flight);

        let in_flight_semaphore = Arc::new(Semaphore::new(max_in_flight));

        // Spawn writer task
        let writer_handle = tokio::spawn(Self::writer_loop(receiver, sink));

        // Spawn reader task
        let pending_clone = pending_requests.clone();
        let reader_handle = tokio::spawn(Self::reader_loop(stream, pending_clone));

        debug!("Connected to {}", addr);

        Ok(Self {
            addr,
            sender,
            pending_requests,
            request_id_counter: AtomicU64::new(1),
            active: AtomicBool::new(true),
            in_flight_semaphore,
        })
    }

    /// Writer loop - sends frames to the server.
    async fn writer_loop(
        mut receiver: mpsc::Receiver<Frame>,
        mut sink: SplitSink<Framed<TcpStream, CelebornCodec>, Frame>,
    ) {
        while let Some(frame) = receiver.recv().await {
            if let Err(e) = sink.send(frame).await {
                error!("Failed to send frame: {}", e);
                break;
            }
        }
        debug!("Writer loop ended");
    }

    /// Reader loop - receives frames from the server.
    async fn reader_loop(
        mut stream: SplitStream<Framed<TcpStream, CelebornCodec>>,
        pending_requests: Arc<DashMap<i64, oneshot::Sender<Result<Frame>>>>,
    ) {
        while let Some(result) = stream.next().await {
            match result {
                Ok(frame) => {
                    // Extract request ID from the frame based on message type
                    let request_id = Self::extract_request_id(&frame);
                    
                    if let Some(request_id) = request_id {
                        if let Some((_, sender)) = pending_requests.remove(&request_id) {
                            let _ = sender.send(Ok(frame));
                        } else {
                            warn!("Received response for unknown request: {}", request_id);
                        }
                    } else {
                        trace!("Received frame without request ID: {:?}", frame.message_type);
                    }
                }
                Err(e) => {
                    error!("Failed to read frame: {}", e);
                    break;
                }
            }
        }
        
        // Connection closed - fail all pending requests
        for entry in pending_requests.iter() {
            let _ = entry.value();
        }
        pending_requests.clear();
        
        debug!("Reader loop ended");
    }

    /// Extract request ID from a frame.
    fn extract_request_id(frame: &Frame) -> Option<i64> {
        match frame.message_type {
            MessageType::RpcResponse | MessageType::RpcFailure => {
                if frame.payload.len() >= 8 {
                    let bytes: [u8; 8] = frame.payload[..8].try_into().ok()?;
                    Some(i64::from_be_bytes(bytes))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Send an RPC request and wait for response.
    pub async fn send_rpc(
        &self,
        body: Bytes,
        timeout_duration: Duration,
    ) -> Result<Frame> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(CelebornError::Connection("Connection is closed".to_string()));
        }

        // Acquire semaphore permit
        let _permit = self
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| CelebornError::Connection("Semaphore closed".to_string()))?;

        let request_id = self.request_id_counter.fetch_add(1, Ordering::Relaxed) as i64;
        
        // Create the RPC request
        let request = RpcRequest::new(request_id, body);
        let mut buf = request.encode_to_bytes();
        
        // Create frame (skip the type byte since it's already in the encoded data)
        let frame = Frame::new(MessageType::RpcRequest, buf.freeze().slice(1..));

        // Register pending request
        let (tx, rx) = oneshot::channel();
        self.pending_requests.insert(request_id, tx);

        // Send the frame
        self.sender
            .send(frame)
            .await
            .map_err(|_| CelebornError::Connection("Failed to send request".to_string()))?;

        // Wait for response with timeout
        match timeout(timeout_duration, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending_requests.remove(&request_id);
                Err(CelebornError::Connection("Request cancelled".to_string()))
            }
            Err(_) => {
                self.pending_requests.remove(&request_id);
                Err(CelebornError::Timeout(timeout_duration.as_millis() as u64))
            }
        }
    }

    /// Send a one-way message (no response expected).
    pub async fn send_one_way(&self, frame: Frame) -> Result<()> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(CelebornError::Connection("Connection is closed".to_string()));
        }

        self.sender
            .send(frame)
            .await
            .map_err(|_| CelebornError::Connection("Failed to send message".to_string()))
    }

    /// Check if the connection is active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Close the connection.
    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Get the remote address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Connection pool for managing multiple connections to servers.
pub struct ConnectionPool {
    /// Connections by address
    connections: DashMap<SocketAddr, Vec<Arc<Connection>>>,
    /// Pool size per address
    pool_size: usize,
    /// Maximum in-flight requests per connection
    max_in_flight: usize,
    /// Round-robin index for connection selection
    rr_index: DashMap<SocketAddr, AtomicU64>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(pool_size: usize, max_in_flight: usize) -> Self {
        Self {
            connections: DashMap::new(),
            pool_size,
            max_in_flight,
            rr_index: DashMap::new(),
        }
    }

    /// Get or create a connection to the given address.
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<Connection>> {
        // Check if we have existing connections
        if let Some(conns) = self.connections.get(&addr) {
            // Find an active connection using round-robin
            let index = self
                .rr_index
                .entry(addr)
                .or_insert_with(|| AtomicU64::new(0));
            let idx = index.fetch_add(1, Ordering::Relaxed) as usize % conns.len();
            
            if conns[idx].is_active() {
                return Ok(conns[idx].clone());
            }
        }

        // Create new connections
        self.create_connections(addr).await?;

        // Return the first connection
        self.connections
            .get(&addr)
            .and_then(|conns| conns.first().cloned())
            .ok_or_else(|| CelebornError::Connection("Failed to get connection".to_string()))
    }

    /// Create connections to the given address.
    async fn create_connections(&self, addr: SocketAddr) -> Result<()> {
        let mut new_conns = Vec::with_capacity(self.pool_size);
        
        for _ in 0..self.pool_size {
            match Connection::connect(addr, self.max_in_flight).await {
                Ok(conn) => new_conns.push(Arc::new(conn)),
                Err(e) => {
                    // If we have at least one connection, that's okay
                    if new_conns.is_empty() {
                        return Err(e);
                    }
                    warn!("Failed to create additional connection to {}: {}", addr, e);
                    break;
                }
            }
        }

        self.connections.insert(addr, new_conns);
        self.rr_index.insert(addr, AtomicU64::new(0));
        
        Ok(())
    }

    /// Remove all connections to the given address.
    pub fn remove_connections(&self, addr: &SocketAddr) {
        if let Some((_, conns)) = self.connections.remove(addr) {
            for conn in conns {
                conn.close();
            }
        }
        self.rr_index.remove(addr);
    }

    /// Close all connections.
    pub fn close_all(&self) {
        for entry in self.connections.iter() {
            for conn in entry.value() {
                conn.close();
            }
        }
        self.connections.clear();
        self.rr_index.clear();
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let pool = ConnectionPool::new(2, 32);
        assert!(pool.connections.is_empty());
    }
}
