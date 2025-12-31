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

//! Client Manager for connection pooling.
//!
//! This module provides a connection pool manager for reusing Celeborn clients
//! across multiple shuffle operations.

use crate::client::ExecutorShuffleClient;
use crate::config::CelebornConfig;
use crate::error::Result;
use dashmap::DashMap;
use std::sync::Arc;

/// Client manager for reusing Celeborn connections.
///
/// This manager maintains a pool of [`ExecutorShuffleClient`] instances,
/// keyed by application ID. This allows multiple shuffle operations within
/// the same application to share connections.
///
/// # Example
///
/// ```rust,no_run
/// use celeborn_client::repartitioner::ClientManager;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let manager = ClientManager::new();
///     
///     let client = manager.get_or_create_client(
///         "my-app",
///         vec!["localhost:9097".to_string()],
///         "driver-host",
///         9098,
///     ).await?;
///     
///     // Use client for shuffle operations...
///     
///     // When done with the application
///     manager.remove_client("my-app");
///     Ok(())
/// }
/// ```
pub struct ClientManager {
    /// Cached clients by app_id
    clients: DashMap<String, Arc<ExecutorShuffleClient>>,
}

impl ClientManager {
    /// Create a new client manager.
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }

    /// Get or create a client for the given configuration.
    ///
    /// If a client for the given `app_id` already exists, it is returned.
    /// Otherwise, a new client is created, connected to the LifecycleManager,
    /// and cached for future use.
    ///
    /// # Arguments
    /// * `app_id` - Application ID
    /// * `master_endpoints` - Celeborn master endpoints
    /// * `lifecycle_manager_host` - LifecycleManager host (usually the Driver)
    /// * `lifecycle_manager_port` - LifecycleManager port
    ///
    /// # Returns
    /// A shared reference to the client
    pub async fn get_or_create_client(
        &self,
        app_id: &str,
        master_endpoints: Vec<String>,
        lifecycle_manager_host: &str,
        lifecycle_manager_port: i32,
    ) -> Result<Arc<ExecutorShuffleClient>> {
        // Check if client already exists
        if let Some(client) = self.clients.get(app_id) {
            return Ok(Arc::clone(&client));
        }

        // Create new client
        let config = CelebornConfig::builder()
            .app_id(app_id)
            .master_endpoints(master_endpoints)
            .build()?;

        let client = ExecutorShuffleClient::new(config);

        // Setup connection to LifecycleManager
        client
            .setup_lifecycle_manager_ref(lifecycle_manager_host, lifecycle_manager_port)
            .await?;

        let client = Arc::new(client);
        self.clients.insert(app_id.to_string(), Arc::clone(&client));

        Ok(client)
    }

    /// Get or create a client with custom compression settings.
    ///
    /// Similar to [`get_or_create_client`], but allows specifying compression codec.
    ///
    /// # Arguments
    /// * `app_id` - Application ID
    /// * `master_endpoints` - Celeborn master endpoints
    /// * `lifecycle_manager_host` - LifecycleManager host
    /// * `lifecycle_manager_port` - LifecycleManager port
    /// * `compression_codec` - Compression codec to use
    pub async fn get_or_create_client_with_compression(
        &self,
        app_id: &str,
        master_endpoints: Vec<String>,
        lifecycle_manager_host: &str,
        lifecycle_manager_port: i32,
        compression_codec: crate::config::CompressionCodec,
    ) -> Result<Arc<ExecutorShuffleClient>> {
        // Check if client already exists
        if let Some(client) = self.clients.get(app_id) {
            return Ok(Arc::clone(&client));
        }

        // Create new client with compression settings
        let config = CelebornConfig::builder()
            .app_id(app_id)
            .master_endpoints(master_endpoints)
            .compression_codec(compression_codec)
            .build()?;

        let client = ExecutorShuffleClient::new(config);

        // Setup connection to LifecycleManager
        client
            .setup_lifecycle_manager_ref(lifecycle_manager_host, lifecycle_manager_port)
            .await?;

        let client = Arc::new(client);
        self.clients.insert(app_id.to_string(), Arc::clone(&client));

        Ok(client)
    }

    /// Get an existing client by app_id.
    ///
    /// Returns `None` if no client exists for the given app_id.
    pub fn get_client(&self, app_id: &str) -> Option<Arc<ExecutorShuffleClient>> {
        self.clients.get(app_id).map(|r| Arc::clone(&r))
    }

    /// Remove a client from the pool.
    ///
    /// This should be called when an application is done with shuffle operations.
    pub fn remove_client(&self, app_id: &str) -> Option<Arc<ExecutorShuffleClient>> {
        self.clients.remove(app_id).map(|(_, v)| v)
    }

    /// Clear all cached clients.
    ///
    /// This should be called during shutdown to release all resources.
    pub fn clear(&self) {
        self.clients.clear();
    }

    /// Get the number of cached clients.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Check if the manager has no cached clients.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Get all cached app IDs.
    pub fn app_ids(&self) -> Vec<String> {
        self.clients.iter().map(|r| r.key().clone()).collect()
    }
}

impl Default for ClientManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_manager_creation() {
        let manager = ClientManager::new();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_client_manager_default() {
        let manager = ClientManager::default();
        assert!(manager.is_empty());
    }

    #[test]
    fn test_get_nonexistent_client() {
        let manager = ClientManager::new();
        assert!(manager.get_client("nonexistent").is_none());
    }

    #[test]
    fn test_clear() {
        let manager = ClientManager::new();
        manager.clear();
        assert!(manager.is_empty());
    }
}
