//! Moka Cache - In-Memory Cache Backend
//!
//! High-performance in-memory cache using Moka for hot data storage.
//!
//! You can either use moka's [`Cache<String, CacheEntry>`] directly or wrap it with [`MokaCache`]
//! for stats tracking.

use anyhow::Result;
use async_trait::async_trait;
use moka::Expiry;
use moka::future::Cache;
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, trace};

use crate::traits::CacheBackend;

/// Moka cache entry with TTL information.
///
/// This cache entry implements the `Expiry` trait for Moka cache.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    value: serde_json::Value,
    expires_at: Instant,
}

impl CacheEntry {
    fn new(value: serde_json::Value, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }
}

impl Expiry<String, CacheEntry> for CacheEntry {
    fn expire_after_create(
        &self,
        _key: &String,
        _value: &CacheEntry,
        created_at: Instant,
    ) -> Option<Duration> {
        Some(self.expires_at - created_at)
    }
}

/// Wrapper around Moka's [`Cache`] with stats tracking.
pub struct MokaCache {
    /// Moka cache instance
    cache: Cache<String, CacheEntry>,
    /// Hit counter
    hits: Arc<AtomicU64>,
    /// Miss counter
    misses: Arc<AtomicU64>,
    /// Set counter
    sets: Arc<AtomicU64>,
}

impl MokaCache {
    /// Create new Moka cache
    pub fn new(cache: Cache<String, CacheEntry>) -> Result<Self> {
        debug!("Initializing Moka Cache");

        Ok(Self {
            cache,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            sets: Arc::new(AtomicU64::new(0)),
        })
    }
}

// ===== Trait Implementations =====

#[async_trait]
impl CacheBackend for MokaCache {
    async fn get(&self, key: &str) -> Option<serde_json::Value> {
        if let Some(entry) = self.cache.get(key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    async fn get_with_ttl(&self, key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        if let Some(entry) = self.cache.get(key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some((entry.value, Some(entry.expires_at - Instant::now())))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    async fn set_with_ttl(&self, key: &str, value: serde_json::Value, ttl: Duration) -> Result<()> {
        let entry = CacheEntry::new(value, ttl);
        self.cache.insert(key.to_string(), entry).await;
        self.sets.fetch_add(1, Ordering::Relaxed);
        trace!(key = %key, ttl_secs = %ttl.as_secs(), "[Moka] Cached key with TTL");
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.cache.remove(key).await;
        Ok(())
    }

    async fn health_check(&self) -> bool {
        // Test basic functionality with custom TTL
        let test_key = "health_check_moka";
        let test_value = serde_json::json!({"test": true});

        match self
            .set_with_ttl(test_key, test_value.clone(), Duration::from_secs(60))
            .await
        {
            Ok(()) => match self.get(test_key).await {
                Some(retrieved) => {
                    let _ = self.remove(test_key).await;
                    retrieved == test_value
                }
                None => false,
            },
            Err(_) => false,
        }
    }

    fn name(&self) -> &'static str {
        "Moka"
    }
}

#[async_trait]
impl CacheBackend for Cache<String, CacheEntry> {
    async fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.get(key).await.map(|value| value.value)
    }

    async fn get_with_ttl(&self, key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        self.get(key)
            .await
            .map(|value| (value.value, Some(value.expires_at - Instant::now())))
    }

    async fn set_with_ttl(&self, key: &str, value: serde_json::Value, ttl: Duration) -> Result<()> {
        let entry = CacheEntry::new(value, ttl);
        self.insert(key.to_string(), entry).await;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.remove(key).await;
        Ok(())
    }

    async fn health_check(&self) -> bool {
        // Test basic functionality with custom TTL
        let test_key = "health_check_moka";
        let test_value = serde_json::json!({"test": true});

        match self
            .set_with_ttl(test_key, test_value.clone(), Duration::from_secs(60))
            .await
        {
            Ok(()) => match self.get(test_key).await {
                Some(retrieved) => {
                    let _ = self.remove(test_key).await;
                    retrieved.value == test_value
                }
                None => false,
            },
            Err(_) => false,
        }
    }

    fn name(&self) -> &'static str {
        "Moka"
    }
}

/// Cache statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
}
