#![cfg(feature = "redis")]
//! Multi-Tier Usage Example
//!
//! Demonstrates how to configure a 3-tier cache system (L1 + L2 + L3).
//!
//! Run with: cargo run --example `multi_tier_usage`

use anyhow::Result;
use multi_level_cache::{CacheBackend, CacheSystemBuilder, async_trait};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ==================== Mock L3 Cache (Simulated Disk/Cold Storage) ====================

/// A simulated "slow" cache backend to represent L3 (e.g., Disk, S3, `RocksDB`)
type L3Store = Arc<RwLock<HashMap<String, (Value, Instant, Duration)>>>;

struct MockL3Cache {
    name: String,
    store: L3Store,
}

impl MockL3Cache {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CacheBackend for MockL3Cache {
    async fn get(&self, key: &str) -> Option<Value> {
        // Simulate latency for "disk" access
        tokio::time::sleep(Duration::from_millis(50)).await;

        let store = self
            .store
            .read()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.get(key).and_then(|(value, expiry, _)| {
            if *expiry > Instant::now() {
                Some(value.clone())
            } else {
                None
            }
        })
    }

    async fn set_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<()> {
        // Simulate latency
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut store = self
            .store
            .write()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        let expiry = Instant::now() + ttl;
        store.insert(key.to_string(), (value, expiry, ttl));
        println!("💾 [{}] Cached '{}' with TTL {:?}", self.name, key, ttl);
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut store = self
            .store
            .write()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.remove(key);
        Ok(())
    }

    async fn health_check(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "MockL3"
    }

    async fn get_with_ttl(&self, key: &str) -> Option<(Value, Option<Duration>)> {
        // Simulate latency
        tokio::time::sleep(Duration::from_millis(50)).await;

        let store = self
            .store
            .read()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.get(key).and_then(|(value, expiry, _)| {
            let now = Instant::now();
            if *expiry > now {
                let remaining = expiry.duration_since(now);
                Some((value.clone(), Some(remaining)))
            } else {
                None
            }
        })
    }
}

// ==================== Main Example ====================

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Multi-Tier Cache: 3-Tier Architecture Example ===\n");

    // 1. Initialize Backends
    let moka_l1: Arc<dyn CacheBackend> = Arc::new(multi_level_cache::backends::MokaCache::new(
        moka::future::Cache::new(10_000),
    )?);
    let redis_l2: Arc<dyn CacheBackend> =
        Arc::new(multi_level_cache::backends::redis::RedisCache::new().await?);
    let l3_backend = Arc::new(MockL3Cache::new("Mock L3 (Disk)"));

    println!("Building 3-tier cache system...");
    let cache = CacheSystemBuilder::new()
        .with_l1(moka_l1)
        .with_l2(redis_l2)
        .with_l3(l3_backend.clone()) // Add L3 tier
        .build()
        .await?;

    println!("✅ Cache system initialized with 3 tiers!");

    // 2. Store data
    println!("\n--- Storing Data ---");
    let data = serde_json::json!({
        "id": "user_123",
        "name": "Alice",
        "role": "premium"
    });

    // When we set data, it goes to ALL tiers
    // L3 will have 2x TTL by default (TierConfig::as_l3())
    println!("Setting 'user:123' with ShortTerm strategy (5 min)...");
    cache
        .cache_manager()
        .set_with_strategy(
            "user:123",
            data.clone(),
            multi_level_cache::CacheStrategy::ShortTerm,
        )
        .await?;

    // 3. Simulate Cache Miss & Promotion
    println!("\n--- Simulating Access Patterns ---");

    // Clear L1 and L2 to force retrieval from L3
    // (In a real scenario, this happens when L1/L2 evict data but L3 keeps it longer)
    // Since we can't easily clear internal default backends, we'll simulate this
    // by using a key that we manually populate in L3 only, or just trust the flow.

    // Let's try a different approach:
    // We'll manually insert into L3 backend to simulate "cold" data
    // But we don't have direct access to the inner L3 backend instance easily here
    // without keeping a reference to `l3_backend` before building.

    // Let's use a new key and pretend it was only in L3
    let cold_data = serde_json::json!({"status": "archived"});

    // We can use the fact that we have `l3_backend` reference!
    // It's wrapped in Arc, so we can still use it.
    // However, `CacheSystem` took ownership of it? No, it took a clone of the Arc.
    // So we can still use our `l3_backend` variable.

    // Manually seed L3 only
    // Note: We need to cast/use the trait methods
    let l3_ref = l3_backend.as_ref();
    l3_ref
        .set_with_ttl("archive:doc1", cold_data, Duration::from_secs(3600))
        .await?;
    println!("(Seeded 'archive:doc1' directly into L3 only)");

    // Now request it via CacheManager
    println!("Requesting 'archive:doc1' (should miss L1/L2, hit L3, and promote)...");

    let start = Instant::now();
    if let Some(val) = cache.cache_manager().get("archive:doc1").await? {
        println!("✅ Found value: {val}");
        println!("   Latency: {:?}", start.elapsed());
    } else {
        println!("❌ Value not found!");
    }

    // Now it should be in L1 (fast access)
    println!("Requesting 'archive:doc1' again (should hit L1)...");
    let start = Instant::now();
    if let Some(val) = cache.cache_manager().get("archive:doc1").await? {
        println!("✅ Found value: {val}");
        println!("   Latency: {:?}", start.elapsed());
    }

    // 4. Statistics
    println!("\n--- Statistics ---");
    let stats = cache.cache_manager().get_stats();
    println!("Total Requests: {}", stats.total_requests);
    println!("L1 Hits: {}", stats.l1_hits);
    println!("L2 Hits: {}", stats.l2_hits);
    println!("Misses: {}", stats.misses);
    println!("Promotions: {}", stats.promotions);

    Ok(())
}
