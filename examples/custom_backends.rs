//! Example: Custom Cache Backends
//!
//! This example demonstrates how to implement custom L1 and L2 cache backends
//! to replace the default Moka (L1) and Redis (L2) implementations.
//!
//! Run with: `cargo run --example custom_backends`

use anyhow::Result;
use multi_tier_cache::{CacheBackend, CacheSystemBuilder, L2CacheBackend, TierConfig, async_trait};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ==================== Example 1: Simple HashMap L1 Cache ====================

/// Simple in-memory cache using `HashMap` and `RwLock`
///
/// This is a basic example showing the minimum required implementation.
/// In production, you might use `DashMap` or other concurrent data structures.
struct HashMapCache {
    name: String,
    store: Arc<RwLock<HashMap<String, (serde_json::Value, Instant)>>>,
}

impl HashMapCache {
    fn new(name: &str) -> Self {
        println!("  🗺️ Initializing {name} with HashMap backend...");
        Self {
            name: name.to_string(),
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn cleanup_expired(&self) {
        let mut store = self
            .store
            .write()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        let now = Instant::now();
        store.retain(|_, (_, expiry)| *expiry > now);
    }
}

#[async_trait]
impl CacheBackend for HashMapCache {
    async fn get(&self, key: &str) -> Option<serde_json::Value> {
        // Clean up expired entries periodically
        if rand::random::<f32>() < 0.1 {
            self.cleanup_expired();
        }

        let store = self
            .store
            .read()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.get(key).and_then(|(value, expiry)| {
            if *expiry > Instant::now() {
                Some(value.clone())
            } else {
                None
            }
        })
    }

    async fn set_with_ttl(&self, key: &str, value: serde_json::Value, ttl: Duration) -> Result<()> {
        let mut store = self
            .store
            .write()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        let expiry = Instant::now() + ttl;
        store.insert(key.to_string(), (value, expiry));
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
        // Simple health check: try to read the lock
        self.store.read().is_ok()
    }

    fn name(&self) -> &'static str {
        "HashMapCache"
    }
}

#[async_trait]
impl L2CacheBackend for HashMapCache {
    async fn get_with_ttl(&self, key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        let store = self
            .store
            .read()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.get(key).and_then(|(value, expiry)| {
            let now = Instant::now();
            if *expiry > now {
                let ttl = expiry.duration_since(now);
                Some((value.clone(), Some(ttl)))
            } else {
                None
            }
        })
    }
}

// ==================== Example 2: In-Memory L2 Cache with TTL ====================

/// In-memory L2 cache that simulates a distributed cache
///
/// This demonstrates implementing `L2CacheBackend` which requires `get_with_ttl()`.
type L2Store = Arc<RwLock<HashMap<String, (serde_json::Value, Instant, Duration)>>>;

struct InMemoryL2Cache {
    store: L2Store,
}

impl InMemoryL2Cache {
    fn new() -> Self {
        println!("  💾 Initializing In-Memory L2 Cache...");
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CacheBackend for InMemoryL2Cache {
    async fn get(&self, key: &str) -> Option<serde_json::Value> {
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

    async fn set_with_ttl(&self, key: &str, value: serde_json::Value, ttl: Duration) -> Result<()> {
        let mut store = self
            .store
            .write()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        let expiry = Instant::now() + ttl;
        store.insert(key.to_string(), (value, expiry, ttl));
        println!("💾 [InMemory L2] Cached '{key}' with TTL {ttl:?}");
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
        self.store.read().is_ok()
    }

    fn name(&self) -> &'static str {
        "InMemoryL2"
    }
}

#[async_trait]
impl L2CacheBackend for InMemoryL2Cache {
    async fn get_with_ttl(&self, key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        let store = self
            .store
            .read()
            .unwrap_or_else(|_| panic!("Lock poisoned"));
        store.get(key).and_then(|(value, expiry, _original_ttl)| {
            let now = Instant::now();
            if *expiry > now {
                // Calculate remaining TTL
                let remaining = expiry.duration_since(now);
                Some((value.clone(), Some(remaining)))
            } else {
                None
            }
        })
    }
}

// ==================== Example 3: No-Op Cache (Testing/Development) ====================

/// No-op cache that doesn't store anything
///
/// Useful for testing or disabling caching in specific environments.
struct NoOpCache {
    name: String,
}

impl NoOpCache {
    fn new(name: &str) -> Self {
        println!("  ⚠️ Initializing No-Op Cache for {name}");
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl CacheBackend for NoOpCache {
    async fn get(&self, _key: &str) -> Option<serde_json::Value> {
        None // Always miss
    }

    async fn set_with_ttl(
        &self,
        key: &str,
        _value: serde_json::Value,
        ttl: Duration,
    ) -> Result<()> {
        println!(
            "💾 [{}] Would cache '{}' with TTL {:?} (no-op)",
            self.name, key, ttl
        );
        Ok(())
    }

    async fn remove(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "NoOp"
    }
}

#[async_trait]
impl L2CacheBackend for NoOpCache {
    async fn get_with_ttl(&self, _key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        None // Always miss
    }
}

// ==================== Main Example ====================

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Multi-Tier Cache: Custom Backends Example ===\n");

    // Example 1: HashMap L1 + InMemory L2
    println!("📦 Example 1: HashMap L1 + InMemory L2");
    println!("─────────────────────────────────────\n");

    let custom_l1 = Arc::new(HashMapCache::new("HashMap L1"));
    let custom_l2 = Arc::new(InMemoryL2Cache::new());

    let cache = CacheSystemBuilder::new()
        .with_tier(custom_l1 as Arc<dyn L2CacheBackend>, TierConfig::as_l1())
        .with_tier(custom_l2 as Arc<dyn L2CacheBackend>, TierConfig::as_l2())
        .build()
        .await?;

    let manager = cache.cache_manager();

    // Test basic operations
    let test_data = serde_json::json!({
        "user": "alice",
        "score": 100,
        "timestamp": 1_234_567_890
    });

    manager
        .set_with_strategy(
            "user:alice",
            test_data.clone(),
            multi_tier_cache::CacheStrategy::ShortTerm,
        )
        .await?;

    if let Some(cached) = manager.get("user:alice").await? {
        println!("✅ Retrieved from cache: {cached}");
    }

    // Example 2: No-Op caches (for testing/development)
    println!("\n📦 Example 2: No-Op Caches (disabled caching)");
    println!("──────────────────────────────────────────────\n");

    let noop_l1 = Arc::new(NoOpCache::new("L1"));
    let noop_l2 = Arc::new(NoOpCache::new("L2"));

    let noop_cache = CacheSystemBuilder::new()
        .with_tier(noop_l1 as Arc<dyn L2CacheBackend>, TierConfig::as_l1())
        .with_tier(noop_l2 as Arc<dyn L2CacheBackend>, TierConfig::as_l2())
        .build()
        .await?;

    let noop_manager = noop_cache.cache_manager();

    noop_manager
        .set_with_strategy(
            "test:key",
            serde_json::json!({"value": "test"}),
            multi_tier_cache::CacheStrategy::Default,
        )
        .await?;

    match noop_manager.get("test:key").await? {
        Some(_) => println!("❌ Unexpected cache hit (no-op should always miss)"),
        None => println!("✅ Cache miss as expected (no-op cache)"),
    }

    // Example 3: Mixed backends (custom L1 + default Redis L2)
    println!("\n📦 Example 3: Custom L1 + Default Redis L2");
    println!("──────────────────────────────────────────────\n");

    let custom_l1_only = Arc::new(HashMapCache::new("HashMap L1"));

    let mixed_cache = CacheSystemBuilder::new()
        .with_tier(
            custom_l1_only as Arc<dyn L2CacheBackend>,
            TierConfig::as_l1(),
        )
        // L2 will use default Redis backend (by default build() adds defaults if empty, but mixing custom tiers with default isn't automatic unless we add it explicitly or build logic handles it.
        // NOTE: The new build() logic ONLY adds defaults if tiers is EMPTY. If we add L1, we MUST add L2 explicitly if we want it.
        // Original example assumed partial configuration. New API requires explicit tiers if not default.
        // So we should add a default Redis L2 tier here to match original behavior.
        .with_tier(
            Arc::new(multi_tier_cache::RedisCache::new().await?),
            TierConfig::as_l2(),
        )
        .build()
        .await?;

    println!("✅ Mixed backend cache system initialized");
    println!("   L1: Custom HashMap");
    println!("   L2: Default Redis");

    // Get statistics
    let stats = mixed_cache.cache_manager().get_stats();
    println!("\n📊 Cache Statistics:");
    println!("   Total requests: {}", stats.total_requests);
    println!("   L1 hits: {}", stats.l1_hits);
    println!("   L2 hits: {}", stats.l2_hits);
    println!("   Misses: {}", stats.misses);
    println!("   Hit rate: {:.2}%", stats.hit_rate);

    println!("\n✅ Custom backends example completed!");

    // Example 4: Custom Tier Configuration (v0.5.0+)
    println!("\n📦 Example 4: Custom Tier Configuration (using with_tier)");
    println!("──────────────────────────────────────────────────────\n");

    // Create a custom L2 backend
    let custom_l2_tier = Arc::new(InMemoryL2Cache::new());

    // Configure it as Tier 2 with custom settings
    let tier_config = TierConfig::as_l2().with_promotion(true).with_ttl_scale(1.5); // 1.5x TTL scaling

    let tiered_cache = CacheSystemBuilder::new()
        // We can mix default L1 with custom L2 tier
        .with_tier(
            Arc::new(multi_tier_cache::MokaCache::new(
                multi_tier_cache::MokaCacheConfig::default(),
            )?),
            TierConfig::as_l1(),
        )
        .with_tier(custom_l2_tier, tier_config)
        .build()
        .await?;

    println!("✅ Tiered cache system initialized");

    tiered_cache
        .cache_manager()
        .set_with_strategy(
            "tiered:key",
            serde_json::json!("value"),
            multi_tier_cache::CacheStrategy::ShortTerm,
        )
        .await?;

    println!("   Stored 'tiered:key' with 1.5x TTL in L2");

    println!("\n✅ Custom backends example completed!");

    Ok(())
}
