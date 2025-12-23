//! Multi-Tier Cache
//!
//! A high-performance, production-ready multi-tier caching library for Rust featuring:
//! - **L1 Cache**: In-memory caching with Moka (sub-millisecond latency)
//! - **L2 Cache**: Distributed caching with Redis (persistent storage)
//! - **Cache Stampede Protection**: `DashMap` + Mutex request coalescing
//! - **Redis Streams**: Built-in support for event streaming
//! - **Automatic L2-to-L1 Promotion**: Intelligent cache tier promotion
//! - **Comprehensive Statistics**: Hit rates, promotions, in-flight tracking
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use multi_level_cache::{CacheSystem, CacheStrategy};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize cache system
//!     let cache = CacheSystem::new().await?;
//!
//!     // Store data with cache strategy
//!     let data = serde_json::json!({"user": "alice", "score": 100});
//!     cache.cache_manager()
//!         .set_with_strategy("user:1", data, CacheStrategy::ShortTerm)
//!         .await?;
//!
//!     // Retrieve data (L1 first, then L2 fallback)
//!     if let Some(cached) = cache.cache_manager().get("user:1").await? {
//!         tracing::info!("Cached data: {}", cached);
//!     }
//!
//!     // Get statistics
//!     let stats = cache.cache_manager().get_stats();
//!     tracing::info!("Hit rate: {:.2}%", stats.hit_rate);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! - **Multi-Tier Architecture**: Combines fast in-memory (L1) with persistent distributed (L2) caching
//! - **Cache Stampede Protection**: Prevents duplicate computations during cache misses
//! - **Redis Streams**: Publish/subscribe with automatic trimming
//! - **Zero-Config**: Sensible defaults, works out of the box
//! - **Production-Proven**: Battle-tested at 16,829+ RPS with 5.2ms latency
//!
//! # Architecture
//!
//! ```text
//! Request → L1 Cache (Moka) → L2 Cache (Redis) → Compute/Fetch
//!           ↓ Hit (90%)       ↓ Hit (75%)        ↓ Miss (5%)
//!           Return            Promote to L1       Store in L1+L2
//! ```

use std::sync::Arc;

pub mod backends;
pub mod builder;
pub mod cache_manager;
pub mod invalidation;
pub mod stats;
pub mod traits;

// Re-export backend types (maintains backward compatibility)
pub use backends::{DashMapCache, MokaCache};

// Optional backends (feature-gated)
#[cfg(feature = "memcached")]
pub use backends::MemcachedCache;

#[cfg(feature = "quickcache")]
pub use backends::QuickCacheBackend;
pub use builder::CacheSystemBuilder;
pub use cache_manager::{
    CacheLevel,
    CacheManager,
    CacheManagerStats,
    CacheStrategy,
    // Multi-tier support (v0.5.0+)
    TierConfig,
    TierStats,
};
pub use traits::{CacheBackend, InvalidationPublisher, InvalidationSubscriber, StreamingBackend};

/// Re-export of `async_trait` for convenience.
pub use async_trait::async_trait;

/// Main entry point for the Multi-Tier Cache system
///
/// Provides unified access to L1 (Moka) and L2 (Redis) caches with
/// automatic failover, promotion, and stampede protection.
///
/// # Example
///
/// ```rust,no_run
/// use multi_level_cache::CacheSystem;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let cache = CacheSystem::new().await?;
///
///     // Use cache_manager for all operations
///     let manager = cache.cache_manager();
///
///     Ok(())
/// }
/// ```
///
/// # Note on `l1_cache` and `l2_cache` Fields
///
/// When using multi-tier mode or custom backends, `l1_cache` and `l2_cache`
/// may be `None`. Always use `cache_manager()` for cache operations.
#[derive(Clone)]
pub struct CacheSystem {
    /// Unified cache manager (primary interface)
    pub cache_manager: Arc<CacheManager>,
}

impl CacheSystem {
    /// Perform health check on all cache tiers
    ///
    /// Returns `true` if at least L1 is operational.
    /// L2 failure is tolerated (graceful degradation).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use multi_level_cache::CacheSystem;
    ///
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()> {
    ///     let cache = CacheSystem::new().await?;
    ///
    ///     if cache.health_check().await {
    ///         tracing::info!("Cache system healthy");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn health_check(&self) -> bool {
        self.cache_manager.health_check().await
    }

    /// Get reference to cache manager (primary interface)
    ///
    /// Use this for all cache operations: get, set, streams, etc.
    #[must_use]
    pub fn cache_manager(&self) -> &Arc<CacheManager> {
        &self.cache_manager
    }
}
