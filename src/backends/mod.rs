//! Cache Backend Implementations
//!
//! This module contains all cache backend implementations for the multi-tier cache system.
//!
//! # Available Backends
//!
//! ## In-Memory (L1 Tier)
//! - **Moka** - High-performance concurrent cache with automatic eviction (default L1)
//! - **`DashMap`** - Simple concurrent HashMap-based cache
//! - **`QuickCache`** - Lightweight, optimized for maximum performance (feature: `quickcache`)
//!
//! ## Distributed (L2 Tier)
//! - **Redis** - Industry-standard distributed cache with persistence (default L2)
//! - **Memcached** - Lightweight distributed cache (feature: `memcached`)
//!
//! ## On-Disk (L3/L4 Tier)
//! - **`RocksDB`** - Embedded persistent key-value store (coming soon)
//!
//! # Usage
//!
//! ```rust,no_run
//! use multi_tier_cache::backends::{MokaCache, RedisCache};
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Explicit backend selection
//! let moka = MokaCache::new()?;
//! let redis = RedisCache::new().await?;
//!
//! // Or use type aliases for backward compatibility
//! use multi_tier_cache::backends::{L1Cache, L2Cache};
//! let l1 = L1Cache::new()?;  // MokaCache
//! let l2 = L2Cache::new().await?;  // RedisCache
//! # Ok(())
//! # }
//! ```

// Core backends (always available)
pub mod dashmap_cache;
pub mod moka_cache;

// Optional backends (feature-gated)
#[cfg(feature = "redis")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
pub mod redis;

#[cfg(feature = "memcached")]
#[cfg_attr(docsrs, doc(cfg(feature = "memcached")))]
pub mod memcached_cache;

#[cfg(feature = "quickcache")]
pub mod quickcache_cache;

// Re-export backend types
pub use dashmap_cache::DashMapCache;
pub use moka_cache::MokaCache;

#[cfg(feature = "memcached")]
pub use memcached_cache::MemcachedCache;

#[cfg(feature = "quickcache")]
pub use quickcache_cache::QuickCacheBackend;

// Future backends will be added here with conditional compilation
// based on feature flags:

// #[cfg(feature = "quickcache")]
// pub mod quickcache_cache;
// #[cfg(feature = "quickcache")]
// pub use quickcache_cache::QuickCacheBackend;

// #[cfg(feature = "backend-dashmap")]
// pub mod dashmap_cache;
// #[cfg(feature = "backend-dashmap")]
// pub use dashmap_cache::DashMapCache;

// #[cfg(feature = "backend-rocksdb")]
// pub mod rocksdb_cache;
// #[cfg(feature = "backend-rocksdb")]
// pub use rocksdb_cache::RocksDBCache;
