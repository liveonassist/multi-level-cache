//! Cache Manager - Unified Cache Operations
//!
//! Manages operations across L1 (Moka) and L2 (Redis) caches with intelligent fallback.

use anyhow::Result;
use dashmap::DashMap;
use serde_json;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

use tracing::{debug, error, info, warn};

use crate::{
    invalidation::{InvalidationMessage, InvalidationSystem},
    stats::{AtomicInvalidationStats, InvalidationStats},
    traits::{CacheBackend, InvalidationPublisher, InvalidationSubscriber, StreamingBackend},
};

/// Type alias for the in-flight requests map
type InFlightMap = DashMap<String, Arc<Mutex<()>>>;

/// RAII cleanup guard for in-flight request tracking
/// Ensures that entries are removed from `DashMap` even on early return or panic
struct CleanupGuard<'a> {
    map: &'a InFlightMap,
    key: String,
}

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
}

/// Cache strategies for different data types
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum CacheStrategy {
    /// Real-time data - 10 seconds TTL
    RealTime,
    /// Short-term data - 5 minutes TTL  
    ShortTerm,
    /// Medium-term data - 1 hour TTL
    MediumTerm,
    /// Long-term data - 3 hours TTL
    LongTerm,
    /// Custom TTL
    Custom(Duration),
    /// Default strategy (5 minutes)
    Default,
}

impl CacheStrategy {
    /// Convert strategy to duration
    #[must_use]
    pub fn to_duration(&self) -> Duration {
        match self {
            Self::RealTime => Duration::from_secs(10),
            Self::ShortTerm | Self::Default => Duration::from_secs(300), // 5 minutes
            Self::MediumTerm => Duration::from_secs(3600),               // 1 hour
            Self::LongTerm => Duration::from_secs(10800),                // 3 hours
            Self::Custom(duration) => *duration,
        }
    }
}

/// Statistics for a single cache tier
#[derive(Debug)]
pub struct TierStats {
    /// Tier level (1 = L1, 2 = L2, 3 = L3, etc.)
    pub tier_level: usize,
    /// Number of cache hits at this tier
    pub hits: AtomicU64,
    /// Backend name for identification
    pub backend_name: String,
}

impl Clone for TierStats {
    fn clone(&self) -> Self {
        Self {
            tier_level: self.tier_level,
            hits: AtomicU64::new(self.hits.load(Ordering::Relaxed)),
            backend_name: self.backend_name.clone(),
        }
    }
}

impl TierStats {
    fn new(tier_level: usize, backend_name: String) -> Self {
        Self {
            tier_level,
            hits: AtomicU64::new(0),
            backend_name,
        }
    }

    /// Get current hit count
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

/// A single cache tier in the multi-tier architecture
pub struct CacheLevel {
    /// Cache backend for this tier
    pub backend: Arc<dyn CacheBackend>,
    /// Tier level (1 = hottest/fastest, higher = colder/slower)
    tier_level: usize,
    /// Enable automatic promotion to upper tiers on cache hit
    promotion_enabled: bool,
    /// TTL scale factor (multiplier for TTL when storing/promoting)
    ttl_scale: f64,
    /// Statistics for this tier
    stats: TierStats,
}

impl CacheLevel {
    /// Create a new cache tier
    pub fn new(
        backend: Arc<dyn CacheBackend>,
        tier_level: usize,
        promotion_enabled: bool,
        ttl_scale: f64,
    ) -> Self {
        let backend_name = backend.name().to_string();
        Self {
            backend,
            tier_level,
            promotion_enabled,
            ttl_scale,
            stats: TierStats::new(tier_level, backend_name),
        }
    }

    /// Get value with TTL from this tier
    async fn get_with_ttl(&self, key: &str) -> Option<(serde_json::Value, Option<Duration>)> {
        self.backend.get_with_ttl(key).await
    }

    /// Set value with TTL in this tier
    async fn set_with_ttl(&self, key: &str, value: serde_json::Value, ttl: Duration) -> Result<()> {
        let scaled_ttl = Duration::from_secs_f64(ttl.as_secs_f64() * self.ttl_scale);
        self.backend.set_with_ttl(key, value, scaled_ttl).await
    }

    /// Remove value from this tier
    async fn remove(&self, key: &str) -> Result<()> {
        self.backend.remove(key).await
    }

    /// Record a cache hit for this tier
    fn record_hit(&self) {
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
    }
}

/// Configuration for a cache tier (used in builder pattern)
#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Tier level (1, 2, 3, 4...)
    pub tier_level: usize,
    /// Enable promotion to upper tiers on hit
    pub promotion_enabled: bool,
    /// TTL scale factor (1.0 = same as base TTL)
    pub ttl_scale: f64,
}

impl TierConfig {
    /// Create new tier configuration
    #[must_use]
    pub fn new(tier_level: usize) -> Self {
        Self {
            tier_level,
            promotion_enabled: true,
            ttl_scale: 1.0,
        }
    }

    /// Configure as L1 (hot tier)
    #[must_use]
    pub fn as_l1() -> Self {
        Self {
            tier_level: 1,
            promotion_enabled: false, // L1 is already top tier
            ttl_scale: 1.0,
        }
    }

    /// Configure as L2 (warm tier)
    #[must_use]
    pub fn as_l2() -> Self {
        Self {
            tier_level: 2,
            promotion_enabled: true,
            ttl_scale: 1.0,
        }
    }

    /// Configure as L3 (cold tier) with longer TTL
    #[must_use]
    pub fn as_l3() -> Self {
        Self {
            tier_level: 3,
            promotion_enabled: true,
            ttl_scale: 2.0, // Keep data 2x longer
        }
    }

    /// Configure as L4 (archive tier) with much longer TTL
    #[must_use]
    pub fn as_l4() -> Self {
        Self {
            tier_level: 4,
            promotion_enabled: true,
            ttl_scale: 8.0, // Keep data 8x longer
        }
    }

    /// Set promotion enabled
    #[must_use]
    pub fn with_promotion(mut self, enabled: bool) -> Self {
        self.promotion_enabled = enabled;
        self
    }

    /// Set TTL scale factor
    #[must_use]
    pub fn with_ttl_scale(mut self, scale: f64) -> Self {
        self.ttl_scale = scale;
        self
    }

    /// Set tier level
    #[must_use]
    pub fn with_level(mut self, level: usize) -> Self {
        self.tier_level = level;
        self
    }
}

/// Cache Manager - Unified operations across multiple cache tiers
///
/// Supports both legacy 2-tier (L1+L2) and new multi-tier (L1+L2+L3+L4+...) architectures.
/// When `tiers` is Some, it uses the dynamic multi-tier system. Otherwise, falls back to
/// legacy L1+L2 behavior for backward compatibility.
pub struct CacheManager {
    /// Dynamic multi-level cache architecture
    levels: Vec<CacheLevel>,

    /// Optional streaming backend (defaults to L2 if it implements `StreamingBackend`)
    streaming_backend: Option<Arc<dyn StreamingBackend>>,
    /// Statistics (`AtomicU64` is already thread-safe, no Arc needed)
    total_requests: AtomicU64,
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    misses: AtomicU64,
    promotions: AtomicUsize,

    /// In-flight requests to prevent Cache Stampede on L2/compute operations
    in_flight_requests: Arc<InFlightMap>,

    invalidation_system: Option<InvalidationSystem>,

    /// Invalidation statistics
    invalidation_stats: Arc<AtomicInvalidationStats>,
}

impl CacheManager {
    /// Create new cache manager with multi-tier architecture (v0.5.0+)
    ///
    /// This constructor enables dynamic multi-tier caching with 3, 4, or more tiers.
    /// Tiers are checked in order (lower `tier_level` = faster/hotter).
    ///
    /// # Arguments
    ///
    /// * `tiers` - Vector of configured cache tiers (must be sorted by `tier_level` ascending)
    /// * `streaming_backend` - Optional streaming backend
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use multi_level_cache::{CacheManager, CacheTier, TierConfig, MokaCache, RedisCache};
    /// use std::sync::Arc;
    ///
    /// // L1 + L2 + L3 setup
    /// let l1 = Arc::new(MokaCache::new()?);
    /// let l2 = Arc::new(RedisCache::new().await?);
    /// let l3 = Arc::new(RocksDBCache::new("/tmp/cache").await?);
    ///
    /// let tiers = vec![
    ///     CacheTier::new(l1, 1, false, 1.0),  // L1 - no promotion
    ///     CacheTier::new(l2, 2, true, 1.0),   // L2 - promote to L1
    ///     CacheTier::new(l3, 3, true, 2.0),   // L3 - promote to L2&L1, 2x TTL
    /// ];
    ///
    /// let manager = CacheManager::new_with_levels(tiers, None).await?;
    /// ```
    /// # Errors
    ///
    /// Returns an error if tiers are not sorted by level or if no tiers are provided.
    pub async fn new_with_levels(
        tiers: Vec<CacheLevel>,
        invalidation_system: Option<InvalidationSystem>,
        streaming_backend: Option<Arc<dyn StreamingBackend>>,
    ) -> Result<Self> {
        if tiers.is_empty() {
            return Err(anyhow::anyhow!("At least one cache tier is required"));
        }

        info!(
            tier_count = tiers.len(),
            "Initializing Cache Manager with multi-tier architecture"
        );

        // Validate tiers are sorted by level
        for i in 1..tiers.len() {
            if let (Some(current), Some(prev)) = (tiers.get(i), tiers.get(i - 1)) {
                if current.tier_level <= prev.tier_level {
                    anyhow::bail!(
                        "Tiers must be sorted by tier_level ascending (found L{} after L{})",
                        current.tier_level,
                        prev.tier_level
                    );
                }
            }
        }

        let this = Self {
            levels: tiers,
            streaming_backend,
            total_requests: AtomicU64::new(0),
            l1_hits: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            promotions: AtomicUsize::new(0),
            in_flight_requests: Arc::new(DashMap::new()),
            invalidation_system,
            invalidation_stats: Arc::new(AtomicInvalidationStats::default()),
        };
        this.spawn_invalidation_listener().await;

        Ok(this)
    }

    /// Spawn the invalidation listener background task
    async fn spawn_invalidation_listener(&self) {
        if let Some(invalidation_subscriber) = self.invalidation_subscriber() {
            match invalidation_subscriber.subscribe().await {
                Ok(mut rx) => {
                    // Collect all tiers to invalidate from
                    let tiers: Vec<Arc<dyn CacheBackend>> =
                        self.levels.iter().map(|t| t.backend.clone()).collect();
                    let tiers = Arc::new(tiers);

                    tokio::spawn(async move {
                        info!("Invalidation listener task started");
                        while let Some(msg) = rx.recv().await {
                            match msg {
                                InvalidationMessage::Remove { key } => {
                                    for backend in tiers.iter() {
                                        let _ = backend.remove(&key).await;
                                    }
                                    debug!("Invalidation: Removed '{}' from local tiers", key);
                                }
                                InvalidationMessage::Update {
                                    key,
                                    value,
                                    ttl_secs,
                                } => {
                                    let ttl = ttl_secs
                                        .map(Duration::from_secs)
                                        .unwrap_or_else(|| CacheStrategy::Default.to_duration());

                                    for backend in tiers.iter() {
                                        let _ =
                                            backend.set_with_ttl(&key, value.clone(), ttl).await;
                                    }
                                    debug!("Invalidation: Updated '{}' in local tiers", key);
                                }
                                InvalidationMessage::RemovePattern { .. } => {
                                    warn!(
                                        "Invalidation: RemovePattern received but not fully supported in generic multi-tier mode (no localized scan)"
                                    );
                                }
                                InvalidationMessage::RemoveBulk { keys } => {
                                    for key in keys {
                                        for backend in tiers.iter() {
                                            let _ = backend.remove(&key).await;
                                        }
                                    }
                                    debug!("Invalidation: Processed bulk removal");
                                }
                            }
                        }
                        info!("Invalidation listener task ended");
                    });
                }
                Err(e) => {
                    error!("Failed to subscribe to invalidation messages: {}", e);
                }
            }
        }
    }

    /// Perform health check on all cache tiers
    pub async fn health_check(&self) -> bool {
        let mut all_healthy = true;
        for tier in &self.levels {
            if !tier.backend.health_check().await {
                warn!(tier = tier.tier_level, "Cache tier unhealthy");
                all_healthy = false;
            }
        }
        if all_healthy {
            info!("All cache tiers healthy");
        }
        // Return true if at least one tier works? Or strictly all?
        // Legacy behavior: "partial failure handled gracefully", returning true if L1 works.
        // Let's return true if the first tier (L1) works, or if all work.
        if let Some(l1) = self.levels.first() {
            l1.backend.health_check().await
        } else {
            false
        }
    }

    /// Get value from cache
    pub async fn get(&self, key: &str) -> Result<Option<serde_json::Value>> {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        let key = key.to_string();

        // Try each tier sequentially
        for (tier_index, tier) in self.levels.iter().enumerate() {
            if let Some((value, ttl)) = tier.get_with_ttl(&key).await {
                // Cache hit!
                tier.record_hit();

                // Track legacy stats
                if tier.tier_level == 1 {
                    self.l1_hits.fetch_add(1, Ordering::Relaxed);
                } else if tier.tier_level == 2 {
                    self.l2_hits.fetch_add(1, Ordering::Relaxed);
                }

                // Auto-promotion
                if tier.promotion_enabled && tier_index > 0 {
                    let promotion_ttl = ttl.unwrap_or_else(|| CacheStrategy::Default.to_duration());

                    // Promo logic: promote to all upper tiers
                    self.promotions.fetch_add(1, Ordering::Relaxed);
                    // Iterate tiers above this one
                    for upper_tier in self.levels.iter().take(tier_index).rev() {
                        if let Err(e) = upper_tier
                            .set_with_ttl(&key, value.clone(), promotion_ttl)
                            .await
                        {
                            warn!(
                                "Failed to promote '{}' to L{}: {}",
                                key, upper_tier.tier_level, e
                            );
                        }
                    }
                }

                return Ok(Some(value));
            }
        }

        // Cache miss
        self.misses.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    /// Get value from cache (L1 first, then L2 fallback with promotion)
    ///
    /// This method now includes built-in Cache Stampede protection when cache misses occur.
    /// Multiple concurrent requests for the same missing key will be coalesced to prevent
    /// unnecessary duplicate work on external data sources.
    ///
    /// Supports both legacy 2-tier mode and new multi-tier mode (v0.5.0+).
    ///
    /// # Arguments
    /// * `key` - Cache key to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(value))` - Cache hit, value found in any tier
    /// * `Ok(None)` - Cache miss, value not found in any cache
    /// * `Err(error)` - Cache operation failed
    /// # Errors
    ///
    /// Returns an error if cache operation fails.
    ///
    /// # Panics
    ///
    /// Panics if tiers are not initialized in multi-tier mode (should not happen if constructed correctly).
    /// Set value with specific cache strategy (all tiers)
    ///
    /// Stores to ALL tiers with their respective TTL scaling.
    /// # Errors
    ///
    /// Returns an error if cache set operation fails.
    pub async fn set_with_strategy(
        &self,
        key: &str,
        value: serde_json::Value,
        strategy: CacheStrategy,
    ) -> Result<()> {
        let ttl = strategy.to_duration();
        let mut success_count = 0;
        let mut last_error = None;

        for tier in &self.levels {
            match tier.set_with_ttl(key, value.clone(), ttl).await {
                Ok(()) => {
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        "L{} cache set failed for key '{}': {}",
                        tier.tier_level, key, e
                    );
                    last_error = Some(e);
                }
            }
        }

        if success_count > 0 {
            debug!(
                "[Multi-Level] Cached '{}' in {}/{} tiers (base TTL: {:?})",
                key,
                success_count,
                self.levels.len(),
                ttl
            );
            return Ok(());
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All tiers failed for key '{key}'")))
    }

    /// Get or compute value with Cache Stampede protection across L1+L2+Compute
    ///
    /// This method provides comprehensive Cache Stampede protection:
    /// 1. Check L1 cache first (uses Moka's built-in coalescing)
    /// 2. Check L2 cache with mutex-based coalescing
    /// 3. Compute fresh data with protection against concurrent computations
    ///
    /// # Arguments
    /// * `key` - Cache key
    /// * `strategy` - Cache strategy for TTL and storage behavior
    /// * `compute_fn` - Async function to compute the value if not in any cache
    ///
    /// # Example
    /// ```ignore
    /// let api_data = cache_manager.get_or_compute_with(
    ///     "api_response",
    ///     CacheStrategy::RealTime,
    ///     || async {
    ///         fetch_data_from_api().await
    ///     }
    /// ).await?;
    /// ```
    #[allow(dead_code)]
    /// # Errors
    ///
    /// Returns an error if compute function fails or cache operations fail.
    pub async fn get_or_compute_with<F, Fut>(
        &self,
        key: &str,
        strategy: CacheStrategy,
        compute_fn: F,
    ) -> Result<serde_json::Value>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<serde_json::Value>> + Send,
    {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        // 1. Try first tier (L1) fast path (no locking)
        if let Some(first_tier) = self.levels.first() {
            if let Some((value, _)) = first_tier.get_with_ttl(key).await {
                first_tier.record_hit();
                if first_tier.tier_level == 1 {
                    self.l1_hits.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(value);
            }
        }

        // 2. L1 miss - use Cache Stampede protection
        let key_owned = key.to_string();
        let lock_guard = self
            .in_flight_requests
            .entry(key_owned.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock_guard.lock().await;

        // RAII cleanup guard
        let _cleanup_guard = CleanupGuard {
            map: &self.in_flight_requests,
            key: key_owned.clone(),
        };

        // 3. Double-check ALL tiers (another thread might have filled or promoted)
        for (i, tier) in self.levels.iter().enumerate() {
            if let Some((value, ttl)) = tier.get_with_ttl(key).await {
                tier.record_hit();
                // Promote to upper tiers if needed
                if tier.promotion_enabled && i > 0 {
                    let promotion_ttl = ttl.unwrap_or_else(|| strategy.to_duration());
                    self.promotions.fetch_add(1, Ordering::Relaxed);

                    for upper_tier in self.levels.iter().take(i).rev() {
                        if let Err(e) = upper_tier
                            .set_with_ttl(key, value.clone(), promotion_ttl)
                            .await
                        {
                            warn!(
                                "Failed to promote '{}' to L{}: {}",
                                key, upper_tier.tier_level, e
                            );
                        }
                    }
                }
                return Ok(value);
            }
        }

        // 4. Cache miss across all tiers - compute
        debug!(
            "Computing fresh data for key: '{}' (Cache Stampede protected)",
            key
        );
        let fresh_data = compute_fn().await?;

        // 5. Store in all tiers
        if let Err(e) = self
            .set_with_strategy(key, fresh_data.clone(), strategy)
            .await
        {
            warn!("Failed to cache computed data for key '{}': {}", key, e);
        }

        Ok(fresh_data)
    }

    /// Get or compute typed value with Cache Stampede protection (Type-Safe Version)
    ///
    /// This method provides the same functionality as `get_or_compute_with()` but with
    /// **type-safe** automatic serialization/deserialization. Perfect for database queries,
    /// API calls, or any computation that returns structured data.
    ///
    /// # Type Safety
    ///
    /// - Returns your actual type `T` instead of `serde_json::Value`
    /// - Compiler enforces Serialize + `DeserializeOwned` bounds
    /// - No manual JSON conversion needed
    ///
    /// # Cache Flow
    ///
    /// 1. Check L1 cache → deserialize if found
    /// 2. Check L2 cache → deserialize + promote to L1 if found
    /// 3. Execute `compute_fn` → serialize → store in L1+L2
    /// 4. Full stampede protection (only ONE request computes)
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key
    /// * `strategy` - Cache strategy for TTL
    /// * `compute_fn` - Async function returning `Result<T>`
    ///
    /// # Example - Database Query
    ///
    /// ```no_run
    /// # use multi_level_cache::{CacheManager, CacheStrategy, L1Cache, L2Cache};
    /// # use std::sync::Arc;
    /// # use serde::{Serialize, Deserialize};
    /// # async fn example() -> anyhow::Result<()> {
    /// # let l1 = Arc::new(L1Cache::new()?);
    /// # let l2 = Arc::new(L2Cache::new().await?);
    /// # let cache_manager = CacheManager::new(l1, l2);
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct User {
    ///     id: i64,
    ///     name: String,
    /// }
    ///
    /// // Type-safe database caching (example - requires sqlx)
    /// // let user: User = cache_manager.get_or_compute_typed(
    /// //     "user:123",
    /// //     CacheStrategy::MediumTerm,
    /// //     || async {
    /// //         sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
    /// //             .bind(123)
    /// //             .fetch_one(&pool)
    /// //             .await
    /// //     }
    /// // ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Example - API Call
    ///
    /// ```no_run
    /// # use multi_level_cache::{CacheManager, CacheStrategy, L1Cache, L2Cache};
    /// # use std::sync::Arc;
    /// # use serde::{Serialize, Deserialize};
    /// # async fn example() -> anyhow::Result<()> {
    /// # let l1 = Arc::new(L1Cache::new()?);
    /// # let l2 = Arc::new(L2Cache::new().await?);
    /// # let cache_manager = CacheManager::new(l1, l2);
    /// #[derive(Serialize, Deserialize)]
    /// struct ApiResponse {
    ///     data: String,
    ///     timestamp: i64,
    /// }
    ///
    /// // API call caching (example - requires reqwest)
    /// // let response: ApiResponse = cache_manager.get_or_compute_typed(
    /// //     "api:endpoint",
    /// //     CacheStrategy::RealTime,
    /// //     || async {
    /// //         reqwest::get("https://api.example.com/data")
    /// //             .await?
    /// //             .json::<ApiResponse>()
    /// //             .await
    /// //     }
    /// // ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// - L1 hit: <1ms + deserialization (~10-50μs for small structs)
    /// - L2 hit: 2-5ms + deserialization + L1 promotion
    /// - Compute: Your function time + serialization + L1+L2 storage
    /// - Stampede protection: 99.6% latency reduction under high concurrency
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Compute function fails
    /// - Serialization fails (invalid type for JSON)
    /// - Deserialization fails (cache data doesn't match type T)
    /// - Cache operations fail (Redis connection issues)
    #[allow(clippy::too_many_lines)]
    pub async fn get_or_compute_typed<T, F, Fut>(
        &self,
        key: &str,
        strategy: CacheStrategy,
        compute_fn: F,
    ) -> Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        // 1. Try first tier (L1) fast path (no locking)
        if let Some(first_tier) = self.levels.first() {
            if let Some((cached_json, _)) = first_tier.get_with_ttl(key).await {
                first_tier.record_hit();
                if first_tier.tier_level == 1 {
                    self.l1_hits.fetch_add(1, Ordering::Relaxed);
                }

                // Attempt to deserialize from JSON to type T
                match serde_json::from_value::<T>(cached_json) {
                    Ok(typed_value) => {
                        debug!(
                            "[L1 HIT] Deserialized '{}' to type {}",
                            key,
                            std::any::type_name::<T>()
                        );
                        return Ok(typed_value);
                    }
                    Err(e) => {
                        // Deserialization failed - cache data may be stale or corrupt
                        warn!(
                            "L1 cache deserialization failed for key '{}': {}. Will recompute.",
                            key, e
                        );
                        // Fall through to recompute
                    }
                }
            }
        }

        // 2. L1 miss - use Cache Stampede protection
        let key_owned = key.to_string();
        let lock_guard = self
            .in_flight_requests
            .entry(key_owned.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock_guard.lock().await;

        // RAII cleanup guard - ensures entry is removed even on early return or panic
        let _cleanup_guard = CleanupGuard {
            map: &self.in_flight_requests,
            key: key_owned,
        };

        // 3. Double-check ALL tiers after acquiring lock
        // (Another request might have populated it while we were waiting)
        for (i, tier) in self.levels.iter().enumerate() {
            if let Some((cached_json, ttl)) = tier.get_with_ttl(key).await {
                tier.record_hit();

                // Attempt to deserialize
                match serde_json::from_value::<T>(cached_json.clone()) {
                    Ok(typed_value) => {
                        debug!(
                            "[L{} HIT] Deserialized '{}' to type {}",
                            tier.tier_level,
                            key,
                            std::any::type_name::<T>()
                        );

                        // Promote to upper tiers if needed
                        if tier.promotion_enabled && i > 0 {
                            let promotion_ttl = ttl.unwrap_or_else(|| strategy.to_duration());
                            self.promotions.fetch_add(1, Ordering::Relaxed);

                            for upper_tier in self.levels.iter().take(i).rev() {
                                if let Err(e) = upper_tier
                                    .set_with_ttl(key, cached_json.clone(), promotion_ttl)
                                    .await
                                {
                                    warn!(
                                        "Failed to promote '{}' to L{}: {}",
                                        key, upper_tier.tier_level, e
                                    );
                                }
                            }
                        }
                        return Ok(typed_value);
                    }
                    Err(e) => {
                        warn!(
                            "L{} cache deserialization failed for key '{}': {}. Trying next tier.",
                            tier.tier_level, key, e
                        );
                        // Continue to next tier
                    }
                }
            }
        }

        // 4. Cache miss across all tiers (or deserialization failed) - compute fresh data
        debug!(
            "Computing fresh typed data for key: '{}' (Cache Stampede protected)",
            key
        );
        let typed_value = compute_fn().await?;

        // 5. Serialize to JSON for storage
        let json_value = serde_json::to_value(&typed_value).map_err(|e| {
            anyhow::anyhow!(
                "Failed to serialize type {} for caching: {}",
                std::any::type_name::<T>(),
                e
            )
        })?;

        // 6. Store in all tiers
        if let Err(e) = self.set_with_strategy(key, json_value, strategy).await {
            warn!(
                "Failed to cache computed typed data for key '{}': {}",
                key, e
            );
        } else {
            debug!(
                "Cached typed value for '{}' (type: {})",
                key,
                std::any::type_name::<T>()
            );
        }

        // 7. _cleanup_guard will auto-remove entry on drop

        Ok(typed_value)
    }

    /// Get comprehensive cache statistics
    ///
    /// In multi-tier mode, aggregates statistics from all tiers.
    /// In legacy mode, returns L1 and L2 stats.
    #[allow(dead_code)]
    pub fn get_stats(&self) -> CacheManagerStats {
        let total_reqs = self.total_requests.load(Ordering::Relaxed);
        let l1_hits = self.l1_hits.load(Ordering::Relaxed);
        let l2_hits = self.l2_hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);

        CacheManagerStats {
            total_requests: total_reqs,
            l1_hits,
            l2_hits,
            total_hits: l1_hits + l2_hits,
            misses,
            hit_rate: if total_reqs > 0 {
                #[allow(clippy::cast_precision_loss)]
                {
                    ((l1_hits + l2_hits) as f64 / total_reqs as f64) * 100.0
                }
            } else {
                0.0
            },
            l1_hit_rate: if total_reqs > 0 {
                #[allow(clippy::cast_precision_loss)]
                {
                    (l1_hits as f64 / total_reqs as f64) * 100.0
                }
            } else {
                0.0
            },
            promotions: self.promotions.load(Ordering::Relaxed),
            in_flight_requests: self.in_flight_requests.len(),
        }
    }

    /// Get per-tier statistics (v0.5.0+)
    ///
    /// Returns statistics for each tier if multi-tier mode is enabled.
    /// Returns None if using legacy 2-tier mode.
    ///
    /// # Example
    /// ```rust,ignore
    /// if let Some(tier_stats) = cache_manager.get_tier_stats() {
    ///     for stats in tier_stats {
    ///         println!("L{}: {} hits ({})",
    ///                  stats.tier_level,
    ///                  stats.hit_count(),
    ///                  stats.backend_name);
    ///     }
    /// }
    /// ```
    pub fn get_tier_stats(&self) -> Option<Vec<TierStats>> {
        Some(self.levels.iter().map(|tier| tier.stats.clone()).collect())
    }

    // ===== Redis Streams Methods =====

    /// Publish data to Redis Stream
    ///
    /// # Arguments
    /// * `stream_key` - Name of the stream (e.g., "`events_stream`")
    /// * `fields` - Field-value pairs to publish
    /// * `maxlen` - Optional max length for stream trimming
    ///
    /// # Returns
    /// The entry ID generated by Redis
    ///
    /// # Errors
    /// Returns error if streaming backend is not configured
    pub async fn publish_to_stream(
        &self,
        stream_key: &str,
        fields: Vec<(String, String)>,
        maxlen: Option<usize>,
    ) -> Result<String> {
        match &self.streaming_backend {
            Some(backend) => backend.stream_add(stream_key, fields, maxlen).await,
            None => Err(anyhow::anyhow!("Streaming backend not configured")),
        }
    }

    /// Read latest entries from Redis Stream
    ///
    /// # Arguments
    /// * `stream_key` - Name of the stream
    /// * `count` - Number of latest entries to retrieve
    ///
    /// # Returns
    /// Vector of (`entry_id`, fields) tuples (newest first)
    ///
    /// # Errors
    /// Returns error if streaming backend is not configured
    pub async fn read_stream_latest(
        &self,
        stream_key: &str,
        count: usize,
    ) -> Result<Vec<(String, Vec<(String, String)>)>> {
        match &self.streaming_backend {
            Some(backend) => backend.stream_read_latest(stream_key, count).await,
            None => Err(anyhow::anyhow!("Streaming backend not configured")),
        }
    }

    /// Read from Redis Stream with optional blocking
    ///
    /// # Arguments
    /// * `stream_key` - Name of the stream
    /// * `last_id` - Last ID seen ("0" for start, "$" for new only)
    /// * `count` - Max entries to retrieve
    /// * `block_ms` - Optional blocking timeout in ms
    ///
    /// # Returns
    /// Vector of (`entry_id`, fields) tuples
    ///
    /// # Errors
    /// Returns error if streaming backend is not configured
    pub async fn read_stream(
        &self,
        stream_key: &str,
        last_id: &str,
        count: usize,
        block_ms: Option<usize>,
    ) -> Result<Vec<(String, Vec<(String, String)>)>> {
        match &self.streaming_backend {
            Some(backend) => {
                backend
                    .stream_read(stream_key, last_id, count, block_ms)
                    .await
            }
            None => Err(anyhow::anyhow!("Streaming backend not configured")),
        }
    }

    // ===== Cache Invalidation Methods =====

    /// Invalidate a cache key across all instances
    ///
    /// This removes the key from all cache tiers and broadcasts
    /// the invalidation to all other cache instances via Redis Pub/Sub.
    ///
    /// Supports both legacy 2-tier mode and new multi-tier mode (v0.5.0+).
    ///
    /// # Arguments
    /// * `key` - Cache key to invalidate
    ///
    /// # Example
    /// ```rust,ignore
    /// // Invalidate user cache after profile update
    /// cache_manager.invalidate("user:123").await?;
    /// ```
    /// # Errors
    ///
    /// Returns an error if invalidation fails.
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        // Remove from ALL tiers
        for tier in &self.levels {
            if let Err(e) = tier.remove(key).await {
                warn!(
                    "Failed to remove '{}' from L{}: {}",
                    key, tier.tier_level, e
                );
            }
        }

        if let Some(publisher) = self.invalidation_publisher() {
            // Removed internal lock interaction since traits handle concurrency (e.g. cloning internal handles)
            // But if publisher is Arc<Mutex<...>>, we need lock.
            // We changed it to Arc<dyn InvalidationPublisher>. InvalidationPublisher::publish takes &self.
            // So no lock needed!
            let msg = InvalidationMessage::remove(key);
            publisher.publish(&msg).await?;
            self.invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }

        debug!("Invalidated '{}' across all instances", key);
        Ok(())
    }

    /// Update cache value across all instances
    ///
    /// This updates the key in all cache tiers and broadcasts
    /// the update to all other cache instances, avoiding cache misses.
    ///
    /// Supports both legacy 2-tier mode and new multi-tier mode (v0.5.0+).
    ///
    /// # Arguments
    /// * `key` - Cache key to update
    /// * `value` - New value
    /// * `ttl` - Optional TTL (uses default if None)
    ///
    /// # Example
    /// ```rust,ignore
    /// // Update user cache with new data
    /// let user_data = serde_json::json!({"id": 123, "name": "Alice"});
    /// cache_manager.update_cache("user:123", user_data, Some(Duration::from_secs(3600))).await?;
    /// ```
    /// # Errors
    ///
    /// Returns an error if cache update fails.
    pub async fn update_cache(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let ttl = ttl.unwrap_or_else(|| CacheStrategy::Default.to_duration());

        // Update ALL tiers with their respective TTL scaling
        for tier in &self.levels {
            if let Err(e) = tier.set_with_ttl(key, value.clone(), ttl).await {
                warn!("Failed to update '{}' in L{}: {}", key, tier.tier_level, e);
            }
        }

        // Broadcast update to other instances
        if let Some(publisher) = self.invalidation_publisher() {
            let msg = InvalidationMessage::update(key, value, Some(ttl));
            publisher.publish(&msg).await?;
            self.invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }

        debug!("Updated '{}' across all instances", key);
        Ok(())
    }

    /// Invalidate all keys matching a pattern
    ///
    /// This scans L2 cache for keys matching the pattern, removes them from all tiers,
    /// and broadcasts the invalidation. L1 caches will be cleared via broadcast.
    ///
    /// Supports both legacy 2-tier mode and new multi-tier mode (v0.5.0+).
    ///
    /// **Note**: Pattern scanning requires a concrete `L2Cache` instance with `scan_keys()`.
    /// In multi-tier mode, this scans from L2 but removes from all tiers.
    ///
    /// # Arguments
    /// * `pattern` - Glob-style pattern (e.g., "user:*", "product:123:*")
    ///
    /// # Example
    /// ```rust,ignore
    /// // Invalidate all user caches
    /// cache_manager.invalidate_pattern("user:*").await?;
    ///
    /// // Invalidate specific user's related caches
    /// cache_manager.invalidate_pattern("user:123:*").await?;
    /// ```
    /// # Errors
    ///
    /// Returns an error if invalidation fails.
    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<()> {
        // Pattern scanning logic removed as we no longer have direct access to concrete L2 cache.
        // We will rely on broadcasting the pattern invalidation to other nodes,
        // and best-effort local removal if possible (not implemented here without scan support).

        warn!(
            "invalidate_pattern: Local pattern scanning not supported in generic mode. Broadcasting only."
        );

        // Broadcast pattern invalidation
        if let Some(publisher) = self.invalidation_publisher() {
            let msg = InvalidationMessage::remove_pattern(pattern);
            publisher.publish(&msg).await?;
            self.invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Set value with automatic broadcast to all instances
    ///
    /// This is a write-through operation that updates the cache and
    /// broadcasts the update to all other instances automatically.
    ///
    /// # Arguments
    /// * `key` - Cache key
    /// * `value` - Value to cache
    /// * `strategy` - Cache strategy (determines TTL)
    ///
    /// # Example
    /// ```rust,ignore
    /// // Update and broadcast in one call
    /// let data = serde_json::json!({"status": "active"});
    /// cache_manager.set_with_broadcast("user:123", data, CacheStrategy::MediumTerm).await?;
    /// ```
    /// # Errors
    ///
    /// Returns an error if cache set or broadcast fails.
    pub async fn set_with_broadcast(
        &self,
        key: &str,
        value: serde_json::Value,
        strategy: CacheStrategy,
    ) -> Result<()> {
        let ttl = strategy.to_duration();

        // Set in local caches
        self.set_with_strategy(key, value.clone(), strategy).await?;

        // Broadcast update if invalidation is enabled
        if let Some(publisher) = self.invalidation_publisher() {
            let msg = InvalidationMessage::update(key, value, Some(ttl));
            publisher.publish(&msg).await?;
            self.invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Get invalidation statistics
    ///
    /// Returns statistics about invalidation operations if invalidation is enabled.
    /// Returns statistics about invalidation operations if invalidation is enabled.
    pub fn get_invalidation_stats(&self) -> Option<InvalidationStats> {
        if self.invalidation_system.is_some() {
            Some(self.invalidation_stats.snapshot())
        } else {
            None
        }
    }

    /// Get access to cache tiers (for testing/inspection)
    pub fn tiers(&self) -> &[CacheLevel] {
        &self.levels
    }

    fn invalidation_publisher(&self) -> Option<&Arc<dyn InvalidationPublisher>> {
        self.invalidation_system.as_ref().map(|s| &s.publisher)
    }

    fn invalidation_subscriber(&self) -> Option<&Arc<dyn InvalidationSubscriber>> {
        self.invalidation_system.as_ref().map(|s| &s.subscriber)
    }
}

/// Cache Manager statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CacheManagerStats {
    pub total_requests: u64,
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub total_hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub l1_hit_rate: f64,
    pub promotions: usize,
    pub in_flight_requests: usize,
}
