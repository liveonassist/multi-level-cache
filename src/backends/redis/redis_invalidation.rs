use std::sync::Arc;
use std::{
    sync::atomic::Ordering,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use futures_util::StreamExt as _;
use redis::AsyncCommands as _;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::invalidation::InvalidationMessage;
use crate::{
    stats::{AtomicInvalidationStats, InvalidationStats},
    traits::{InvalidationPublisher, InvalidationSubscriber},
};

/// Configuration for cache invalidation
#[derive(Debug, Clone)]
pub struct RedisInvalidationConfig {
    /// Redis Pub/Sub channel name for invalidation messages
    pub channel: String,

    /// Whether to automatically broadcast invalidation on writes
    pub auto_broadcast_on_write: bool,

    /// Whether to also publish invalidation events to Redis Streams for audit
    pub enable_audit_stream: bool,

    /// Redis Stream name for invalidation audit trail
    pub audit_stream: String,

    /// Maximum length of audit stream (older entries are trimmed)
    pub audit_stream_maxlen: Option<usize>,
}

impl Default for RedisInvalidationConfig {
    fn default() -> Self {
        Self {
            channel: "cache:invalidate".to_string(),
            auto_broadcast_on_write: false, // Conservative default
            enable_audit_stream: false,
            audit_stream: "cache:invalidations".to_string(),
            audit_stream_maxlen: Some(10000),
        }
    }
}

/// Redis streams-backed invalidation publisher.
pub struct RedisInvalidationPublisher {
    connection: redis::aio::ConnectionManager,
    config: RedisInvalidationConfig,
}

#[async_trait::async_trait]
impl InvalidationPublisher for RedisInvalidationPublisher {
    async fn publish(&self, message: &InvalidationMessage) -> anyhow::Result<()> {
        let json = message.to_json()?;
        let mut conn = self.connection.clone();

        // Publish to Pub/Sub channel
        let _: () = conn
            .publish(&self.config.channel, &json)
            .await
            .context("Failed to publish invalidation message")?;

        // Optionally publish to audit stream
        if self.config.enable_audit_stream {
            // We need a mutable reference to call the helper, but we only have &self.
            // Since we cloned connection above, we can use that if we refactor publish_to_audit_stream
            // or just clone again/construct a helper.
            // Let's refactor publish_to_audit_stream to take a connection.
            if let Err(e) = self.publish_to_audit_stream(&mut conn, message).await {
                // Don't fail the invalidation if audit logging fails
                warn!("Failed to publish to audit stream: {}", e);
            }
        }

        Ok(())
    }
}

impl RedisInvalidationPublisher {
    /// Create a new publisher
    #[must_use]
    pub fn new(connection: redis::aio::ConnectionManager, config: RedisInvalidationConfig) -> Self {
        Self { connection, config }
    }

    /// Publish to audit stream for observability
    async fn publish_to_audit_stream(
        &self,
        conn: &mut redis::aio::ConnectionManager,
        message: &InvalidationMessage,
    ) -> anyhow::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
            .to_string();

        // Use &str to avoid unnecessary allocations
        let (type_str, key_str): (&str, &str);
        let extra_str: String;

        match message {
            InvalidationMessage::Remove { key } => {
                type_str = "remove";
                key_str = key.as_str();
                extra_str = String::new();
            }
            InvalidationMessage::Update { key, .. } => {
                type_str = "update";
                key_str = key.as_str();
                extra_str = String::new();
            }
            InvalidationMessage::RemovePattern { pattern } => {
                type_str = "remove_pattern";
                key_str = pattern.as_str();
                extra_str = String::new();
            }
            InvalidationMessage::RemoveBulk { keys } => {
                type_str = "remove_bulk";
                key_str = "";
                extra_str = keys.len().to_string();
            }
        }

        let mut fields = vec![("type", type_str), ("timestamp", timestamp.as_str())];

        if !key_str.is_empty() {
            fields.push(("key", key_str));
        }
        if !extra_str.is_empty() {
            fields.push(("count", extra_str.as_str()));
        }

        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.config.audit_stream);

        if let Some(maxlen) = self.config.audit_stream_maxlen {
            cmd.arg("MAXLEN").arg("~").arg(maxlen);
        }

        cmd.arg("*"); // Auto-generate ID

        for (key, value) in fields {
            cmd.arg(key).arg(value);
        }

        let _: String = cmd
            .query_async(conn)
            .await
            .context("Failed to add to audit stream")?;

        Ok(())
    }
}

/// Handle for subscribing to invalidation messages (Redis implementation)
///
/// This spawns a background task that listens to Redis Pub/Sub and processes
/// invalidation messages by calling the provided handler callback.
pub struct RedisInvalidationSubscriber {
    /// Redis client for creating Pub/Sub connections
    client: redis::Client,
    /// Configuration
    config: RedisInvalidationConfig,
    /// Statistics
    stats: Arc<AtomicInvalidationStats>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
}

#[async_trait::async_trait]
impl InvalidationSubscriber for RedisInvalidationSubscriber {
    async fn subscribe(&self) -> anyhow::Result<tokio::sync::mpsc::Receiver<InvalidationMessage>> {
        let client = self.client.clone();
        let channel = self.config.channel.clone();
        let stats = Arc::clone(&self.stats);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Create mpsc channel for invalidation messages
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            loop {
                // Check for shutdown signal
                if shutdown_rx.try_recv().is_ok() {
                    info!("Invalidation subscriber shutting down...");
                    break;
                }

                // Attempt to connect and subscribe
                match Self::run_subscriber_loop(
                    &client,
                    &channel,
                    tx.clone(),
                    Arc::clone(&stats),
                    &mut shutdown_rx,
                )
                .await
                {
                    Ok(()) => {
                        info!("Invalidation subscriber loop completed normally");
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Invalidation subscriber error: {}. Reconnecting in 5s...",
                            e
                        );
                        stats.processing_errors.fetch_add(1, Ordering::Relaxed);

                        // Wait before reconnecting
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_secs(5)) => {},
                            _ = shutdown_rx.recv() => {
                                info!("Invalidation subscriber shutting down...");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}

impl RedisInvalidationSubscriber {
    /// Create a new subscriber
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL
    /// * `config` - Invalidation configuration
    /// # Errors
    ///
    /// Returns an error if Redis client creation fails.
    pub fn new(redis_url: &str, config: RedisInvalidationConfig) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("Failed to create Redis client for subscriber")?;

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            client,
            config,
            stats: Arc::new(AtomicInvalidationStats::default()),
            shutdown_tx,
        })
    }

    /// Get a snapshot of current statistics
    #[must_use]
    pub fn stats(&self) -> InvalidationStats {
        self.stats.snapshot()
    }

    /// Internal subscriber loop
    async fn run_subscriber_loop(
        client: &redis::Client,
        channel: &str,
        tx: tokio::sync::mpsc::Sender<InvalidationMessage>,
        stats: Arc<AtomicInvalidationStats>,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        // Get Pub/Sub connection
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .context("Failed to get pubsub connection")?;

        // Subscribe to channel
        pubsub
            .subscribe(channel)
            .await
            .context("Failed to subscribe to channel")?;

        info!("Subscribed to invalidation channel: {}", channel);

        // Get message stream
        let mut stream = pubsub.on_message();

        loop {
            // Wait for message or shutdown signal
            tokio::select! {
                msg_result = stream.next() => {
                    match msg_result {
                        Some(msg) => {
                            // Get payload
                            let payload: String = match msg.get_payload() {
                                Ok(p) => p,
                                Err(e) => {
                                    warn!("Failed to get message payload: {}", e);
                                    stats.processing_errors.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                            };

                            // Deserialize message
                            let invalidation_msg = match InvalidationMessage::from_json(&payload) {
                                Ok(m) => m,
                                Err(e) => {
                                    warn!("Failed to deserialize invalidation message: {}", e);
                                    stats.processing_errors.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                            };

                            // Update stats
                            stats.messages_received.fetch_add(1, Ordering::Relaxed);
                            match &invalidation_msg {
                                InvalidationMessage::Remove { .. } => {
                                    stats.removes_received.fetch_add(1, Ordering::Relaxed);
                                }
                                InvalidationMessage::Update { .. } => {
                                    stats.updates_received.fetch_add(1, Ordering::Relaxed);
                                }
                                InvalidationMessage::RemovePattern { .. } => {
                                    stats.patterns_received.fetch_add(1, Ordering::Relaxed);
                                }
                                InvalidationMessage::RemoveBulk { .. } => {
                                    stats.bulk_removes_received.fetch_add(1, Ordering::Relaxed);
                                }
                            }

                            // Send to channel
                            if let Err(e) = tx.send(invalidation_msg).await {
                                // Receiver dropped, stop loop
                                return Err(anyhow::anyhow!("Invalidation channel closed: {}", e));
                            }
                        }
                        None => {
                            // Stream ended
                            return Err(anyhow::anyhow!("Pub/Sub message stream ended"));
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    return Ok(());
                }
            }
        }
    }

    /// Signal the subscriber to shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}
