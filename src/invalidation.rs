//! Cache invalidation and synchronization module
//!
//! This module provides cross-instance cache invalidation using Redis Pub/Sub.
//! It supports both cache removal (invalidation) and cache updates (refresh).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::{InvalidationPublisher, InvalidationSubscriber};

/// Invalidation system for cross-instance cache invalidation.
///
/// This struct simply couples a publisher and a subscriber.
pub struct InvalidationSystem {
    pub(crate) publisher: Arc<dyn InvalidationPublisher>,
    pub(crate) subscriber: Arc<dyn InvalidationSubscriber>,
}

impl InvalidationSystem {
    /// Create a new InvalidationSystem.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use multi_level_cache::backends::redis::{RedisInvalidationPublisher, RedisInvalidationSubscriber};
    /// let publisher = Arc::new(RedisInvalidationPublisher::new("localhost:6379"));
    /// let subscriber = Arc::new(RedisInvalidationSubscriber::new("localhost:6379"));
    ///
    /// let invalidation_system = InvalidationSystem::new(publisher, subscriber);
    /// ```
    pub fn new(
        publisher: Arc<dyn InvalidationPublisher>,
        subscriber: Arc<dyn InvalidationSubscriber>,
    ) -> Self {
        Self {
            publisher,
            subscriber,
        }
    }
}

/// Invalidation message types sent across cache instances via Redis Pub/Sub
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InvalidationMessage {
    /// Remove a single key from all cache instances
    Remove { key: String },

    /// Update a key with new value across all cache instances
    /// This is more efficient than Remove for hot keys as it avoids cache miss
    Update {
        key: String,
        value: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl_secs: Option<u64>,
    },

    /// Remove all keys matching a pattern from all cache instances
    /// Uses glob-style patterns (e.g., "user:*", "product:123:*")
    RemovePattern { pattern: String },

    /// Bulk remove multiple keys at once
    RemoveBulk { keys: Vec<String> },
}

impl InvalidationMessage {
    /// Create a Remove message
    pub fn remove(key: impl Into<String>) -> Self {
        Self::Remove { key: key.into() }
    }

    /// Create an Update message
    pub fn update(key: impl Into<String>, value: serde_json::Value, ttl: Option<Duration>) -> Self {
        Self::Update {
            key: key.into(),
            value,
            ttl_secs: ttl.map(|d| d.as_secs()),
        }
    }

    /// Create a `RemovePattern` message
    pub fn remove_pattern(pattern: impl Into<String>) -> Self {
        Self::RemovePattern {
            pattern: pattern.into(),
        }
    }

    /// Create a `RemoveBulk` message
    #[must_use]
    pub fn remove_bulk(keys: Vec<String>) -> Self {
        Self::RemoveBulk { keys }
    }

    /// Serialize to JSON for transmission
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).context("Failed to serialize invalidation message")
    }

    /// Deserialize from JSON
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).context("Failed to deserialize invalidation message")
    }

    /// Get TTL as Duration if present
    pub fn ttl(&self) -> Option<Duration> {
        match self {
            Self::Update { ttl_secs, .. } => ttl_secs.map(Duration::from_secs),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_message_serialization() -> Result<()> {
        // Test Remove
        let msg = InvalidationMessage::remove("test_key");
        let json = msg.to_json()?;
        let parsed = InvalidationMessage::from_json(&json)?;
        match parsed {
            InvalidationMessage::Remove { key } => assert_eq!(key, "test_key"),
            _ => panic!("Wrong message type"),
        }

        // Test Update
        let msg = InvalidationMessage::update(
            "test_key",
            serde_json::json!({"value": 123}),
            Some(Duration::from_secs(300)),
        );
        let json = msg.to_json()?;
        let parsed = InvalidationMessage::from_json(&json)?;
        match parsed {
            InvalidationMessage::Update {
                key,
                value,
                ttl_secs,
            } => {
                assert_eq!(key, "test_key");
                assert_eq!(value, serde_json::json!({"value": 123}));
                assert_eq!(ttl_secs, Some(300));
            }
            _ => panic!("Wrong message type"),
        }

        // Test RemovePattern
        let msg = InvalidationMessage::remove_pattern("user:*");
        let json = msg.to_json()?;
        let parsed = InvalidationMessage::from_json(&json)?;
        match parsed {
            InvalidationMessage::RemovePattern { pattern } => assert_eq!(pattern, "user:*"),
            _ => panic!("Wrong message type"),
        }

        // Test RemoveBulk
        let msg = InvalidationMessage::remove_bulk(vec!["key1".to_string(), "key2".to_string()]);
        let json = msg.to_json()?;
        let parsed = InvalidationMessage::from_json(&json)?;
        match parsed {
            InvalidationMessage::RemoveBulk { keys } => assert_eq!(keys, vec!["key1", "key2"]),
            _ => panic!("Wrong message type"),
        }
        Ok(())
    }
}
