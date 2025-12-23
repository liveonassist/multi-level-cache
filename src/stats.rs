//! Types for tracking cache statistics.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Trait to map numeric types to their atomic equivalents
pub trait StatType {
    type Atomic: Send + Sync + std::fmt::Debug + Default;
}

impl StatType for u64 {
    type Atomic = AtomicU64;
}

impl StatType for usize {
    type Atomic = AtomicUsize;
}

/// Macro to define a stats struct and its atomic counterpart.
#[macro_export]
macro_rules! define_stats {
    (
        $(#[$attr:meta])*
        struct $name:ident {
            $(
                $(#[$field_attr:meta])*
                pub $field:ident : $type:ty,
            )*
        }
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Default)]
        pub struct $name {
            $(
                $(#[$field_attr])*
                pub(crate) $field: $type,
            )*
        }

        impl $name {
            $(
                $(#[$field_attr])*
                pub fn $field(&self) -> $type {
                    self.$field
                }
            )*
        }

        paste::paste! {
            $(#[$attr])*
            #[derive(Debug, Default)]
            pub struct [<Atomic $name>] {
                $(
                    $(#[$field_attr])*
                    pub(crate) $field: <$type as $crate::stats::StatType>::Atomic,
                )*
            }

            impl [<Atomic $name>] {
                /// Create a snapshot of current stats
                pub fn snapshot(&self) -> $name {
                    $name {
                        $(
                            $field: self.$field.load(Ordering::Relaxed),
                        )*
                    }
                }

                $(
                    $(#[$field_attr])*
                    pub fn $field(&self) -> $type {
                        self.$field.load(Ordering::Relaxed)
                    }
                )*
            }
        }
    };
}

define_stats! {
    /// Statistics for invalidation operations
    struct InvalidationStats {
        pub messages_sent: u64,
        pub messages_received: u64,
        pub processing_errors: u64,
        pub removes_received: u64,
        pub updates_received: u64,
        pub patterns_received: u64,
        pub bulk_removes_received: u64,
    }
}

define_stats! {
    /// Statistics for a cache
    struct CacheStats {
        pub hits: u64,
        pub misses: u64,
        pub sets: u64,
    }
}
