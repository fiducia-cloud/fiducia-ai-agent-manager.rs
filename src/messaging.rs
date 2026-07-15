//! The platform-standard message envelope.
//!
//! This module deliberately re-exports `fiducia-messaging` instead of carrying
//! another wire-format copy. NATS is delivery; fiducia-node remains authority.

pub use fiducia_messaging::MessageEnvelope;
