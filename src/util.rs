//! Small security-sensitive helpers.

/// Constant-time byte-slice equality for secret comparison (auth headers), so a
/// mismatch position is not leaked via timing. Differing lengths may be observed
/// — standard for secret/HMAC checks.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
