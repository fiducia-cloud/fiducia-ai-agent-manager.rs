//! Artifact storage — the Rust port of `src/storage/`. After an agent run, the
//! orchestrator scans `${OUTPUTS_DIR}/<taskId>/` and publishes each file through
//! the configured adapter, emitting an `artifact` event per file.
//!
//! The Node service shipped s3/r2/gcs/drive/local adapters. This crate ships the
//! `local` adapter (copy into a served directory) behind the same
//! [`StorageAdapter`] trait, so the cloud adapters can be added without touching
//! the orchestration. Every adapter returns the same [`PublishedArtifact`] shape.

use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedArtifact {
    pub filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    pub storage_provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_key: Option<String>,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

pub struct PublishOptions {
    pub task_id: String,
    pub file_path: String,
    pub filename: Option<String>,
}

#[async_trait::async_trait]
pub trait StorageAdapter: Send + Sync {
    fn provider(&self) -> &'static str;
    async fn publish(&self, opts: PublishOptions) -> Result<PublishedArtifact, String>;
}

/// Copies published files into `<root>/<taskId>/<filename>` and returns a
/// `file://` URL. `root` is typically served by the ingress or synced out-of-band.
pub struct LocalStorage {
    root: String,
}

impl LocalStorage {
    pub fn new(root: impl Into<String>) -> Self {
        LocalStorage { root: root.into() }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for LocalStorage {
    fn provider(&self) -> &'static str {
        "local"
    }

    async fn publish(&self, opts: PublishOptions) -> Result<PublishedArtifact, String> {
        let src = Path::new(&opts.file_path);
        let filename = opts
            .filename
            .or_else(|| src.file_name().map(|n| n.to_string_lossy().to_string()))
            .ok_or("cannot determine artifact filename")?;
        let key = format!("remote-dev/{}/{}", opts.task_id, filename);
        let dest_dir = Path::new(&self.root).join(&opts.task_id);
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| format!("mkdir {}: {e}", dest_dir.display()))?;
        let dest = dest_dir.join(&filename);
        let bytes = tokio::fs::read(src)
            .await
            .map_err(|e| format!("read {}: {e}", src.display()))?;
        tokio::fs::write(&dest, &bytes)
            .await
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
        Ok(PublishedArtifact {
            filename,
            content_type: sniff_content_type(&opts.file_path),
            size_bytes: Some(bytes.len() as u64),
            storage_provider: "local".into(),
            storage_key: Some(key),
            url: format!("file://{}", dest.display()),
            sha256: Some(sha256_hex(&bytes)),
        })
    }
}

fn sniff_content_type(path: &str) -> Option<String> {
    let ext = Path::new(path).extension()?.to_string_lossy().to_lowercase();
    let ct = match ext.as_str() {
        "json" => "application/json",
        "txt" | "log" => "text/plain",
        "md" => "text/markdown",
        "html" => "text/html",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        _ => return None,
    };
    Some(ct.to_string())
}

/// Minimal, dependency-free SHA-256 (FIPS 180-4) for artifact digests.
fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = String::with_capacity(64);
    for word in h {
        out.push_str(&format!("{word:08x}"));
    }
    out
}
