//! Secret redaction for anything that leaves the process (SSE, NATS, ingest,
//! breadcrumbs). Port of `sanitizeEventText` in `server.ts`: env-configured
//! secrets are replaced by value, and provider key shapes are replaced by regex.

use regex::Regex;
use std::sync::OnceLock;

/// Env var names whose *values* are scrubbed out of any emitted text.
const SECRET_ENV_KEYS: &[&str] = &[
    "OPENAI_API_KEY",
    "OPENAI_API_KEYS",
    "OPENAI_API_KEYS_JSON",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_API_KEYS",
    "ANTHROPIC_API_KEYS_JSON",
    "CLAUDE_API_KEY",
    "CLAUDE_API_KEYS",
    "CLAUDE_API_KEYS_JSON",
    "GEMINI_API_KEY",
    "GEMINI_API_KEYS",
    "GEMINI_API_KEYS_JSON",
    "GOOGLE_API_KEY",
    "GOOGLE_API_KEYS",
    "GOOGLE_API_KEYS_JSON",
    "OPENCODE_API_KEY",
    "OPENCODE_API_KEYS",
    "OPENCODE_API_KEYS_JSON",
    "OPENCODE_ZEN_API_KEY",
    "OPENCODE_ZEN_API_KEYS",
    "OPENCODE_ZEN_API_KEYS_JSON",
    "DEEPSEEK_API_KEY",
    "DEEPSEEK_API_KEYS",
    "DEEPSEEK_API_KEYS_JSON",
    "XAI_API_KEY",
    "XAI_API_KEYS",
    "XAI_API_KEYS_JSON",
    "GROK_API_KEY",
    "GROK_API_KEYS",
    "GROK_API_KEYS_JSON",
    "GH_PAT",
    "GH_DEPLOY_KEY",
    "SERVER_AUTH_SECRET",
    "EVENT_INGEST_SECRET",
    "FIDUCIA_CONTROL_PLANE_SECRET",
    "FIDUCIA_INTERNAL_SECRET",
    "FIDUCIA_NODE_INTERNAL_SECRET",
    "SUPABASE_SERVICE_ROLE_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];

struct Patterns {
    rules: Vec<(Regex, &'static str)>,
}

fn patterns() -> &'static Patterns {
    static P: OnceLock<Patterns> = OnceLock::new();
    P.get_or_init(|| {
        let rules = vec![
            (
                r"\bsk-ant-[A-Za-z0-9_*.-]{8,}\b",
                "[redacted-anthropic-key]",
            ),
            (r"\bsk-oc-[A-Za-z0-9_*.-]{8,}\b", "[redacted-opencode-key]"),
            (r"\bsk-[A-Za-z0-9_*.-]{8,}\b", "[redacted-openai-key]"),
            (r"\bAIza[A-Za-z0-9_*\-]{12,}\b", "[redacted-google-key]"),
            (r"\bxai-[A-Za-z0-9_*.-]{24,}\b", "[redacted-xai-key]"),
            (
                r"\b(?:ghp|github_pat)_[A-Za-z0-9_*.-]{8,}\b",
                "[redacted-github-token]",
            ),
            (
                r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b",
                "[redacted-aws-access-key]",
            ),
            (
                r"\bIQoJ[A-Za-z0-9+/=_\-]{180,}\b",
                "[redacted-aws-session-token]",
            ),
        ]
        .into_iter()
        .filter_map(|(pat, repl)| Regex::new(pat).ok().map(|re| (re, repl)))
        .collect();
        Patterns { rules }
    })
}

/// Redact env-configured secret values and provider key shapes from `value`.
pub fn sanitize_event_text(value: &str) -> String {
    let mut output = value.to_string();
    for key in SECRET_ENV_KEYS {
        if let Ok(secret) = std::env::var(key) {
            if secret.len() >= 8 {
                output = output.replace(&secret, "[redacted-secret]");
            }
        }
    }
    // Anchored key-shape ordering matters: `sk-ant-`/`sk-oc-` before `sk-`.
    for (re, repl) in &patterns().rules {
        output = re.replace_all(&output, *repl).into_owned();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_provider_key_shapes() {
        let s = sanitize_event_text("token sk-ant-api03-abcdefgh_ijkl here");
        assert!(s.contains("[redacted-anthropic-key]"), "{s}");
        assert!(!s.contains("sk-ant-"));

        let openai = sanitize_event_text("sk-abcdefgh12345678");
        assert!(openai.contains("[redacted-openai-key]"));

        let gh = sanitize_event_text("ghp_ABCDEFGH12345678");
        assert!(gh.contains("[redacted-github-token]"));

        let aws = sanitize_event_text("AKIAIOSFODNN7EXAMPLE");
        assert!(aws.contains("[redacted-aws-access-key]"));
    }

    #[test]
    fn anthropic_shape_wins_over_generic_sk() {
        // `sk-ant-` must be redacted as anthropic, not as the generic openai `sk-`.
        let s = sanitize_event_text("sk-ant-api03-XXXXXXXX");
        assert!(s.contains("[redacted-anthropic-key]"));
        assert!(!s.contains("[redacted-openai-key]"));
    }

    #[test]
    fn redacts_env_configured_secret_values() {
        std::env::set_var("GH_PAT", "supersecretvalue123");
        std::env::set_var("FIDUCIA_NODE_INTERNAL_SECRET", "node-secret-value-456");
        let s =
            sanitize_event_text("leaking supersecretvalue123 and node-secret-value-456 into a log");
        assert!(s.contains("[redacted-secret]"), "{s}");
        assert!(!s.contains("supersecretvalue123"));
        assert!(!s.contains("node-secret-value-456"));
        std::env::remove_var("GH_PAT");
        std::env::remove_var("FIDUCIA_NODE_INTERNAL_SECRET");
    }

    #[test]
    fn leaves_benign_text_untouched() {
        let s = "just a normal message with no secrets";
        assert_eq!(sanitize_event_text(s), s);
    }
}
