//! Prompt heuristics — port of the intent detectors in `server.ts` that decide,
//! from the natural-language prompt alone, whether the agent will want to open a
//! PR, edit the workspace, or use the shell. They gate which capabilities the
//! agent runner is granted, and they drive the optimistic-mode fast paths.

use regex::Regex;
use std::sync::OnceLock;

fn re(pattern: &str) -> Regex {
    Regex::new(pattern).expect("static regex")
}

macro_rules! lazy_re {
    ($name:ident, $pat:expr) => {
        fn $name() -> &'static Regex {
            static R: OnceLock<Regex> = OnceLock::new();
            R.get_or_init(|| re($pat))
        }
    };
}

lazy_re!(
    negated_pr,
    r"(?i)\b(?:do\s+not|don't|dont|never)\s+(?:(?:open|create|submit|make|raise)\s+)?(?:a\s+)?(?:draft\s+)?(?:pr|pull\s+request|merge\s+request)\b"
);
lazy_re!(
    negated_pr_without,
    r"(?i)\b(?:without|no)\s+(?:(?:opening|creating|submitting|making|raising)\s+)?(?:a\s+)?(?:draft\s+)?(?:pr|pull\s+request|merge\s+request)s?\b"
);
lazy_re!(
    negated_pr_bare,
    r"(?i)\b(?:without|no)\s+(?:pr|pull\s+request|merge\s+request)s?\b"
);
lazy_re!(
    pr_request,
    r"(?i)\b(?:open|create|submit|make|raise)\s+(?:a\s+)?(?:draft\s+)?(?:pr|pull\s+request|merge\s+request)\b"
);
lazy_re!(pr_mention, r"(?i)\b(?:pr|pull\s+request|merge\s+request)\b");

fn strip_negated_pr(prompt: &str) -> String {
    let s = negated_pr().replace_all(prompt, " ");
    let s = negated_pr_without().replace_all(&s, " ");
    negated_pr_bare().replace_all(&s, " ").into_owned()
}

/// Does the prompt actually ask to open a PR (after stripping negations)?
pub fn prompt_requests_pull_request(prompt: &str) -> bool {
    let stripped = strip_negated_pr(prompt);
    pr_request().is_match(&stripped) || pr_mention().is_match(&stripped)
}

lazy_re!(
    negated_change_1,
    r"(?i)\b(?:do\s+not|don't|dont|never)\s+(?:make\s+)?(?:any\s+)?(?:file|code|workspace|repo(?:sitory)?|source)?\s*(?:changes?|edits?|modifications?)\b"
);
lazy_re!(
    negated_change_2,
    r"(?i)\b(?:do\s+not|don't|dont|never)\s+(?:edit|change|modify|write|update|create|delete|remove|patch|fix|implement)\b"
);
lazy_re!(
    negated_change_3,
    r"(?i)\b(?:without|no)\s+(?:making\s+)?(?:any\s+)?(?:file|code|workspace|repo(?:sitory)?|source)?\s*(?:changes?|edits?|modifications?)\b"
);

fn strip_negated_change(prompt: &str) -> String {
    let s = negated_change_1().replace_all(prompt, " ");
    let s = negated_change_2().replace_all(&s, " ");
    negated_change_3().replace_all(&s, " ").into_owned()
}

lazy_re!(
    change_verb,
    r"(?i)\b(add|append|change|create|delete|edit|fix|implement|modify|move|patch|refactor|remove|rename|replace|update|write)\b"
);

pub fn prompt_likely_requires_workspace_change(prompt: &str) -> bool {
    change_verb().is_match(&strip_negated_change(prompt))
}

lazy_re!(
    named_file,
    r"(?i)\b(readme(?:\.md)?|package(?:\.json)?|pnpm-lock|dockerfile|makefile|tsconfig|cargo\.toml|go\.mod)\b"
);
lazy_re!(
    workspace_noun,
    r"(?i)\b(repo(?:sitory)?|codebase|workspace|working tree|source tree|folders?|directories|dirs?|files?|top[- ]level|root)\b"
);
lazy_re!(
    inspection_verb,
    r"(?i)\b(count|find|grep|how many|inspect|list|look|open|read|search|show|tree|what|where|which)\b"
);

pub fn prompt_likely_requires_workspace_access(prompt: &str) -> bool {
    if prompt_likely_requires_workspace_change(prompt) {
        return true;
    }
    let workspace = strip_negated_change(prompt);
    if named_file().is_match(&workspace) {
        return true;
    }
    workspace_noun().is_match(&workspace) && inspection_verb().is_match(&workspace)
}

lazy_re!(
    shell_git,
    r"(?i)\bgit\s+(?:fetch|merge|push|commit|branch)\b"
);
lazy_re!(
    shell_fetch_push,
    r"(?i)\b(?:fetch|push)\s+(?:origin|the\s+current\s+branch|current\s+branch|branches?)\b"
);
lazy_re!(
    shell_commit,
    r"(?i)\bcommit\s+(?:the\s+integrated\s+result|current\s+changes?|workspace\s+changes?|merge\s+result)\b"
);
lazy_re!(shell_merge_sibling, r"(?i)\bmerge\s+(?:with\s+)?sibling\b");
lazy_re!(
    shell_sibling_branches,
    r"(?i)\bsibling\s+feature\s+branches?\b"
);

pub fn prompt_likely_requires_shell_access(prompt: &str) -> bool {
    let p = strip_negated_change(prompt);
    shell_git().is_match(&p)
        || shell_fetch_push().is_match(&p)
        || shell_commit().is_match(&p)
        || shell_merge_sibling().is_match(&p)
        || shell_sibling_branches().is_match(&p)
}

/// A deterministic "append <text> to <file>" edit parsed from the prompt, used by
/// optimistic mode to satisfy trivial requests without invoking a model.
#[derive(Debug, Clone)]
pub struct AppendFileEdit {
    pub text: String,
    pub relative_path: String,
}

lazy_re!(
    append_quoted,
    r#"(?i)\b(?:append(?:ing)?|add(?:ing)?)\s+(?:"([^"]+)"|'([^']+)'|`([^`]+)`)\s+(?:to|into)\s+(?:the\s+)?(?:file\s+)?([A-Za-z0-9][A-Za-z0-9._/-]*)"#
);
lazy_re!(
    append_unquoted,
    r#"(?i)\b(?:append(?:ing)?|add(?:ing)?)\s+([A-Za-z0-9][A-Za-z0-9._-]*)\s+(?:to|into)\s+(?:the\s+)?(?:file\s+)?([A-Za-z0-9][A-Za-z0-9._/-]*)"#
);

pub fn parse_deterministic_append(prompt: &str) -> Option<AppendFileEdit> {
    if let Some(c) = append_quoted().captures(prompt) {
        let text = c
            .get(1)
            .or_else(|| c.get(2))
            .or_else(|| c.get(3))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        return Some(AppendFileEdit {
            text,
            relative_path: c.get(4)?.as_str().to_string(),
        });
    }
    let c = append_unquoted().captures(prompt)?;
    Some(AppendFileEdit {
        text: c.get(1)?.as_str().to_string(),
        relative_path: c.get(2)?.as_str().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_pr_intent_and_respects_negation() {
        assert!(prompt_requests_pull_request(
            "please open a draft PR when done"
        ));
        assert!(prompt_requests_pull_request("raise a pull request"));
        assert!(!prompt_requests_pull_request("do not open a PR"));
        assert!(!prompt_requests_pull_request(
            "without opening a pull request"
        ));
    }

    #[test]
    fn detects_workspace_change_intent() {
        assert!(prompt_likely_requires_workspace_change(
            "add a function to utils"
        ));
        assert!(prompt_likely_requires_workspace_change(
            "refactor the parser"
        ));
        assert!(!prompt_likely_requires_workspace_change(
            "do not make any changes, just explain"
        ));
    }

    #[test]
    fn workspace_access_covers_inspection() {
        assert!(prompt_likely_requires_workspace_access(
            "list the files in the repo"
        ));
        assert!(prompt_likely_requires_workspace_access("read the README"));
        assert!(!prompt_likely_requires_workspace_access(
            "what is the capital of France"
        ));
    }

    #[test]
    fn shell_access_detects_git_ops() {
        assert!(prompt_likely_requires_shell_access(
            "git push the current branch"
        ));
        assert!(prompt_likely_requires_shell_access(
            "merge sibling feature branches"
        ));
        assert!(!prompt_likely_requires_shell_access("summarize the code"));
    }

    #[test]
    fn parses_deterministic_append() {
        let e = parse_deterministic_append(r#"append "hello world" to notes.txt"#).unwrap();
        assert_eq!(e.text, "hello world");
        assert_eq!(e.relative_path, "notes.txt");

        let u = parse_deterministic_append("add foobar to docs/log.md").unwrap();
        assert_eq!(u.text, "foobar");
        assert_eq!(u.relative_path, "docs/log.md");

        assert!(parse_deterministic_append("just do something").is_none());
    }
}
