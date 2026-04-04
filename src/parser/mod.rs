mod claude;
mod codex;
mod factory;
mod opencode;

pub use claude::ClaudeParser;
pub use codex::CodexParser;
pub use factory::FactoryParser;
pub use opencode::OpenCodeParser;

use crate::session::{Message, Session};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Join consecutive messages from the same role into single messages.
/// Uses the latest timestamp when joining.
pub fn join_consecutive_messages(messages: Vec<Message>) -> Vec<Message> {
    messages.into_iter().fold(Vec::new(), |mut acc, msg| {
        if let Some(last) = acc.last_mut() {
            if last.role == msg.role {
                last.content.push_str("\n\n");
                last.content.push_str(&msg.content);
                last.timestamp = msg.timestamp; // use latest
                return acc;
            }
        }
        acc.push(msg);
        acc
    })
}

/// Trait for parsing session files
pub trait SessionParser {
    /// Parse a session file into a Session
    fn parse_file(path: &Path) -> Result<Session>;

    /// Check if this parser can handle the given file
    fn can_parse(path: &Path) -> bool;
}

/// Scan a directory tree for JSONL session files and insert into the seen map.
/// Uses `or_insert` so the first path seen for a given file_stem wins.
fn scan_jsonl_dir(dir: &Path, skip_agent: bool, seen: &mut HashMap<String, PathBuf>) {
    if !dir.exists() {
        return;
    }
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        let path = entry.path();
        if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if skip_agent && name.starts_with("agent-") {
                    continue;
                }
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    seen.entry(stem.to_string())
                        .or_insert_with(|| path.to_path_buf());
                }
            }
        }
    }
}

/// Discover all session files from Claude Code, Codex CLI, Factory, and OpenCode.
/// Deduplicates by file_stem (session UUID) across live and archive paths.
pub fn discover_session_files() -> Vec<PathBuf> {
    // Dedup key: file_stem (session UUID for Claude, rollout-* for Codex,
    // ses_* for OpenCode). Cross-source collision is benign — each source
    // uses a distinct naming scheme.
    let mut seen: HashMap<String, PathBuf> = HashMap::new();

    // Allow override for testing
    let home = std::env::var("RECALL_HOME_OVERRIDE")
        .map(PathBuf::from)
        .ok()
        .or_else(dirs::home_dir);

    if let Some(home) = home {
        // Archive MUST be scanned before live sessions: or_insert keeps the
        // first-seen path per session UUID, so archive (canonical copy) wins
        // over live (may be truncated or stale).
        let archive_dir = home.join(".claude/archive");
        scan_jsonl_dir(&archive_dir, true, &mut seen);

        // Claude Code: ~/.claude/projects/*/*.jsonl
        // Uses read_dir (not walkdir) because projects/ has a fixed 2-level structure.
        let claude_dir = home.join(".claude/projects");
        if claude_dir.exists() {
            if let Ok(projects) = std::fs::read_dir(&claude_dir) {
                for project in projects.flatten() {
                    if let Ok(sessions) = std::fs::read_dir(project.path()) {
                        for session in sessions.flatten() {
                            let path = session.path();
                            if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                    if name.starts_with("agent-") {
                                        continue;
                                    }
                                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                        seen.entry(stem.to_string())
                                            .or_insert_with(|| path.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Codex CLI: ~/.codex/sessions/**/*.jsonl
        let codex_dir = home.join(".codex/sessions");
        scan_jsonl_dir(&codex_dir, false, &mut seen);

        // Factory: ~/.factory/sessions/**/*.jsonl
        let factory_dir = home.join(".factory/sessions");
        scan_jsonl_dir(&factory_dir, false, &mut seen);

        // OpenCode: ~/.local/share/opencode/storage/session/**/*.json
        // Different extension (.json) and prefix filter (ses_*), so handled inline.
        let opencode_dir = home.join(".local/share/opencode/storage/session");
        if opencode_dir.exists() {
            for entry in walkdir::WalkDir::new(&opencode_dir)
                .into_iter()
                .flatten()
            {
                let path = entry.path();
                if path.extension().map(|e| e == "json").unwrap_or(false) {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with("ses_") {
                            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                seen.entry(stem.to_string())
                                    .or_insert_with(|| path.to_path_buf());
                            }
                        }
                    }
                }
            }
        }
    }

    seen.into_values().collect()
}

/// Parse a session file, auto-detecting the format
pub fn parse_session_file(path: &Path) -> Result<Session> {
    if ClaudeParser::can_parse(path) {
        ClaudeParser::parse_file(path)
    } else if CodexParser::can_parse(path) {
        CodexParser::parse_file(path)
    } else if FactoryParser::can_parse(path) {
        FactoryParser::parse_file(path)
    } else if OpenCodeParser::can_parse(path) {
        OpenCodeParser::parse_file(path)
    } else {
        anyhow::bail!("Unknown session file format: {:?}", path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Role;
    use chrono::Utc;

    #[test]
    fn test_join_consecutive_messages_different_roles() {
        let now = Utc::now();
        let messages = vec![
            Message { role: Role::User, content: "Hello".to_string(), timestamp: now },
            Message { role: Role::Assistant, content: "Hi".to_string(), timestamp: now },
            Message { role: Role::User, content: "Bye".to_string(), timestamp: now },
        ];
        let joined = join_consecutive_messages(messages);
        assert_eq!(joined.len(), 3);
    }

    #[test]
    fn test_join_consecutive_messages_same_role() {
        let t1 = Utc::now();
        let t2 = t1 + chrono::Duration::seconds(10);
        let messages = vec![
            Message { role: Role::User, content: "Part 1".to_string(), timestamp: t1 },
            Message { role: Role::User, content: "Part 2".to_string(), timestamp: t2 },
            Message { role: Role::Assistant, content: "Response".to_string(), timestamp: t2 },
        ];
        let joined = join_consecutive_messages(messages);
        assert_eq!(joined.len(), 2);
        assert_eq!(joined[0].content, "Part 1\n\nPart 2");
        assert_eq!(joined[0].timestamp, t2); // Uses latest timestamp
        assert_eq!(joined[1].content, "Response");
    }

    #[test]
    fn test_join_consecutive_messages_multiple_same_role() {
        let now = Utc::now();
        let messages = vec![
            Message { role: Role::Assistant, content: "A".to_string(), timestamp: now },
            Message { role: Role::Assistant, content: "B".to_string(), timestamp: now },
            Message { role: Role::Assistant, content: "C".to_string(), timestamp: now },
        ];
        let joined = join_consecutive_messages(messages);
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].content, "A\n\nB\n\nC");
    }

    /// Helper: create a minimal JSONL file that ClaudeParser can parse.
    fn write_dummy_session(path: &std::path::Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            r#"{"type":"summary","sessionId":"test","cwd":"/tmp","timestamp":"2024-01-01T00:00:00Z"}"#,
        )
        .unwrap();
    }

    #[test]
    fn test_dedup_prefers_archive() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let uuid = "deadbeef-1234-5678-9abc-def012345678";

        // Create the same session in both archive and live
        write_dummy_session(&home.join(format!(".claude/archive/proj/{uuid}.jsonl")));
        write_dummy_session(
            &home.join(format!(".claude/projects/proj/{uuid}.jsonl")),
        );

        std::env::set_var("RECALL_HOME_OVERRIDE", home.to_str().unwrap());
        let files = discover_session_files();
        std::env::remove_var("RECALL_HOME_OVERRIDE");

        let matching: Vec<_> = files
            .iter()
            .filter(|p| p.to_string_lossy().contains(uuid))
            .collect();
        assert_eq!(matching.len(), 1, "expected exactly one entry for {uuid}");
        assert!(
            matching[0].to_string_lossy().contains(".claude/archive"),
            "expected archive path, got {:?}",
            matching[0]
        );
    }

    #[test]
    fn test_no_dedup_for_unique() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let uuid = "aaaaaaaa-1111-2222-3333-444444444444";

        // Only in live
        write_dummy_session(
            &home.join(format!(".claude/projects/proj/{uuid}.jsonl")),
        );

        std::env::set_var("RECALL_HOME_OVERRIDE", home.to_str().unwrap());
        let files = discover_session_files();
        std::env::remove_var("RECALL_HOME_OVERRIDE");

        let matching: Vec<_> = files
            .iter()
            .filter(|p| p.to_string_lossy().contains(uuid))
            .collect();
        assert_eq!(matching.len(), 1);
        assert!(matching[0].to_string_lossy().contains(".claude/projects"));
    }

    #[test]
    fn test_archive_only() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let uuid = "bbbbbbbb-5555-6666-7777-888888888888";

        // Only in archive
        write_dummy_session(&home.join(format!(".claude/archive/proj/{uuid}.jsonl")));

        std::env::set_var("RECALL_HOME_OVERRIDE", home.to_str().unwrap());
        let files = discover_session_files();
        std::env::remove_var("RECALL_HOME_OVERRIDE");

        let matching: Vec<_> = files
            .iter()
            .filter(|p| p.to_string_lossy().contains(uuid))
            .collect();
        assert_eq!(matching.len(), 1);
        assert!(matching[0].to_string_lossy().contains(".claude/archive"));
    }

    #[test]
    fn test_cross_source_no_collision() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();

        // Claude session
        let claude_uuid = "cccccccc-aaaa-bbbb-cccc-dddddddddddd";
        write_dummy_session(
            &home.join(format!(".claude/projects/proj/{claude_uuid}.jsonl")),
        );

        // Codex session with rollout- prefix
        let codex_name = "rollout-2026-01-01T00-00-00-cccccccc-aaaa-bbbb-cccc-dddddddddddd";
        let codex_dir = home.join(".codex/sessions/2026");
        std::fs::create_dir_all(&codex_dir).unwrap();
        std::fs::write(
            codex_dir.join(format!("{codex_name}.jsonl")),
            r#"{"type":"message","role":"user"}"#,
        )
        .unwrap();

        std::env::set_var("RECALL_HOME_OVERRIDE", home.to_str().unwrap());
        let files = discover_session_files();
        std::env::remove_var("RECALL_HOME_OVERRIDE");

        // Both should be present (different file_stems, no collision)
        let claude_count = files
            .iter()
            .filter(|p| p.to_string_lossy().contains(claude_uuid) && !p.to_string_lossy().contains("rollout"))
            .count();
        let codex_count = files
            .iter()
            .filter(|p| p.to_string_lossy().contains("rollout"))
            .count();
        assert_eq!(claude_count, 1, "claude session should be found");
        assert_eq!(codex_count, 1, "codex session should be found");
    }
}
