use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogEntry>,
    #[serde(default)]
    last_included_index: u64,
    #[serde(default)]
    last_included_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "raft", rename_all = "snake_case")]
pub enum Rpc {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        term: u64,
        vote_granted: bool,
        voter: String,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        match_index: u64,
        follower: String,
    },
    InstallSnapshot {
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        data_hex: String,
        done: bool,
        sha256: String,
    },
    InstallSnapshotResp {
        term: u64,
        success: bool,
        offset: u64,
        follower: String,
    },
}

#[derive(Debug)]
pub enum Action {
    Send { to: Option<String>, rpc: Rpc },
    Apply { index: u64, command: String },
    Info(String),
}

pub struct Raft {
    pub id: String,
    pub cluster: Vec<String>,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<String>,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    votes: HashSet<String>,
    election_deadline: Instant,
    heartbeat_due: Instant,
    data_path: PathBuf,
    pub last_included_index: u64,
    pub last_included_term: u64,
}

include!("raft_core.rs");
include!("raft_elect.rs");
include!("raft_append.rs");
include!("raft_snap.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    fn node(id: &str, cluster: &[&str]) -> Raft {
        let dir = temp_dir().join(format!("xanadu-raft-test-{id}-{}", nanos()));
        let _ = std::fs::create_dir_all(&dir);
        Raft::new(
            id.into(),
            cluster.iter().map(|s| s.to_string()).collect(),
            &dir,
        )
    }

    fn nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[test]
    fn single_node_wins_election() {
        let mut r = node("alpha", &["alpha"]);
        let now = Instant::now() + Duration::from_secs(5);
        r.tick(now, &[]);
        assert_eq!(r.role, Role::Leader);
        assert!(r.current_term >= 1);
    }

    #[test]
    fn three_node_waits_for_live_peer() {
        let mut r = node("alpha", &["alpha", "beta", "gamma"]);
        let now = Instant::now() + Duration::from_secs(5);
        r.tick(now, &[]);
        assert_eq!(r.role, Role::Follower);
        r.tick(now + Duration::from_secs(5), &["beta".into()]);
        assert_eq!(r.role, Role::Candidate);
    }

    #[test]
    fn follower_grants_vote_on_first_request() {
        let mut r = node("beta", &["alpha", "beta", "gamma"]);
        let now = Instant::now();
        let out = r.on_rpc(
            Rpc::RequestVote {
                term: 1,
                candidate_id: "alpha".into(),
                last_log_index: 0,
                last_log_term: 0,
            },
            now,
        );
        match &out[0] {
            Action::Send {
                rpc: Rpc::RequestVoteResp { vote_granted, .. },
                ..
            } => assert!(*vote_granted),
            _ => panic!("expected vote resp"),
        }
        assert_eq!(r.voted_for.as_deref(), Some("alpha"));
    }

    #[test]
    fn stale_term_append_rejected() {
        let mut r = node("beta", &["alpha", "beta"]);
        r.current_term = 3;
        let out = r.on_rpc(
            Rpc::AppendEntries {
                term: 1,
                leader_id: "alpha".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        match &out[0] {
            Action::Send {
                rpc: Rpc::AppendEntriesResp { success, .. },
                ..
            } => assert!(!*success),
            _ => panic!("expected ae resp"),
        }
    }

    #[test]
    fn leader_commit_needs_majority() {
        let mut r = node("alpha", &["alpha", "beta", "gamma"]);
        r.role = Role::Leader;
        r.current_term = 1;
        r.leader_id = Some("alpha".into());
        r.log.push(LogEntry {
            term: 1,
            command: "x".into(),
        });
        r.match_index.insert("alpha".into(), 1);
        r.match_index.insert("beta".into(), 1);
        r.match_index.insert("gamma".into(), 0);
        r.advance_commit();
        assert_eq!(r.commit_index, 1);
    }

    #[test]
    fn compact_rewrites_indices() {
        let mut r = node("alpha", &["alpha"]);
        r.current_term = 3;
        r.log.push(LogEntry { term: 3, command: "SET city hannover".into() });
        r.log.push(LogEntry { term: 3, command: "SET river leine".into() });
        r.commit_index = 2;
        r.last_applied = 2;
        assert_eq!(r.last_log_index(), 2);
        r.compact(2, 3);
        assert_eq!(r.last_included_index, 2);
        assert_eq!(r.last_included_term, 3);
        assert_eq!(r.last_log_index(), 2);
        assert_eq!(r.log.len(), 1);
        assert_eq!(r.term_at(2), Some(3));
        assert!(r.term_at(1).is_none());
    }

    #[test]
    fn heartbeat_requests_snapshot_when_next_behind_prefix() {
        let mut r = node("alpha", &["alpha", "beta"]);
        r.role = Role::Leader;
        r.leader_id = Some("alpha".into());
        r.current_term = 4;
        r.log.push(LogEntry { term: 4, command: "SET a 1".into() });
        r.log.push(LogEntry { term: 4, command: "SET b 2".into() });
        r.commit_index = 2;
        r.last_applied = 2;
        r.compact(2, 4);
        r.next_index.insert("beta".into(), 1);
        let act = r.heartbeat_one("beta").unwrap();
        match act {
            Action::Info(msg) if msg == "need-snapshot beta" => {}
            other => panic!("expected need-snapshot info, got {other:?}"),
        }
    }
}
