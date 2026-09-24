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
    NeedSnapshot { to: String },
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
