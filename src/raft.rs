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
}

impl Raft {
    pub fn new(id: String, cluster: Vec<String>, data_dir: &Path) -> Self {
        let mut cluster = cluster;
        if !cluster.iter().any(|n| n == &id) {
            cluster.push(id.clone());
        }
        cluster.sort();
        cluster.dedup();
        let data_path = data_dir.join(format!("{id}.raft.json"));
        let persist = load(&data_path);
        let mut raft = Self {
            id,
            cluster,
            role: Role::Follower,
            current_term: persist.as_ref().map(|p| p.current_term).unwrap_or(0),
            voted_for: persist.as_ref().and_then(|p| p.voted_for.clone()),
            log: persist
                .map(|p| p.log)
                .unwrap_or_else(|| vec![LogEntry {
                    term: 0,
                    command: String::new(),
                }]),
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            election_deadline: Instant::now() + jitter_election(),
            heartbeat_due: Instant::now(),
            data_path,
        };
        if raft.log.is_empty() {
            raft.log.push(LogEntry {
                term: 0,
                command: String::new(),
            });
        }
        raft
    }

    pub fn status(&self) -> String {
        format!(
            "role={:?} term={} leader={} commit={} last={} log={} cluster={:?} voted_for={:?}",
            self.role,
            self.current_term,
            self.leader_id.as_deref().unwrap_or("-"),
            self.commit_index,
            self.last_log_index(),
            self.log.len().saturating_sub(1),
            self.cluster,
            self.voted_for,
        )
    }

    pub fn last_log_index(&self) -> u64 {
        (self.log.len() as u64).saturating_sub(1)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn majority(&self) -> usize {
        self.cluster.len() / 2 + 1
    }

    pub fn tick(&mut self, now: Instant) -> Vec<Action> {
        let mut out = Vec::new();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_due {
                    self.heartbeat_due = now + Duration::from_millis(80);
                    out.extend(self.heartbeat_all());
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    out.extend(self.start_election(now));
                }
            }
        }
        out.extend(self.apply_committed());
        out
    }

    pub fn submit(&mut self, command: String) -> Result<Vec<Action>, String> {
        if self.role != Role::Leader {
            return Err(format!(
                "not leader (leader={})",
                self.leader_id.as_deref().unwrap_or("?")
            ));
        }
        self.log.push(LogEntry {
            term: self.current_term,
            command,
        });
        self.persist();
        let match_self = self.last_log_index();
        self.match_index.insert(self.id.clone(), match_self);
        Ok(self.heartbeat_all())
    }

    pub fn on_rpc(&mut self, rpc: Rpc, now: Instant) -> Vec<Action> {
        match rpc {
            Rpc::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(term, candidate_id, last_log_index, last_log_term, now),
            Rpc::RequestVoteResp {
                term,
                vote_granted,
                voter,
            } => self.on_vote_resp(term, vote_granted, voter, now),
            Rpc::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                now,
            ),
            Rpc::AppendEntriesResp {
                term,
                success,
                match_index,
                follower,
            } => self.on_append_resp(term, success, match_index, follower, now),
        }
    }

    fn start_election(&mut self, now: Instant) -> Vec<Action> {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.election_deadline = now + jitter_election();
        self.persist();
        info!(term = self.current_term, "raft election start");
        let mut out = vec![Action::Info(format!(
            "election term {}",
            self.current_term
        ))];
        if self.votes.len() >= self.majority() {
            out.extend(self.become_leader());
            return out;
        }
        out.push(Action::Send {
            to: None,
            rpc: Rpc::RequestVote {
                term: self.current_term,
                candidate_id: self.id.clone(),
                last_log_index: self.last_log_index(),
                last_log_term: self.last_log_term(),
            },
        });
        out
    }

    fn become_leader(&mut self) -> Vec<Action> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        self.heartbeat_due = Instant::now();
        let next = self.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for peer in self.others() {
            self.next_index.insert(peer.clone(), next);
            self.match_index.insert(peer, 0);
        }
        self.match_index.insert(self.id.clone(), self.last_log_index());
        info!(term = self.current_term, "raft became leader");
        let mut out = vec![Action::Info(format!("leader term {}", self.current_term))];
        out.extend(self.heartbeat_all());
        out
    }

    fn step_down(&mut self, term: u64, now: Instant) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes.clear();
        self.election_deadline = now + jitter_election();
        self.persist();
    }

    fn on_request_vote(
        &mut self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        now: Instant,
    ) -> Vec<Action> {
        if !self.in_cluster(&candidate_id) {
            return vec![];
        }
        if term > self.current_term {
            self.step_down(term, now);
        }
        let log_ok = last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index());
        let can_vote = self.voted_for.is_none() || self.voted_for.as_deref() == Some(&candidate_id);
        let granted = term >= self.current_term && can_vote && log_ok;
        if granted {
            self.voted_for = Some(candidate_id.clone());
            self.election_deadline = now + jitter_election();
            self.persist();
        }
        vec![Action::Send {
            to: Some(candidate_id),
            rpc: Rpc::RequestVoteResp {
                term: self.current_term,
                vote_granted: granted,
                voter: self.id.clone(),
            },
        }]
    }

    fn on_vote_resp(
        &mut self,
        term: u64,
        vote_granted: bool,
        voter: String,
        now: Instant,
    ) -> Vec<Action> {
        if term > self.current_term {
            self.step_down(term, now);
            return vec![];
        }
        if self.role != Role::Candidate || term != self.current_term {
            return vec![];
        }
        if vote_granted {
            self.votes.insert(voter);
        }
        if self.votes.len() >= self.majority() {
            self.become_leader()
        } else {
            vec![]
        }
    }

    fn on_append(
        &mut self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        now: Instant,
    ) -> Vec<Action> {
        if !self.in_cluster(&leader_id) {
            return vec![];
        }
        if term < self.current_term {
            return vec![Action::Send {
                to: Some(leader_id),
                rpc: Rpc::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    follower: self.id.clone(),
                },
            }];
        }
        if term > self.current_term || self.role != Role::Follower {
            self.step_down(term, now);
        }
        self.leader_id = Some(leader_id.clone());
        self.election_deadline = now + jitter_election();

        let prev = prev_log_index as usize;
        let match_ok = self.log.get(prev).map(|e| e.term) == Some(prev_log_term);
        if !match_ok {
            return vec![Action::Send {
                to: Some(leader_id),
                rpc: Rpc::AppendEntriesResp {
                    term: self.current_term,
                    success: false,
                    match_index: self.last_log_index().min(prev_log_index.saturating_sub(1)),
                    follower: self.id.clone(),
                },
            }];
        }
        if !entries.is_empty() {
            self.log.truncate(prev + 1);
            self.log.extend(entries);
            self.persist();
        }
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
        }
        let mut out = vec![Action::Send {
            to: Some(leader_id),
            rpc: Rpc::AppendEntriesResp {
                term: self.current_term,
                success: true,
                match_index: self.last_log_index(),
                follower: self.id.clone(),
            },
        }];
        out.extend(self.apply_committed());
        out
    }

    fn on_append_resp(
        &mut self,
        term: u64,
        success: bool,
        match_index: u64,
        follower: String,
        now: Instant,
    ) -> Vec<Action> {
        if term > self.current_term {
            self.step_down(term, now);
            return vec![];
        }
        if self.role != Role::Leader || term != self.current_term {
            return vec![];
        }
        if success {
            let next = match_index + 1;
            self.next_index.insert(follower.clone(), next);
            self.match_index.insert(follower, match_index);
            self.advance_commit();
            self.apply_committed()
        } else {
            let cur = self.next_index.get(&follower).copied().unwrap_or(1);
            self.next_index
                .insert(follower.clone(), cur.saturating_sub(1).max(1));
            self.heartbeat_one(&follower).into_iter().collect()
        }
    }

    fn advance_commit(&mut self) {
        let last = self.last_log_index();
        for n in (self.commit_index + 1..=last).rev() {
            if self.log[n as usize].term != self.current_term {
                continue;
            }
            let replicas = self
                .cluster
                .iter()
                .filter(|id| self.match_index.get(*id).copied().unwrap_or(0) >= n)
                .count();
            if replicas >= self.majority() {
                self.commit_index = n;
                break;
            }
        }
    }

    fn apply_committed(&mut self) -> Vec<Action> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let cmd = self.log[self.last_applied as usize].command.clone();
            if !cmd.is_empty() {
                out.push(Action::Apply {
                    index: self.last_applied,
                    command: cmd,
                });
            }
        }
        out
    }

    fn heartbeat_all(&self) -> Vec<Action> {
        self.others()
            .into_iter()
            .filter_map(|peer| self.heartbeat_one(&peer))
            .collect()
    }

    fn heartbeat_one(&self, peer: &str) -> Option<Action> {
        let next = self.next_index.get(peer).copied().unwrap_or(self.last_log_index() + 1);
        let prev = next.saturating_sub(1);
        let prev_term = self.log.get(prev as usize).map(|e| e.term).unwrap_or(0);
        let entries = if (next as usize) < self.log.len() {
            self.log[next as usize..].to_vec()
        } else {
            Vec::new()
        };
        Some(Action::Send {
            to: Some(peer.to_string()),
            rpc: Rpc::AppendEntries {
                term: self.current_term,
                leader_id: self.id.clone(),
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
            },
        })
    }

    fn others(&self) -> Vec<String> {
        self.cluster
            .iter()
            .filter(|n| *n != &self.id)
            .cloned()
            .collect()
    }

    fn in_cluster(&self, id: &str) -> bool {
        self.cluster.iter().any(|n| n == id)
    }

    fn persist(&self) {
        let snap = Persist {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            log: self.log.clone(),
        };
        if let Some(dir) = self.data_path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&snap) {
            let _ = std::fs::write(&self.data_path, bytes);
        }
    }
}

fn load(path: &Path) -> Option<Persist> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn jitter_election() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(1);
    Duration::from_millis(350 + (nanos % 350) as u64)
}

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
        let now = Instant::now() + Duration::from_secs(2);
        r.tick(now);
        assert_eq!(r.role, Role::Leader);
        assert_eq!(r.current_term, 1);
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
}
