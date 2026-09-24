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
        let last_included_index = persist.as_ref().map(|p| p.last_included_index).unwrap_or(0);
        let last_included_term = persist.as_ref().map(|p| p.last_included_term).unwrap_or(0);
        let log = persist.as_ref().map(|p| p.log.clone()).unwrap_or_else(|| vec![LogEntry {
            term: last_included_term,
            command: String::new(),
        }]);
        let mut raft = Self {
            id,
            cluster,
            role: Role::Follower,
            current_term: persist.as_ref().map(|p| p.current_term).unwrap_or(0),
            voted_for: persist.as_ref().and_then(|p| p.voted_for.clone()),
            log,
            commit_index: last_included_index,
            last_applied: last_included_index,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            election_deadline: Instant::now() + jitter_election(),
            heartbeat_due: Instant::now(),
            data_path,
            last_included_index,
            last_included_term,
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
            "role={:?} term={} leader={} commit={} last={} log={} snap={} cluster={:?} voted_for={:?}",
            self.role,
            self.current_term,
            self.leader_id.as_deref().unwrap_or("-"),
            self.commit_index,
            self.last_log_index(),
            self.log.len().saturating_sub(1),
            self.last_included_index,
            self.cluster,
            self.voted_for,
        )
    }

    pub fn last_log_index(&self) -> u64 {
        self.last_included_index + (self.log.len() as u64).saturating_sub(1)
    }

    fn phys(&self, abs: u64) -> Option<usize> {
        if abs < self.last_included_index {
            return None;
        }
        Some((abs - self.last_included_index) as usize)
    }

    pub fn term_at(&self, abs: u64) -> Option<u64> {
        if abs == self.last_included_index {
            return Some(self.last_included_term);
        }
        let i = self.phys(abs)?;
        self.log.get(i).map(|e| e.term)
    }

    /// Discard the log prefix covered by a snapshot. Index 0 of `log` becomes
    /// the sentinel at `last_included_index`.
    pub fn compact(&mut self, last_included_index: u64, last_included_term: u64) {
        if last_included_index <= self.last_included_index {
            return;
        }
        if let Some(i) = self.phys(last_included_index) {
            if i < self.log.len() {
                self.log.drain(0..i);
            } else {
                self.log.clear();
            }
        } else {
            self.log.clear();
        }
        if self.log.is_empty() {
            self.log.push(LogEntry {
                term: last_included_term,
                command: String::new(),
            });
        } else {
            self.log[0] = LogEntry {
                term: last_included_term,
                command: String::new(),
            };
        }
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        if self.commit_index < last_included_index {
            self.commit_index = last_included_index;
        }
        if self.last_applied < last_included_index {
            self.last_applied = last_included_index;
        }
        self.persist();
        info!(
            last_included_index,
            last_included_term,
            remaining = self.log.len().saturating_sub(1),
            "raft log compacted"
        );
    }

    pub fn install_snapshot(&mut self, last_included_index: u64, last_included_term: u64) {
        self.compact(last_included_index, last_included_term);
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn majority(&self) -> usize {
        self.cluster.len() / 2 + 1
    }

    fn live_quorum(&self, live: &[String]) -> bool {
        let n = 1 + live
            .iter()
            .filter(|n| self.in_cluster(n) && *n != &self.id)
            .count();
        n >= self.majority()
    }

    pub fn tick(&mut self, now: Instant, live: &[String]) -> Vec<Action> {
        let mut out = Vec::new();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_due {
                    self.heartbeat_due = now + Duration::from_millis(150);
                    out.extend(self.heartbeat_all());
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    if self.live_quorum(live) {
                        out.extend(self.start_election(now));
                    } else {
                        self.role = Role::Follower;
                        self.election_deadline = now + jitter_election();
                    }
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
            Rpc::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                done,
                ..
            } => self.on_install_snapshot(
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                done,
                now,
            ),
            Rpc::InstallSnapshotResp {
                term,
                success,
                offset,
                follower,
            } => self.on_install_snapshot_resp(term, success, offset, follower, now),
        }
    }
}
