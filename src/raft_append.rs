impl Raft {
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

        if prev_log_index < self.last_included_index {
            return vec![Action::Info(format!("need-snapshot {leader_id}"))];
        }
        let match_ok = self.term_at(prev_log_index) == Some(prev_log_term);
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
            if let Some(phys) = self.phys(prev_log_index) {
                self.log.truncate(phys + 1);
                self.log.extend(entries);
                self.persist();
            }
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
            if self.term_at(n) != Some(self.current_term) {
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
            if self.last_applied <= self.last_included_index {
                continue;
            }
            let cmd = match self.phys(self.last_applied).and_then(|i| self.log.get(i)) {
                Some(e) => e.command.clone(),
                None => continue,
            };
            if !cmd.is_empty() {
                out.push(Action::Apply {
                    index: self.last_applied,
                    command: cmd,
                });
            }
        }
        out
    }
}
