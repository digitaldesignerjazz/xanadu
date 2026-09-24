impl Raft {
    fn heartbeat_all(&self) -> Vec<Action> {
        self.others()
            .into_iter()
            .filter_map(|peer| self.heartbeat_one(&peer))
            .collect()
    }

    fn heartbeat_one(&self, peer: &str) -> Option<Action> {
        let next = self
            .next_index
            .get(peer)
            .copied()
            .unwrap_or(self.last_log_index() + 1);
        if next <= self.last_included_index {
            return Some(Action::Info(format!("need-snapshot {peer}")));
        }
        let prev = next.saturating_sub(1);
        let prev_term = self.term_at(prev).unwrap_or(self.last_included_term);
        let entries = match self.phys(next) {
            Some(i) if i < self.log.len() => self.log[i..].to_vec(),
            _ => Vec::new(),
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

    fn on_install_snapshot(
        &mut self,
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        done: bool,
        now: Instant,
    ) -> Vec<Action> {
        if !self.in_cluster(&leader_id) {
            return vec![];
        }
        if term < self.current_term {
            return vec![Action::Send {
                to: Some(leader_id),
                rpc: Rpc::InstallSnapshotResp {
                    term: self.current_term,
                    success: false,
                    offset,
                    follower: self.id.clone(),
                },
            }];
        }
        if term > self.current_term {
            self.step_down(term, now);
        }
        self.leader_id = Some(leader_id.clone());
        self.election_deadline = now + jitter_election();
        if done {
            self.install_snapshot(last_included_index, last_included_term);
        }
        vec![Action::Send {
            to: Some(leader_id),
            rpc: Rpc::InstallSnapshotResp {
                term: self.current_term,
                success: true,
                offset,
                follower: self.id.clone(),
            },
        }]
    }

    fn on_install_snapshot_resp(
        &mut self,
        term: u64,
        success: bool,
        offset: u64,
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
            let next = self.last_included_index.max(self.last_log_index()) + 1;
            self.next_index.insert(follower.clone(), next);
            self.match_index
                .insert(follower.clone(), self.last_included_index);
            self.advance_commit();
            vec![Action::Info(format!(
                "snapshot ack {follower} offset={offset}"
            ))]
        } else {
            vec![Action::Info(format!("need-snapshot {follower}"))]
        }
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
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
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
    Duration::from_millis(1500 + (nanos % 1500) as u64)
}
