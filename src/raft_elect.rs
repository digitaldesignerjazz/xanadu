impl Raft {
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
}
