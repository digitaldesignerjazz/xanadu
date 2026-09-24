# Xanadu

**Gossip + Raft + persistent KV + InstallSnapshot + log prefix compaction**

> Status: **v0.5**. Local / Docker only. No live overlay keys.

## Cluster

Port 9000 is Docker. Use 9100+. Three separate terminals. Commands go into the running process, not bash.

```bash
pkill -x xanadu
cd ~/xanadu && git pull

cargo run -- --listen 0.0.0.0:9100 --name alpha --cluster alpha,beta,gamma
cargo run -- --listen 0.0.0.0:9101 --name beta --cluster alpha,beta,gamma --peer 127.0.0.1:9100
cargo run -- --listen 0.0.0.0:9102 --name gamma --cluster alpha,beta,gamma --peer 127.0.0.1:9100 --peer 127.0.0.1:9101
```

```text
/raft
/set city hannover
/get city
/kv
/snapshot
```

`/snapshot` on the leader writes `.xanadu/<name>.kv.json`, calls `Raft::compact` so `last_included_index` moves forward, and pushes chunked InstallSnapshot as both `Wire::Raft { rpc: InstallSnapshot }` and legacy `Wire::Snapshot` frames (256 B, sha256). Followers install the KV map **and** compact their log. A joining peer gets catch-up if the leader already has applied state. A follower whose `nextIndex` sits behind `lastIncludedIndex` triggers a real snapshot push (`need-snapshot`), not only a log line. `/raft` then shows `snap=` as the compacted prefix.

Operator must restart all three nodes after `git pull`.
