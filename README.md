# Xanadu

**Gossip + Raft + persistent KV + InstallSnapshot**

> Status: **v0.4**. Local / Docker only. No live overlay keys.

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

`/snapshot` on the leader writes `.xanadu/<name>.kv.json` and pushes chunked InstallSnapshot frames (256 B, sha256) to followers. A joining peer also gets catch-up if the leader already has applied state.

No log prefix compaction yet. No overlay.
