# Xanadu

**Decentralized Mesh Networking Core** — Gossip + Raft + persistent KV

> Status: **v0.3**. Local / Docker only. No live overlay keys.

## Running cluster (Onyx)

Port 9000 is Docker. Use 9100+. Three **separate** terminals. `pkill -x xanadu` before a version bump.

```bash
cd ~/xanadu && git pull

cargo run -- --listen 0.0.0.0:9100 --name alpha --cluster alpha,beta,gamma
cargo run -- --listen 0.0.0.0:9101 --name beta --cluster alpha,beta,gamma --peer 127.0.0.1:9100
cargo run -- --listen 0.0.0.0:9102 --name gamma --cluster alpha,beta,gamma --peer 127.0.0.1:9100 --peer 127.0.0.1:9101
```

Commands (type into the **running** process, not bash):

```text
/raft
/peers
/set city hannover
/get city
/kv
/del city
/snapshot
/commit note
```

`/set` and `/del` go through the leader log. Followers apply the same command. `/get` and `/kv` are local reads of the applied map. Snapshot file: `.xanadu/<name>.kv.json`.

No InstallSnapshot RPC yet. File chunks are next.
