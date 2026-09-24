# Xanadu

**Decentralized Mesh Networking Core** — Gossip + Raft consensus

> Status: **v0.2**. Local / Docker only. No live overlay keys.

## Three-node mesh + Raft (Onyx)

Host port 9000 is Docker. Use 9100+.

Stop the old binaries first (`kill` the three `xanadu` PIDs), then:

```bash
cd ~/xanadu && git pull

# Terminal 1
cargo run -- --listen 0.0.0.0:9100 --name alpha --cluster alpha,beta,gamma

# Terminal 2
cargo run -- --listen 0.0.0.0:9101 --name beta --cluster alpha,beta,gamma \
  --peer 127.0.0.1:9100

# Terminal 3
cargo run -- --listen 0.0.0.0:9102 --name gamma --cluster alpha,beta,gamma \
  --peer 127.0.0.1:9100 --peer 127.0.0.1:9101
```

In any terminal:

```text
/raft
/peers
/commit hello-from-leader
```

`/commit` is accepted only on the leader. Followers log `commit rejected: not leader`. After quorum (2 of 3) every node prints `raft committed:`.

Raft state is stored under `.xanadu/<name>.raft.json`.

## Gossip

A line that is not a `/command` still floods as before.

## Protocol

First line: `XANADU/0.1`  
JSON lines: `hello` / `welcome` / `gossip` / `raft`

Raft RPCs: RequestVote, RequestVoteResp, AppendEntries, AppendEntriesResp.
This is a teaching implementation (in-process ticks, file persist, no membership change protocol, no pre-vote, no snapshotting).

## Tests

```bash
cargo test
```
