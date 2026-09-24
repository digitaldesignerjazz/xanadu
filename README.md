# Xanadu

**Decentralized Mesh Networking Core** — Messages, Broadcasts, and Secure File Transfer

> Status: **v0.1.1**. Gossip mesh with TTL + dedup + `XANADU/0.1` handshake. Local / Docker only. No live keys.

## Quick start

Use free ports. **Host 9000 is often Docker.** Default listen is `0.0.0.0:9100`.

```bash
# Terminal 1 — bootstrap
cargo run -- --listen 0.0.0.0:9100 --name alpha

# Terminal 2
cargo run -- --listen 0.0.0.0:9101 --name beta --peer 127.0.0.1:9100

# Terminal 3 — alpha AND beta as peers
cargo run -- --listen 0.0.0.0:9102 --name gamma \
  --peer 127.0.0.1:9100 --peer 127.0.0.1:9101
```

`--peer` is repeatable. On a running node (after `git pull`):

```text
/peer 127.0.0.1:9101
/peers
```

Type a line and press Enter to flood. You should see `hello` / `welcome`, not `bad frame`.

```bash
ss -ltnp | grep -E '9100|9000'
```

## Docker

```bash
docker compose up --build
```

Compose maps 9100, not host 9000.

## Protocol

First line: `XANADU/0.1`
Then JSON lines: `hello` / `welcome` / `gossip`.

## Related

- [Nexus](https://github.com/digitaldesignerjazz/nexus)
- [xnet-mesh](https://github.com/digitaldesignerjazz/xnet-mesh)
