# Xanadu

**Decentralized Mesh Networking Core** — Messages, Broadcasts, and Secure File Transfer

> Status: **v0.1.1**. Gossip mesh with TTL + dedup + `XANADU/0.1` handshake. Local / Docker only. No live keys.

## Quick start

Use free ports. **9000 is often already taken** (HTTP, PHP-FPM, other prototypes). Default listen is now `0.0.0.0:9100`.

```bash
git pull

# Terminal 1 — bootstrap
cargo run -- --listen 0.0.0.0:9100 --name alpha

# Terminal 2
cargo run -- --listen 0.0.0.0:9101 --name beta --peer 127.0.0.1:9100
```

You should see `hello` / `welcome`, not `bad frame`. Type a line, press Enter. `/peers` prints the live count.

If a peer answers with anything other than `XANADU/0.1`, the node closes and logs the first line so you can see what actually owns that port:

```bash
ss -ltnp | grep -E '9100|9000'
```

## Docker

```bash
docker compose up --build
```

## Protocol

First line: `XANADU/0.1`
Then JSON lines: `hello` / `welcome` / `gossip`.

## Related

- [Nexus](https://github.com/digitaldesignerjazz/nexus)
- [xnet-mesh](https://github.com/digitaldesignerjazz/xnet-mesh)
