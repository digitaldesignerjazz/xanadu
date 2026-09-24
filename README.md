# Xanadu

**Decentralized Mesh Networking Core** — Messages, Broadcasts, and Secure File Transfer

> Status: **v0.1 live in-repo**. Gossip mesh with TTL + dedup. No live keys. Local / Docker only.

Xanadu is the mesh core for the Esslinger & Co. / Nexus lineage. v0.1 ships a working TCP gossip node: handshake, flood broadcast with message-id deduplication and TTL, stdin publish, Docker three-node harness.

This is not a live overlay on the Hannover host. Overlay binaries (Tailscale / NetBird / Yggdrasil / Docker on that host) remain a separate activation step.

## Quick start (local)

```bash
git clone https://github.com/digitaldesignerjazz/xanadu.git
cd xanadu

# Node A
cargo run -- --listen 0.0.0.0:9000 --name alpha

# Node B (second terminal)
cargo run -- --listen 0.0.0.0:9001 --name beta --peer 127.0.0.1:9000

# Node C
cargo run -- --listen 0.0.0.0:9002 --name gamma --peer 127.0.0.1:9000
```

Type a line and press Enter. It floods to all connected peers. `/peers` prints the live peer count.

## Quick start (Docker harness)

```bash
docker compose up --build
```

Three nodes: `alpha` (bootstrap), `beta` and `gamma` peer to alpha. Attach to a container and type:

```bash
docker compose exec alpha sh -c 'echo hello-from-alpha'
```

Stdin attach works if you run a node without compose detach, or use `cargo run` locally.

## Protocol (v0.1)

JSON lines over TCP:

- `hello` / `welcome` — node_id + name
- `gossip` — `{id, origin, ttl, body}`

Fresh ids are remembered (cap 4096). TTL decrements on forward. No encryption yet (roadmap v0.2+). Do not put secrets on this plane.

## Roadmap

- [x] v0.1 Foundational messaging + gossip + Docker harness
- [ ] v0.2 Secure file transfer + Noise/TLS
- [ ] v0.3 Yggdrasil / custom overlay + discovery
- [ ] v0.4 AI agent hooks
- [ ] v0.5 QNET coordination hooks

## Related

- [Nexus](https://github.com/digitaldesignerjazz/nexus)
- [xnet-mesh](https://github.com/digitaldesignerjazz/xnet-mesh)

**X**: [@SirLancelotEsq](https://x.com/SirLancelotEsq)

MIT License.
