# Contributing to Xanadu

Thank you for helping build **Xanadu** — the decentralized mesh networking core powering resilient messaging, efficient broadcasts, and secure file transfer for the Esslinger & Co. ecosystem.

Xanadu emphasizes privacy-first design, partition tolerance, and clean extensibility for integration with Nexus (orchestration), QNET blockchain, AI agent swarms, and creative/immersive applications.

## Code of Conduct

Inclusive, respectful, and constructive collaboration. We value contributions from networking engineers, privacy advocates, protocol designers, Rust/Python developers, and those exploring creative or immersive uses of decentralized mesh (roleplay worlds, agent-driven narratives, artistic installations).

## How to Contribute

### Issues
- Use clear labels: `mesh-core`, `messaging`, `broadcast`, `file-transfer`, `privacy`, `yggdrasil`, `docker`, `integration-nexus`, `ai-routing`, `creative`
- Provide environment details (OS, Docker/Yggdrasil version, Rust/Python), reproduction steps, logs, and diagrams where helpful.
- Security or privacy-sensitive issues: contact privately first.

### Pull Requests
1. Branch from `main` (feature/ prefix recommended).
2. Follow code style (rustfmt + clippy for Rust; black + type hints for Python).
3. Include tests for new protocol logic, especially edge cases (network partitions, message ordering, broadcast storms, resumable transfers under flaky links).
4. Update documentation (README, future PROTOCOL.md or ARCHITECTURE.md).
5. Commit messages: imperative, scoped (e.g., "feat(messaging): add encrypted broadcast primitive with replay protection").

### Development Environment
- **Rust core**: `cargo build`, `cargo test`, `cargo clippy`
- **Python tooling/agents**: venv + requirements (to be added)
- **Docker mesh simulation**: `docker compose` for multi-node test networks
- **Yggdrasil / Tenda Nova**: Local or containerized instances for integration testing
- **Privacy note**: Never commit private keys, .env files, or sensitive peer configs. Use example templates.

### Focus Areas

**Core Mesh Protocol**
- Messaging primitives (reliable/unreliable, ordered, pub/sub)
- Broadcast/gossip mechanisms (efficient flooding, topic routing, presence)
- File transfer (chunking, Merkle verification, resumability, encryption layers)
- Peer discovery, NAT traversal, and link management

**Privacy & Security**
- Tor/I2P pluggable transports
- End-to-end encryption (Noise, MLS, or custom)
- Capability-based access control and replay protection

**Integration & Extensibility**
- Clean APIs and event buses for Nexus orchestration
- Hooks for AI agent swarms (intelligent routing, predictive network healing, swarm file distribution)
- QNET/XCoin blockchain hooks (incentives, consensus-assisted coordination, on-mesh state)
- Hardware prototype support (embedded targets, monitoring via Grok Launcher)

**Creative & Immersive Extensions**
- Mesh as substrate for immersive storytelling, roleplay environments, or agent-coordinated narrative worlds
- Integration points for Suno-generated audio or other media streamed over mesh
- Fantasy/cyberpunk themed protocol extensions or demo scenarios (optional but celebrated)

**Testing & Tooling**
- Comprehensive simulation harnesses (Docker-based multi-node, chaos testing for partitions)
- Performance benchmarks (latency, throughput, broadcast efficiency under load)
- Monitoring and visualization dashboards

## Style & Quality
- Prioritize clarity, modularity, security, and resilience.
- Document all edge cases thoroughly (partial connectivity, malicious peers, resource exhaustion).
- Write tests that simulate real-world mesh conditions.
- Keep the core lean; push complex features (AI, blockchain) to integration layers or separate crates/modules.

## Recognition
Significant contributors acknowledged in releases and a future `CONTRIBUTORS.md`. Opportunities for deeper collaboration on Esslinger & Co. initiatives.

Questions? Open an issue or connect via X (@SirLancelotEsq).

Let's build the resilient, private, and creatively empowering mesh foundation together.