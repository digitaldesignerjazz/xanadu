use anyhow::{bail, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

const MAX_SEEN: usize = 4096;
const DEFAULT_TTL: u8 = 8;
const PROTO: &str = "XANADU/0.1";

#[derive(Parser, Debug)]
#[command(name = "xanadu", about = "Xanadu v0.1 mesh node — gossip broadcast")]
struct Args {
    /// Listen address, e.g. 0.0.0.0:9100
    #[arg(long, default_value = "0.0.0.0:9100")]
    listen: SocketAddr,

    /// Bootstrap peer addresses (repeatable)
    #[arg(long)]
    peer: Vec<SocketAddr>,

    /// Human-readable node name
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Wire {
    Hello { node_id: String, name: String },
    Welcome { node_id: String, name: String },
    Gossip { msg: GossipMsg },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GossipMsg {
    id: String,
    origin: String,
    ttl: u8,
    body: String,
}

struct State {
    node_id: String,
    name: String,
    seen: HashSet<String>,
    peers: HashMap<String, broadcast::Sender<String>>,
}

impl State {
    fn remember(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        if self.seen.len() >= MAX_SEEN {
            self.seen.clear();
        }
        self.seen.insert(id.to_string());
        true
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let node_id = Uuid::new_v4().to_string();
    let name = args
        .name
        .unwrap_or_else(|| format!("xanadu-{}", &node_id[..8]));

    let state = Arc::new(Mutex::new(State {
        node_id: node_id.clone(),
        name: name.clone(),
        seen: HashSet::new(),
        peers: HashMap::new(),
    }));

    info!(%name, %node_id, listen = %args.listen, "xanadu v0.1 starting");

    let listener = TcpListener::bind(args.listen).await?;
    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(state, stream, addr, false).await {
                                warn!(%addr, "inbound closed: {e:#}");
                            }
                        });
                    }
                    Err(e) => error!("accept: {e:#}"),
                }
            }
        });
    }

    for peer in args.peer {
        let state = state.clone();
        tokio::spawn(async move {
            let mut delay = Duration::from_secs(2);
            loop {
                match TcpStream::connect(peer).await {
                    Ok(stream) => {
                        match handle_conn(state.clone(), stream, peer, true).await {
                            Ok(()) => delay = Duration::from_secs(2),
                            Err(e) => {
                                warn!(%peer, "outbound closed: {e:#}");
                                delay = (delay * 2).min(Duration::from_secs(30));
                            }
                        }
                    }
                    Err(e) => {
                        warn!(%peer, "connect failed: {e:#}");
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
                tokio::time::sleep(delay).await;
            }
        });
    }

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = stdin.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if line == "/peers" {
            let st = state.lock().await;
            info!(peers = st.peers.len(), "connected peers");
            continue;
        }
        let gossip = {
            let mut st = state.lock().await;
            let msg = GossipMsg {
                id: Uuid::new_v4().to_string(),
                origin: st.node_id.clone(),
                ttl: DEFAULT_TTL,
                body: line,
            };
            st.remember(&msg.id);
            msg
        };
        info!(id = %gossip.id, "local broadcast: {}", gossip.body);
        flood(&state, &gossip, None).await;
    }
    Ok(())
}

async fn handle_conn(
    state: Arc<Mutex<State>>,
    stream: TcpStream,
    addr: SocketAddr,
    initiator: bool,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let (local_id, local_name) = {
        let st = state.lock().await;
        (st.node_id.clone(), st.name.clone())
    };

    writer.write_all(PROTO.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    if initiator {
        write_frame(
            &mut writer,
            &Wire::Hello {
                node_id: local_id.clone(),
                name: local_name.clone(),
            },
        )
        .await?;
    }

    let banner = lines.next_line().await?;
    match banner.as_deref() {
        Some(PROTO) => {}
        Some(other) => {
            let preview: String = other.chars().take(80).collect();
            bail!("peer is not Xanadu (first line: {preview:?}) — is another service bound on {addr}?");
        }
        None => bail!("peer closed before handshake"),
    }

    let (tx, mut rx) = broadcast::channel::<String>(64);
    let mut peer_id: Option<String> = None;

    if !initiator {
        write_frame(
            &mut writer,
            &Wire::Hello {
                node_id: local_id.clone(),
                name: local_name.clone(),
            },
        )
        .await?;
    }

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { break; };
                let frame = match serde_json::from_str::<Wire>(&line) {
                    Ok(frame) => frame,
                    Err(_) => {
                        let preview: String = line.chars().take(80).collect();
                        bail!("bad frame from {addr}: {preview:?}");
                    }
                };
                match frame {
                    Wire::Hello { node_id, name } => {
                        info!(%addr, %name, %node_id, "hello");
                        peer_id = Some(node_id.clone());
                        {
                            let mut st = state.lock().await;
                            st.peers.insert(node_id, tx.clone());
                        }
                        write_frame(
                            &mut writer,
                            &Wire::Welcome {
                                node_id: local_id.clone(),
                                name: local_name.clone(),
                            },
                        ).await?;
                    }
                    Wire::Welcome { node_id, name } => {
                        info!(%addr, %name, %node_id, "welcome");
                        peer_id = Some(node_id.clone());
                        let mut st = state.lock().await;
                        st.peers.insert(node_id, tx.clone());
                    }
                    Wire::Gossip { msg } => {
                        let fresh = {
                            let mut st = state.lock().await;
                            st.remember(&msg.id)
                        };
                        if !fresh {
                            continue;
                        }
                        info!(from = %msg.origin, id = %msg.id, ttl = msg.ttl, "recv: {}", msg.body);
                        if msg.ttl > 1 {
                            let mut fwd = msg.clone();
                            fwd.ttl -= 1;
                            flood(&state, &fwd, peer_id.as_deref()).await;
                        }
                    }
                }
            }
            out = rx.recv() => {
                match out {
                    Ok(payload) => {
                        writer.write_all(payload.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }
    }

    if let Some(id) = peer_id {
        let mut st = state.lock().await;
        st.peers.remove(&id);
    }
    Ok(())
}

async fn flood(state: &Arc<Mutex<State>>, msg: &GossipMsg, except: Option<&str>) {
    let frame = match serde_json::to_string(&Wire::Gossip { msg: msg.clone() }) {
        Ok(s) => s,
        Err(_) => return,
    };
    let st = state.lock().await;
    for (id, tx) in &st.peers {
        if except.is_some_and(|ex| ex == id) {
            continue;
        }
        let _ = tx.send(frame.clone());
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, frame: &Wire) -> Result<()> {
    let line = serde_json::to_string(frame)?;
    w.write_all(line.as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}
