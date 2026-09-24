mod raft;

use anyhow::{bail, Result};
use clap::Parser;
use raft::{Action, Raft, Rpc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

const MAX_SEEN: usize = 4096;
const DEFAULT_TTL: u8 = 8;
const PROTO: &str = "XANADU/0.1";

#[derive(Parser, Debug)]
#[command(name = "xanadu", about = "Xanadu mesh — gossip + Raft")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9100")]
    listen: SocketAddr,
    #[arg(long)]
    peer: Vec<SocketAddr>,
    #[arg(long)]
    name: Option<String>,
    /// Cluster member names for Raft quorum, e.g. alpha,beta,gamma
    #[arg(long, value_delimiter = ',')]
    cluster: Vec<String>,
    #[arg(long, default_value = ".xanadu")]
    data: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Wire {
    Hello { node_id: String, name: String },
    Welcome { node_id: String, name: String },
    Gossip { msg: GossipMsg },
    Raft { rpc: Rpc },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GossipMsg {
    id: String,
    origin: String,
    ttl: u8,
    body: String,
}

struct PeerLink {
    name: String,
    tx: broadcast::Sender<String>,
}

struct State {
    node_id: String,
    name: String,
    seen: HashSet<String>,
    peers: HashMap<String, PeerLink>,
    by_name: HashMap<String, String>,
    raft: Raft,
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
    std::fs::create_dir_all(&args.data)?;
    let raft = Raft::new(name.clone(), args.cluster.clone(), &args.data);

    let state = Arc::new(Mutex::new(State {
        node_id: node_id.clone(),
        name: name.clone(),
        seen: HashSet::new(),
        peers: HashMap::new(),
        by_name: HashMap::new(),
        raft,
    }));

    info!(%name, %node_id, listen = %args.listen, cluster = ?args.cluster, "xanadu v0.2 starting");

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
        spawn_dialer(state.clone(), peer);
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tick.tick().await;
                let actions = {
                    let mut st = state.lock().await;
                    let live: Vec<String> = st.by_name.keys().cloned().collect();
                    st.raft.tick(Instant::now(), &live)
                };
                dispatch(&state, actions).await;
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
            info!(count = st.peers.len(), "connected peers");
            for (id, link) in &st.peers {
                info!(%id, name = %link.name, "peer");
            }
            continue;
        }
        if line == "/raft" {
            let st = state.lock().await;
            info!("{}", st.raft.status());
            continue;
        }
        if let Some(rest) = line.strip_prefix("/peer ") {
            match rest.trim().parse::<SocketAddr>() {
                Ok(addr) => {
                    info!(%addr, "adding outbound peer");
                    spawn_dialer(state.clone(), addr);
                }
                Err(e) => warn!("usage: /peer 127.0.0.1:9101 ({e})"),
            }
            continue;
        }
        if let Some(cmd) = line.strip_prefix("/commit ") {
            let actions = {
                let mut st = state.lock().await;
                match st.raft.submit(cmd.to_string()) {
                    Ok(a) => a,
                    Err(e) => {
                        warn!("commit rejected: {e}");
                        Vec::new()
                    }
                }
            };
            dispatch(&state, actions).await;
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

fn spawn_dialer(state: Arc<Mutex<State>>, peer: SocketAddr) {
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(2);
        loop {
            match TcpStream::connect(peer).await {
                Ok(stream) => match handle_conn(state.clone(), stream, peer, true).await {
                    Ok(()) => delay = Duration::from_secs(2),
                    Err(e) => {
                        warn!(%peer, "outbound closed: {e:#}");
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                },
                Err(e) => {
                    warn!(%peer, "connect failed: {e:#}");
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
            tokio::time::sleep(delay).await;
        }
    });
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

    let (tx, mut rx) = broadcast::channel::<String>(256);
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
                            st.by_name.insert(name.clone(), node_id.clone());
                            st.peers.insert(node_id, PeerLink { name, tx: tx.clone() });
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
                        st.by_name.insert(name.clone(), node_id.clone());
                        st.peers.insert(node_id, PeerLink { name, tx: tx.clone() });
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
                    Wire::Raft { rpc } => {
                        let actions = {
                            let mut st = state.lock().await;
                            st.raft.on_rpc(rpc, Instant::now())
                        };
                        dispatch(&state, actions).await;
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
        if let Some(link) = st.peers.remove(&id) {
            st.by_name.remove(&link.name);
        }
    }
    Ok(())
}

async fn dispatch(state: &Arc<Mutex<State>>, actions: Vec<Action>) {
    for action in actions {
        match action {
            Action::Info(msg) => info!(raft = %msg, "raft"),
            Action::Apply { index, command } => {
                info!(index, "raft committed: {command}");
            }
            Action::Send { to, rpc } => {
                let frame = match serde_json::to_string(&Wire::Raft { rpc }) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let st = state.lock().await;
                match to {
                    None => {
                        for link in st.peers.values() {
                            let _ = link.tx.send(frame.clone());
                        }
                    }
                    Some(name) => {
                        if let Some(id) = st.by_name.get(&name) {
                            if let Some(link) = st.peers.get(id) {
                                let _ = link.tx.send(frame);
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn flood(state: &Arc<Mutex<State>>, msg: &GossipMsg, except: Option<&str>) {
    let frame = match serde_json::to_string(&Wire::Gossip { msg: msg.clone() }) {
        Ok(s) => s,
        Err(_) => return,
    };
    let st = state.lock().await;
    for (id, link) in &st.peers {
        if except.is_some_and(|ex| ex == id) {
            continue;
        }
        let _ = link.tx.send(frame.clone());
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, frame: &Wire) -> Result<()> {
    let line = serde_json::to_string(frame)?;
    w.write_all(line.as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}
