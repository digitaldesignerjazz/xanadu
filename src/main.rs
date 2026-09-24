mod kv;
mod raft;
mod snapshot;

use anyhow::{bail, Result};
use clap::Parser;
use kv::Kv;
use raft::{Action, Raft, Rpc, Role};
use serde::{Deserialize, Serialize};
use snapshot::{chunks, sha256_hex, Assembler, SnapBlob};
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
#[command(name = "xanadu", about = "Xanadu mesh — gossip + Raft + KV + snapshot")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9100")]
    listen: SocketAddr,
    #[arg(long)]
    peer: Vec<SocketAddr>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long, value_delimiter = ',')]
    cluster: Vec<String>,
    #[arg(long, default_value = ".xanadu")]
    data: PathBuf,
}
