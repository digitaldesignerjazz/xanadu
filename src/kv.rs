use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Snap {
    last_applied: u64,
    map: HashMap<String, String>,
}

pub struct Kv {
    map: HashMap<String, String>,
    pub last_applied: u64,
    path: PathBuf,
}

impl Kv {
    pub fn load(dir: &Path, name: &str) -> Self {
        let path = dir.join(format!("{name}.kv.json"));
        let snap: Snap = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        Self {
            map: snap.map,
            last_applied: snap.last_applied,
            path,
        }
    }

    pub fn apply(&mut self, index: u64, command: &str) {
        if index <= self.last_applied {
            return;
        }
        self.last_applied = index;
        if let Some((op, rest)) = split_cmd(command) {
            match op {
                "SET" => {
                    let Some((key, value)) = rest.split_once(char::is_whitespace) else {
                        return;
                    };
                    let key = key.trim();
                    let value = value.trim();
                    if key.is_empty() {
                        return;
                    }
                    self.map.insert(key.to_string(), value.to_string());
                    info!(index, key, value, "kv set");
                }
                "DEL" => {
                    let key = rest.trim();
                    if !key.is_empty() {
                        self.map.remove(key);
                        info!(index, key, "kv del");
                    }
                }
                _ => {}
            }
        }
        self.persist();
    }

    pub fn install(&mut self, index: u64, map: HashMap<String, String>) {
        self.map = map;
        self.last_applied = index;
        self.persist();
        info!(index, keys = self.map.len(), "kv installed from snapshot");
    }

    pub fn map_clone(&self) -> HashMap<String, String> {
        self.map.clone()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    pub fn dump(&self) -> Vec<(String, String)> {
        let mut rows: Vec<_> = self.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    pub fn snapshot(&mut self) {
        self.persist();
        info!(
            last_applied = self.last_applied,
            keys = self.map.len(),
            path = %self.path.display(),
            "kv snapshot written"
        );
    }

    fn persist(&self) {
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let snap = Snap {
            last_applied: self.last_applied,
            map: self.map.clone(),
        };
        if let Ok(bytes) = serde_json::to_vec_pretty(&snap) {
            let _ = std::fs::write(&self.path, bytes);
        }
    }
}

fn split_cmd(command: &str) -> Option<(&str, &str)> {
    let command = command.trim();
    let (op, rest) = command.split_once(char::is_whitespace)?;
    Some((op, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn set_get_del_roundtrip() {
        let dir = temp_dir().join(format!(
            "xanadu-kv-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut kv = Kv::load(&dir, "alpha");
        kv.apply(1, "SET city hannover");
        assert_eq!(kv.get("city"), Some("hannover"));
        kv.apply(2, "DEL city");
        assert_eq!(kv.get("city"), None);
        let loaded = Kv::load(&dir, "alpha");
        assert_eq!(loaded.last_applied, 2);
        assert!(loaded.get("city").is_none());
    }
}
