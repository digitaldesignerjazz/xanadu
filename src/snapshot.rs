use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub const CHUNK: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapBlob {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub map: HashMap<String, String>,
}

impl SnapBlob {
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

pub fn chunks(data: &[u8]) -> Vec<(u64, Vec<u8>, bool)> {
    if data.is_empty() {
        return vec![(0, Vec::new(), true)];
    }
    let total = data.len();
    let mut out = Vec::new();
    let mut off = 0usize;
    while off < total {
        let end = (off + CHUNK).min(total);
        let done = end == total;
        out.push((off as u64, data[off..end].to_vec(), done));
        off = end;
    }
    out
}

#[derive(Default)]
pub struct Assembler {
    pub expected_hash: Option<String>,
    pub last_included_index: u64,
    pub last_included_term: u64,
    buf: Vec<u8>,
    next_off: u64,
}

impl Assembler {
    pub fn push(
        &mut self,
        offset: u64,
        data: &[u8],
        done: bool,
        sha: &str,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<Option<SnapBlob>, String> {
        if offset == 0 {
            *self = Self {
                expected_hash: Some(sha.to_string()),
                last_included_index,
                last_included_term,
                buf: data.to_vec(),
                next_off: data.len() as u64,
            };
        } else {
            if offset != self.next_off {
                return Err(format!("offset skip want={} got={offset}", self.next_off));
            }
            self.buf.extend_from_slice(data);
            self.next_off += data.len() as u64;
        }
        if !done {
            return Ok(None);
        }
        let got = sha256_hex(&self.buf);
        let want = self.expected_hash.clone().unwrap_or_default();
        if !want.is_empty() && got != want {
            return Err(format!("sha256 mismatch"));
        }
        let blob = SnapBlob::decode(&self.buf).map_err(|e| e.to_string())?;
        Ok(Some(blob))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_one_chunk() {
        let blob = SnapBlob {
            last_included_index: 2,
            last_included_term: 233,
            map: HashMap::from([("city".into(), "hannover".into())]),
        };
        let raw = blob.encode().unwrap();
        let hash = sha256_hex(&raw);
        let mut a = Assembler::default();
        let mut got = None;
        for (off, part, done) in chunks(&raw) {
            got = a
                .push(off, &part, done, &hash, 2, 233)
                .unwrap();
        }
        let got = got.unwrap();
        assert_eq!(got.map.get("city").unwrap(), "hannover");
    }
}
