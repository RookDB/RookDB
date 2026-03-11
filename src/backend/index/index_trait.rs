use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordId {
    pub page_no: u32,
    pub item_id: u32,
}

impl RecordId {
    pub fn new(page_no: u32, item_id: u32) -> Self {
        Self { page_no, item_id }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexKey {
    Int(i64),
    Float(f64),
    Text(String),
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Text(a), Self::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for IndexKey {}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            _ => self.discriminant().cmp(&other.discriminant()),
        }
    }
}

impl Hash for IndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.discriminant().hash(state);
        match self {
            Self::Int(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
            Self::Text(v) => v.hash(state),
        }
    }
}

impl IndexKey {
    fn discriminant(&self) -> u8 {
        match self {
            Self::Int(_) => 0,
            Self::Float(_) => 1,
            Self::Text(_) => 2,
        }
    }

    pub fn hash_code(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }

    /// Byte representation preserving sort order for radix/trie index.
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::Int(v) => {
                let u = (*v as u64) ^ (1u64 << 63);
                u.to_be_bytes().to_vec()
            }
            Self::Float(v) => {
                let bits = v.to_bits();
                let u = if bits >> 63 == 0 {
                    bits | (1u64 << 63)
                } else {
                    !bits
                };
                u.to_be_bytes().to_vec()
            }
            Self::Text(s) => s.as_bytes().to_vec(),
        }
    }
}

impl std::fmt::Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
            Self::Text(v) => write!(f, "{}", v),
        }
    }
}

pub trait IndexTrait {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()>;
    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>>;
    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool>;
    fn save(&self, path: &str) -> io::Result<()>;
    fn entry_count(&self) -> usize;
    fn index_type_name(&self) -> &'static str;
}

pub trait TreeBasedIndex: IndexTrait {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>>;
    fn min_key(&self) -> Option<IndexKey>;
    fn max_key(&self) -> Option<IndexKey>;
}

pub trait HashBasedIndex: IndexTrait {
    fn load_factor(&self) -> f64;
    fn bucket_count(&self) -> usize;
}
