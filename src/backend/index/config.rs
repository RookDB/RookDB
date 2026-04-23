#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashIndexType {
    Static,
    Extendible,
    Linear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeIndexType {
    BTree,
    BPlusTree,
    RadixTree,
}

pub const DEFAULT_HASH_INDEX: HashIndexType = HashIndexType::Extendible;
pub const DEFAULT_TREE_INDEX: TreeIndexType = TreeIndexType::BPlusTree;

pub const STATIC_HASH_BUCKET_CAPACITY: usize = 8;
pub const STATIC_HASH_NUM_BUCKETS: usize = 64;
pub const EXTENDIBLE_HASH_BUCKET_CAPACITY: usize = 4;
pub const LINEAR_HASH_INITIAL_BUCKETS: usize = 4;
pub const LINEAR_HASH_LOAD_FACTOR_THRESHOLD: f64 = 0.75;
pub const BTREE_MIN_DEGREE: usize = 256;
