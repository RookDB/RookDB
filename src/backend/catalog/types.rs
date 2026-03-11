use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexAlgorithm {
    StaticHash,
    ExtendibleHash,
    LinearHash,
    BTree,
    BPlusTree,
    RadixTree,
}

impl IndexAlgorithm {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::StaticHash => "Static Hash",
            Self::ExtendibleHash => "Extendible Hash",
            Self::LinearHash => "Linear Hash",
            Self::BTree => "B-Tree",
            Self::BPlusTree => "B+ Tree",
            Self::RadixTree => "Radix Tree",
        }
    }

    pub fn is_hash(&self) -> bool {
        matches!(self, Self::StaticHash | Self::ExtendibleHash | Self::LinearHash)
    }

    pub fn is_tree(&self) -> bool {
        !self.is_hash()
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "static_hash" | "static" => Some(Self::StaticHash),
            "extendible_hash" | "extendible" => Some(Self::ExtendibleHash),
            "linear_hash" | "linear" => Some(Self::LinearHash),
            "btree" | "b_tree" => Some(Self::BTree),
            "bplus_tree" | "b+tree" | "bplustree" => Some(Self::BPlusTree),
            "radix_tree" | "radix" => Some(Self::RadixTree),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexEntry {
    pub index_name: String,
    pub column_name: String,
    pub algorithm: IndexAlgorithm,
}

#[derive(Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
    #[serde(default)]
    pub indexes: Vec<IndexEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}