//! Index management – create and drop B-tree / Hash index metadata.
//!
//! Full B-tree traversal is implemented here. It manages the metadata in
//! pg_index and utilizes a page-based B-Tree for constraint validation.

use std::fs;
use std::path::Path;

use crate::buffer_manager::BufferManager;
use crate::catalog::page_manager::{CAT_CONSTRAINT, CAT_INDEX, CAT_TABLE, CAT_DATABASE, CatalogPageManager};
use crate::catalog::serialize::{
    deserialize_constraint_tuple, deserialize_index_tuple, serialize_index_tuple, deserialize_table_tuple, deserialize_database_tuple
};
use crate::catalog::types::{Catalog, CatalogError, Index, IndexType};
use crate::layout::{INDEX_DIR_TEMPLATE, INDEX_FILE_TEMPLATE};

// ─────────────────────────────────────────────────────────────
// Helper: resolve db_name for a table OID
// ─────────────────────────────────────────────────────────────

fn db_name_for_table(pm: &CatalogPageManager, bm: &mut BufferManager, table_oid: u32) -> Option<String> {
    if let Ok(tables) = pm.scan_catalog(bm, CAT_TABLE) {
        let mut target_db_oid = None;
        for t in &tables {
            if let Ok((toid, _, db_oid, ..)) = deserialize_table_tuple(t) {
                if toid == table_oid {
                    target_db_oid = Some(db_oid);
                    break;
                }
            }
        }
        if let Some(db_oid) = target_db_oid {
            if let Ok(dbs) = pm.scan_catalog(bm, CAT_DATABASE) {
                for d in &dbs {
                    if let Ok((doid, name, ..)) = deserialize_database_tuple(d) {
                        if doid == db_oid {
                            return Some(name);
                        }
                    }
                }
            }
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────
// create_index
// ─────────────────────────────────────────────────────────────

/// Create an index on `column_oids` for `table_oid`.
pub fn create_index(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    column_oids: Vec<u32>,
    is_unique: bool,
    is_primary: bool,
    index_name: Option<String>,
) -> Result<u32, CatalogError> {
    let db_name = db_name_for_table(pm, bm, table_oid)
        .ok_or_else(|| CatalogError::TableNotFound(table_oid.to_string()))?;

    let name = index_name.unwrap_or_else(|| {
        let col_part = column_oids
            .iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>()
            .join("_");
        format!("idx_{}_{}", table_oid, col_part)
    });

    let idx_dir = INDEX_DIR_TEMPLATE.replace("{database}", &db_name);
    if !Path::new(&idx_dir).exists() {
        fs::create_dir_all(&idx_dir)?;
    }

    let idx_file = INDEX_FILE_TEMPLATE
        .replace("{database}", &db_name)
        .replace("{index}", &name);
    if !Path::new(&idx_file).exists() {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&idx_file)
            .map_err(CatalogError::IoError)?;
        let mut root_page = crate::page::Page::new();
        root_page.data[0] = 1;
        root_page.data[1..3].copy_from_slice(&0u16.to_le_bytes());
        root_page.data[3..5].copy_from_slice(&11u16.to_le_bytes());
        root_page.data[5..7].copy_from_slice(&(crate::page::PAGE_SIZE as u16).to_le_bytes());
        root_page.data[7..11].copy_from_slice(&0u32.to_le_bytes());
        crate::disk::write_page(&mut file, &mut root_page, 0).map_err(CatalogError::IoError)?;
    }

    let index_oid = catalog.alloc_oid();
    let index = Index {
        index_oid,
        index_name: name,
        table_oid,
        index_type: IndexType::BTree,
        column_oids: column_oids.clone(),
        is_unique,
        is_primary,
        index_pages: 1,
    };

    let bytes = serialize_index_tuple(&index);
    pm.insert_catalog_tuple(bm, CAT_INDEX, bytes)?;


    catalog.cache.invalidate_indexes(table_oid);

    Ok(index_oid)
}

// ─────────────────────────────────────────────────────────────
// drop_index
// ─────────────────────────────────────────────────────────────

/// Drop an index by `index_oid`.
pub fn drop_index(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    index_oid: u32,
) -> Result<(), CatalogError> {
    let result = pm.find_catalog_tuple(bm, CAT_INDEX, |b| {
        deserialize_index_tuple(b)
            .map(|idx| idx.index_oid == index_oid)
            .unwrap_or(false)
    })?;

    let (pn, slot, raw) =
        result.ok_or_else(|| CatalogError::IndexNotFound(index_oid.to_string()))?;
    let index = deserialize_index_tuple(&raw).map_err(CatalogError::IoError)?;

    let constraints = pm.scan_catalog(bm, CAT_CONSTRAINT)?;
    for t in &constraints {
        let c = deserialize_constraint_tuple(t).map_err(CatalogError::IoError)?;
        let references = match &c.metadata {
            crate::catalog::types::ConstraintMetadata::PrimaryKey { index_oid: ioid } => {
                *ioid == index_oid
            }
            crate::catalog::types::ConstraintMetadata::Unique { index_oid: ioid } => {
                *ioid == index_oid
            }
            _ => false,
        };
        if references {
            return Err(CatalogError::ForeignKeyDependency(
                c.constraint_name.clone(),
            ));
        }
    }

    let db_name = db_name_for_table(pm, bm, index.table_oid).unwrap_or_default();
    let idx_file = INDEX_FILE_TEMPLATE
        .replace("{database}", &db_name)
        .replace("{index}", &index.index_name);
    let _ = fs::remove_file(&idx_file);

    pm.delete_catalog_tuple(bm, CAT_INDEX, pn, slot)?;


    catalog.cache.invalidate_indexes(index.table_oid);

    Ok(())
}

// ─────────────────────────────────────────────────────────────
// get_indexes_for_table
// ─────────────────────────────────────────────────────────────

pub fn get_indexes_for_table(
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
) -> Result<Vec<Index>, CatalogError> {
    let tuples = pm.scan_catalog(bm, CAT_INDEX)?;
    tuples
        .iter()
        .map(|t| deserialize_index_tuple(t).map_err(CatalogError::IoError))
        .filter(|r| {
            r.as_ref()
                .map(|idx: &Index| idx.table_oid == table_oid)
                .unwrap_or(true)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────
// Page-Based B-Tree Implementation
// ─────────────────────────────────────────────────────────────

fn get_index_path(db_name: &str, index_name: &str) -> String {
    INDEX_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{index}", index_name)
}

fn allocate_index_page(file_path: &str) -> Result<u32, CatalogError> {
    let file_size = fs::metadata(file_path)
        .map_err(CatalogError::IoError)?
        .len();
    let new_page_num = (file_size / crate::page::PAGE_SIZE as u64) as u32;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(file_path)
        .map_err(CatalogError::IoError)?;
    crate::disk::write_page(&mut file, &mut crate::page::Page::new(), new_page_num)
        .map_err(CatalogError::IoError)?;
    Ok(new_page_num)
}

fn init_leaf_page(page: &mut crate::page::Page) {
    page.data[0] = 1;
    page.data[1..3].copy_from_slice(&0u16.to_le_bytes());
    page.data[3..5].copy_from_slice(&11u16.to_le_bytes());
    page.data[5..7].copy_from_slice(&(crate::page::PAGE_SIZE as u16).to_le_bytes());
    page.data[7..11].copy_from_slice(&0u32.to_le_bytes());
}

fn search_node(page: &crate::page::Page, key_bytes: &[u8], is_leaf: bool) -> (bool, u16) {
    let num_keys = u16::from_le_bytes(page.data[1..3].try_into().unwrap());
    let mut low = if is_leaf { 0 } else { 1 };
    let mut high = num_keys;
    while low < high {
        let mid = low + (high - low) / 2;
        let slot = 11 + mid as usize * 4;
        let p_off = u16::from_le_bytes(page.data[slot..slot + 2].try_into().unwrap()) as usize;
        let p_len = u16::from_le_bytes(page.data[slot + 2..slot + 4].try_into().unwrap()) as usize;
        let key_len = if is_leaf {
            p_len.saturating_sub(8)
        } else {
            p_len.saturating_sub(4)
        };
        let node_key = &page.data[p_off..p_off + key_len];
        if node_key == key_bytes {
            return (true, mid);
        } else if node_key < key_bytes {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    (false, low)
}

fn get_internal_child(page: &crate::page::Page, key_bytes: &[u8]) -> u32 {
    let num_keys = u16::from_le_bytes(page.data[1..3].try_into().unwrap());
    let mut low = 1;
    let mut high = num_keys;
    while low < high {
        let mid = low + (high - low) / 2;
        let slot = 11 + mid as usize * 4;
        let p_off = u16::from_le_bytes(page.data[slot..slot + 2].try_into().unwrap()) as usize;
        let p_len = u16::from_le_bytes(page.data[slot + 2..slot + 4].try_into().unwrap()) as usize;
        let key_len = p_len - 4;
        let node_key = &page.data[p_off..p_off + key_len];
        if node_key <= key_bytes {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let target_idx = low - 1;
    let slot = 11 + target_idx as usize * 4;
    let p_off = u16::from_le_bytes(page.data[slot..slot + 2].try_into().unwrap()) as usize;
    let p_len = u16::from_le_bytes(page.data[slot + 2..slot + 4].try_into().unwrap()) as usize;
    let ptr = p_off + p_len - 4;
    u32::from_le_bytes(page.data[ptr..ptr + 4].try_into().unwrap())
}

pub fn index_lookup(
    bm: &mut BufferManager,
    db_name: &str,
    index_name: &str,
    key_bytes: &[u8],
) -> Result<bool, CatalogError> {
    let file_path = get_index_path(db_name, index_name);
    if !Path::new(&file_path).exists() {
        return Ok(false);
    }
    let mut current_page = 0;
    loop {
        let fi = bm
            .pin_page(crate::buffer_manager::PageId::new(&file_path, current_page))
            .map_err(CatalogError::IoError)?;
        let is_leaf = bm.frames[fi].data[0] == 1;
        let next_page = if is_leaf {
            let (found, _) = search_node(&bm.frames[fi], key_bytes, true);
            bm.unpin_page(
                &crate::buffer_manager::PageId::new(&file_path, current_page),
                false,
            )
            .map_err(CatalogError::IoError)?;
            return Ok(found);
        } else {
            get_internal_child(&bm.frames[fi], key_bytes)
        };
        bm.unpin_page(
            &crate::buffer_manager::PageId::new(&file_path, current_page),
            false,
        )
        .map_err(CatalogError::IoError)?;
        current_page = next_page;
    }
}

fn insert_into_page(
    page: &mut crate::page::Page,
    key_bytes: &[u8],
    payload_suffix: &[u8],
    is_leaf: bool,
) -> Result<(), ()> {
    let num_keys = u16::from_le_bytes(page.data[1..3].try_into().unwrap());
    let lower = u16::from_le_bytes(page.data[3..5].try_into().unwrap());
    let upper = u16::from_le_bytes(page.data[5..7].try_into().unwrap());

    let total_payload_len = key_bytes.len() + payload_suffix.len();
    let required = total_payload_len as u16 + 4;
    if lower + required > upper {
        return Err(());
    }

    let (_, mut insert_idx) = search_node(page, key_bytes, is_leaf);
    if !is_leaf && key_bytes.is_empty() {
        insert_idx = 0;
    } else if !is_leaf && insert_idx == 0 {
        insert_idx = 1;
    }

    let slot_start = 11 + insert_idx as usize * 4;
    let slot_end = 11 + num_keys as usize * 4;
    page.data.copy_within(slot_start..slot_end, slot_start + 4);

    let new_upper = upper - total_payload_len as u16;
    page.data[new_upper as usize..new_upper as usize + key_bytes.len()].copy_from_slice(key_bytes);
    page.data[new_upper as usize + key_bytes.len()..upper as usize].copy_from_slice(payload_suffix);

    page.data[slot_start..slot_start + 2].copy_from_slice(&new_upper.to_le_bytes());
    page.data[slot_start + 2..slot_start + 4]
        .copy_from_slice(&(total_payload_len as u16).to_le_bytes());

    page.data[1..3].copy_from_slice(&(num_keys + 1).to_le_bytes());
    page.data[3..5].copy_from_slice(&(lower + 4).to_le_bytes());
    page.data[5..7].copy_from_slice(&new_upper.to_le_bytes());
    Ok(())
}

fn split_page(
    page: &mut crate::page::Page,
    new_page: &mut crate::page::Page,
    is_leaf: bool,
) -> Vec<u8> {
    let num_keys = u16::from_le_bytes(page.data[1..3].try_into().unwrap());
    let mid = num_keys / 2;

    init_leaf_page(new_page);
    if !is_leaf {
        new_page.data[0] = 0;
    }

    let mut keys_to_move = Vec::new();
    for i in mid..num_keys {
        let slot = 11 + i as usize * 4;
        let p_off = u16::from_le_bytes(page.data[slot..slot + 2].try_into().unwrap()) as usize;
        let p_len = u16::from_le_bytes(page.data[slot + 2..slot + 4].try_into().unwrap()) as usize;
        keys_to_move.push(page.data[p_off..p_off + p_len].to_vec());
    }

    let mut old_keys = Vec::new();
    for i in 0..mid {
        let slot = 11 + i as usize * 4;
        let p_off = u16::from_le_bytes(page.data[slot..slot + 2].try_into().unwrap()) as usize;
        let p_len = u16::from_le_bytes(page.data[slot + 2..slot + 4].try_into().unwrap()) as usize;
        old_keys.push(page.data[p_off..p_off + p_len].to_vec());
    }

    init_leaf_page(page);
    if !is_leaf {
        page.data[0] = 0;
    }
    for k in old_keys {
        let suffix_len = if is_leaf { 8 } else { 4 };
        let key_bytes = &k[0..k.len() - suffix_len];
        let suffix = &k[k.len() - suffix_len..];
        insert_into_page(page, key_bytes, suffix, is_leaf).unwrap();
    }

    if !is_leaf {
        let k = &keys_to_move[0];
        let suffix = &k[k.len() - 4..];
        insert_into_page(new_page, &[], suffix, false).unwrap();

        for k in &keys_to_move[1..] {
            let key_bytes = &k[0..k.len() - 4];
            let suffix = &k[k.len() - 4..];
            insert_into_page(new_page, key_bytes, suffix, false).unwrap();
        }
    } else {
        for k in &keys_to_move {
            let key_bytes = &k[0..k.len() - 8];
            let suffix = &k[k.len() - 8..];
            insert_into_page(new_page, key_bytes, suffix, true).unwrap();
        }
    }

    let suffix_len = if is_leaf { 8 } else { 4 };
    keys_to_move[0][0..keys_to_move[0].len() - suffix_len].to_vec()
}

pub fn insert_index_entry(
    bm: &mut BufferManager,
    db_name: &str,
    index_name: &str,
    key_bytes: &[u8],
    page_num: u32,
    slot_id: u32,
) -> Result<(), CatalogError> {
    let file_path = get_index_path(db_name, index_name);
    let mut path = Vec::new();
    let mut current_page = 0;
    loop {
        path.push(current_page);
        let fi = bm
            .pin_page(crate::buffer_manager::PageId::new(&file_path, current_page))
            .map_err(CatalogError::IoError)?;
        let is_leaf = bm.frames[fi].data[0] == 1;
        let next_page = if is_leaf {
            None
        } else {
            Some(get_internal_child(&bm.frames[fi], key_bytes))
        };
        bm.unpin_page(
            &crate::buffer_manager::PageId::new(&file_path, current_page),
            false,
        )
        .map_err(CatalogError::IoError)?;
        if is_leaf {
            break;
        }
        current_page = next_page.unwrap();
    }

    let mut payload_suffix = vec![0u8; 8];
    payload_suffix[0..4].copy_from_slice(&page_num.to_le_bytes());
    payload_suffix[4..8].copy_from_slice(&slot_id.to_le_bytes());

    let mut insert_key = key_bytes.to_vec();
    let mut insert_suffix = payload_suffix;

    while let Some(node_num) = path.pop() {
        let fi = bm
            .pin_page(crate::buffer_manager::PageId::new(&file_path, node_num))
            .map_err(CatalogError::IoError)?;
        let is_leaf = bm.frames[fi].data[0] == 1;

        let mut split = false;
        let mut promoted_key = Vec::new();
        let mut new_child = 0;

        if insert_into_page(&mut bm.frames[fi], &insert_key, &insert_suffix, is_leaf).is_err() {
            let mut new_page = crate::page::Page::new();
            let new_page_num = allocate_index_page(&file_path)?;

            promoted_key = split_page(&mut bm.frames[fi], &mut new_page, is_leaf);

            if is_leaf {
                let old_right = u32::from_le_bytes(bm.frames[fi].data[7..11].try_into().unwrap());
                new_page.data[7..11].copy_from_slice(&old_right.to_le_bytes());
                bm.frames[fi].data[7..11].copy_from_slice(&new_page_num.to_le_bytes());
            }

            if insert_key >= promoted_key {
                insert_into_page(&mut new_page, &insert_key, &insert_suffix, is_leaf).unwrap();
            } else {
                insert_into_page(&mut bm.frames[fi], &insert_key, &insert_suffix, is_leaf)
                    .unwrap();
            }

            let nfi = bm
                .pin_page(crate::buffer_manager::PageId::new(&file_path, new_page_num))
                .map_err(CatalogError::IoError)?;
            bm.frames[nfi].data.copy_from_slice(&new_page.data);
            bm.unpin_page(
                &crate::buffer_manager::PageId::new(&file_path, new_page_num),
                true,
            )
            .map_err(CatalogError::IoError)?;

            split = true;
            new_child = new_page_num;
        }

        bm.unpin_page(
            &crate::buffer_manager::PageId::new(&file_path, node_num),
            true,
        )
        .map_err(CatalogError::IoError)?;

        if !split {
            return Ok(());
        }

        insert_key = promoted_key;
        insert_suffix = new_child.to_le_bytes().to_vec();
    }

    let old_root_page_num = allocate_index_page(&file_path)?;
    let root_fi = bm
        .pin_page(crate::buffer_manager::PageId::new(&file_path, 0))
        .map_err(CatalogError::IoError)?;
    let old_root_fi = bm
        .pin_page(crate::buffer_manager::PageId::new(
            &file_path,
            old_root_page_num,
        ))
        .map_err(CatalogError::IoError)?;

    let root_data = bm.frames[root_fi].data.clone();
    bm.frames[old_root_fi].data.copy_from_slice(&root_data);

    let mut new_root = crate::page::Page::new();
    new_root.data[0] = 0; // internal
    new_root.data[1..3].copy_from_slice(&0u16.to_le_bytes());
    new_root.data[3..5].copy_from_slice(&11u16.to_le_bytes());
    new_root.data[5..7].copy_from_slice(&(crate::page::PAGE_SIZE as u16).to_le_bytes());

    insert_into_page(&mut new_root, &[], &old_root_page_num.to_le_bytes(), false).unwrap();
    insert_into_page(&mut new_root, &insert_key, &insert_suffix, false).unwrap();

    bm.frames[root_fi].data.copy_from_slice(&new_root.data);

    bm.unpin_page(
        &crate::buffer_manager::PageId::new(&file_path, old_root_page_num),
        true,
    )
    .map_err(CatalogError::IoError)?;
    bm.unpin_page(&crate::buffer_manager::PageId::new(&file_path, 0), true)
        .map_err(CatalogError::IoError)?;

    Ok(())
}
