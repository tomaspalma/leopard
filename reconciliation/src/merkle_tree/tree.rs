use blake3::Hasher;
use std::collections::BTreeMap;
use std::sync::{Mutex, RwLock};

#[derive(Clone, Debug, PartialEq)]
pub struct MerkleNode {
    pub hash: [u8; 32],
    pub left: Option<Box<MerkleNode>>,
    pub right: Option<Box<MerkleNode>>,
    pub key: Option<String>,
}

/// A frozen, immutable view of the tree taken at one point in time.
/// Used by the reconciliation protocol so that a session sees a consistent
/// tree throughout its lifetime, even while the live tree is being updated.
pub struct MerkleTreeSnapshot {
    root: Option<Box<MerkleNode>>,
}

impl MerkleTreeSnapshot {
    pub fn get_root_hash(&self) -> [u8; 32] {
        self.root.as_ref().map(|n| n.hash).unwrap_or([0; 32])
    }

    pub fn get_node(&self, path: &str) -> Option<(MerkleNode, Option<[u8; 32]>)> {
        let root = self.root.as_deref()?;
        let mut current = root;
        let mut parent_hash = None;

        if path == "root" {
            return Some(((*current).clone(), parent_hash));
        }

        let parts: Vec<&str> = path.split('-').collect();
        for part in parts {
            if part == "root" {
                continue;
            }
            parent_hash = Some(current.hash);
            if part == "0" || part == "left" {
                current = current.left.as_deref()?;
            } else if part == "1" || part == "right" {
                current = current.right.as_deref()?;
            } else {
                return None;
            }
        }
        Some(((*current).clone(), parent_hash))
    }
}

pub struct BinaryMerkleTree {
    pub data: RwLock<BTreeMap<String, String>>,
    pub root: RwLock<Option<Box<MerkleNode>>>,
    // Serialises insert+rebuild so concurrent insertions never let a slower
    // rebuild overwrite a faster one that already incorporated more keys.
    insert_rebuild_lock: Mutex<()>,
}

impl BinaryMerkleTree {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            root: RwLock::new(None),
            insert_rebuild_lock: Mutex::new(()),
        }
    }

    /// Return an immutable snapshot of the current root for use during a
    /// single reconciliation session.
    pub fn snapshot(&self) -> MerkleTreeSnapshot {
        MerkleTreeSnapshot {
            root: self.root.read().unwrap().clone(),
        }
    }

    pub fn get_root_hash(&self) -> [u8; 32] {
        let root = self.root.read().unwrap();
        match &*root {
            Some(node) => node.hash,
            None => [0; 32],
        }
    }

    pub fn get_node(&self, path: &str) -> Option<(MerkleNode, Option<[u8; 32]>)> {
        let root = self.root.read().unwrap();
        let mut current = root.as_deref()?;
        let mut parent_hash = None;

        if path == "root" {
            return Some(((*current).clone(), parent_hash));
        }

        let parts: Vec<&str> = path.split('-').collect();
        // Assume path is like "root-left-right" or "0-1"
        for part in parts {
            if part == "root" {
                continue;
            }
            parent_hash = Some(current.hash);
            if part == "0" || part == "left" {
                current = current.left.as_deref()?;
            } else if part == "1" || part == "right" {
                current = current.right.as_deref()?;
            } else {
                return None;
            }
        }
        Some(((*current).clone(), parent_hash))
    }

    pub fn insert(&self, key: String, value: String) {
        let _guard = self.insert_rebuild_lock.lock().unwrap();
        {
            let mut data = self.data.write().unwrap();
            data.insert(key, value);
        }
        self.rebuild_tree();
    }

    pub fn remove(&self, key: &str) {
        let _guard = self.insert_rebuild_lock.lock().unwrap();
        {
            let mut data = self.data.write().unwrap();
            data.remove(key);
        }
        self.rebuild_tree();
    }

    /// Replace the entire dataset and rebuild the tree exactly once. Used to
    /// apply a batch of reconciliation changes with a single rebuild instead
    /// of one rebuild per inserted item.
    pub fn replace_all(&self, entries: Vec<(String, String)>) {
        let _guard = self.insert_rebuild_lock.lock().unwrap();
        {
            let mut data = self.data.write().unwrap();
            *data = entries.into_iter().collect();
        }
        self.rebuild_tree();
    }

    fn rebuild_tree(&self) {
        // insert_rebuild_lock must already be held by the caller.
        let entries: Vec<(String, String)> = {
            let data = self.data.read().unwrap();
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let new_root = Self::build_recursive(&entries);
        let mut root: std::sync::RwLockWriteGuard<'_, Option<Box<MerkleNode>>> = self.root.write().unwrap();
        *root = new_root;
    }

    fn build_recursive(entries: &[(String, String)]) -> Option<Box<MerkleNode>> {
        if entries.is_empty() {
            return None;
        }

        if entries.len() == 1 {
            let (k, v) = &entries[0];
            let mut hasher = Hasher::new();
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
            let hash = hasher.finalize().into();

            return Some(Box::new(MerkleNode {
                hash,
                left: None,
                right: None,
                key: Some(k.clone()),
            }));
        }

        let mid = entries.len() / 2;
        let left_child = Self::build_recursive(&entries[..mid]);
        let right_child = Self::build_recursive(&entries[mid..]);

        let mut hasher = Hasher::new();
        if let Some(left) = &left_child {
            hasher.update(&left.hash);
        }
        if let Some(right) = &right_child {
            hasher.update(&right.hash);
        }

        let hash = hasher.finalize().into();

        Some(Box::new(MerkleNode {
            hash,
            left: left_child,
            right: right_child,
            key: None,
        }))
    }
}
