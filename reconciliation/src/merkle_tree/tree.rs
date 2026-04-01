use blake3::Hasher;
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(Clone, Debug, PartialEq)]
pub struct MerkleNode {
    pub hash: [u8; 32],
    pub left: Option<Box<MerkleNode>>,
    pub right: Option<Box<MerkleNode>>,
    pub key: Option<String>,
}

pub struct BinaryMerkleTree {
    pub data: RwLock<BTreeMap<String, String>>,
    pub root: RwLock<Option<Box<MerkleNode>>>,
}

impl BinaryMerkleTree {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            root: RwLock::new(None),
        }
    }

    pub fn get_root_hash(&self) -> [u8; 32] {
        let root = self.root.read().unwrap();
        match &*root {
            Some(node) => node.hash,
            None => [0; 32],
        }
    }

    pub fn insert(&self, key: String, value: String) {
        {
            let mut data = self.data.write().unwrap();
            data.insert(key, value);
        }
        self.rebuild_tree();
    }

    pub fn remove(&self, key: &str) {
        {
            let mut data = self.data.write().unwrap();
            data.remove(key);
        }
        self.rebuild_tree();
    }

    fn rebuild_tree(&self) {
        let data = self.data.read().unwrap();
        let entries: Vec<(String, String)> =
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let new_root = Self::build_recursive(&entries);
        let mut root = self.root.write().unwrap();
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
