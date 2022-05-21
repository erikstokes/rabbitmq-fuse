use libc::stat;
use std::collections::hash_map::{Entry, HashMap, RandomState};
use std::{
    mem,
    sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
#[allow(unused_imports)]
use tracing::{debug, error, info, warn};

use super::Rabbit;

type Ino = u64;
type FileName = String;

const ROOT_INO: u64 = 1;

#[derive(Clone)]
pub(crate) struct DirEntry {
    pub name: String,
    pub ino: Ino,
    pub typ: u32,
    pub parent_ino: Ino,
    // Names of child entries and their inodes.
    children: DashMap<FileName, Ino, RandomState>,
    attr: libc::stat,
}

// type RegularFile = Vec<u8>;

pub(crate) struct DirectoryTable {
    pub map: DashMap<Ino, DirEntry, RandomState>,
    pub root_ino: Ino,
    next_ino: AtomicU64,
}

impl DirEntry {
    /// Create a new root Inode entry.
    ///
    /// A given filesystem table may only have a single such root
    ///```
    /// let root = DirEntry::root(0, 0, 0o700);
    /// !assert_eq(root.ino, ROOT_INO)
    ///```
    pub fn root(uid: u32, gid: u32, mode: u32) -> Self {
        let r = Self {
            name: ".".to_string(),
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
            parent_ino: ROOT_INO,
            attr: {
                let mut attr = unsafe { mem::zeroed::<libc::stat>() };
                attr.st_ino = ROOT_INO;
                attr.st_nlink = 1; // that's right, 1 not 2. The second link will be made when the directory table assembles itself
                attr.st_mode = libc::S_IFDIR | mode;
                attr.st_gid = gid;
                attr.st_uid = uid;
                attr
            },
            children: DashMap::with_hasher(RandomState::new()),
        };
        r.children.insert(".".to_string(), r.ino);
        r
    }

    /// Create a new file or directory as a child of this node
    pub fn new_child(&mut self, ino: Ino, name: &str, mode: u32, typ: u32) -> Self {
        let child = Self {
            name: name.to_string(),
            ino,
            typ,
            parent_ino: self.ino,
            children: DashMap::with_hasher(RandomState::new()),
            attr: {
                let mut attr = unsafe { mem::zeroed::<libc::stat>() };
                attr.st_ino = ino;
                attr.st_nlink = 1;
                attr.st_mode = mode;
                attr
            },
        };
        self.attr.st_nlink += 1;
        self.children.insert(child.name.clone(), child.ino);
        child
    }

    pub fn lookup(&self, name: &str) -> Option<Ino> {
        self.children.get(&name.to_string()).map(|ino_ref| *ino_ref)
    }

    pub fn attr(&self) -> libc::stat {
        self.attr
    }
}

impl DirectoryTable {
    pub fn new(root: &DirEntry) -> Self {
        let map = DashMap::with_hasher(RandomState::new());
        let dir_names = vec![".."];
        let mut tbl = Self {
            map,
            root_ino: ROOT_INO,
            next_ino: AtomicU64::new(ROOT_INO + 1),
        };
        tbl.map.insert(ROOT_INO, root.clone());
        for name in dir_names.iter() {
            // If we can't make the root directory, the world is
            // broken. Panic immediatly.
            tbl.mkdir(name, root.attr.st_uid, root.attr.st_gid).unwrap();
        }
        tbl
    }

    fn next_ino(&self) -> Ino {
        self.next_ino.fetch_add(1, Ordering::SeqCst)
    }

    /// Make a directory in the root. Note that subdirectories are not
    /// allowed and so no parent is passed into this
    pub fn mkdir(&mut self, name: &str, uid: u32, gid: u32) -> Result<libc::stat, libc::c_int> {
        let ino = self.next_ino();
        info!("Creating directory {} with inode {}", name, ino);
        use dashmap::mapref::entry::Entry;
        let attr = match self.map.entry(ino) {
            Entry::Occupied(..) => panic!("duplicate inode error"),
            Entry::Vacant(entry) => {
                let mut parent = self.map.get_mut(&ROOT_INO).unwrap();
                let mut dir = parent.value_mut().new_child(
                    ino,
                    name,
                    libc::S_IFDIR | 0o700,
                    libc::DT_DIR as u32,
                );
                dir.attr.st_uid = uid;
                dir.attr.st_gid = gid;
                dir.attr.st_blocks = 8;
                dir.attr.st_size = 4096;
                dir.attr.st_nlink = if name != "." { 2 } else { 0 };
                info!("Directory {} has {} children", dir.name, dir.children.len());
                // Add the default child entries pointing to the itself and to its parent
                dir.children.insert(".".to_string(), dir.ino);
                dir.children.insert("..".to_string(), ROOT_INO);
                entry.insert(dir.clone());
                dir.attr
            }
        };
        self.map
            .entry(ROOT_INO)
            .and_modify(|root| root.attr.st_nlink += 1);
        self.map
            .get_mut(&ROOT_INO)
            .unwrap()
            .children
            .insert(name.to_string(), ino);
        info!("Filesystem contains {} directories", self.map.len());
        Ok(attr)
    }

    pub fn mknod(
        &mut self,
        name: &str,
        mode: u32,
        parent_ino: Ino,
    ) -> Result<libc::stat, libc::c_int> {
        let ino = self.next_ino();
        info!("Creating node {} with inode {}", name, ino);
        use dashmap::mapref::entry::Entry;
        match self.map.entry(ino) {
            Entry::Occupied(..) => panic!("duplicate inode error"),
            Entry::Vacant(entry) => match self.map.entry(parent_ino) {
                Entry::Vacant(..) => {
                    Err(libc::ENOENT)
                }
                Entry::Occupied(mut parent) => {
                    let node = parent.get_mut().new_child(
                        ino,
                        name,
                        libc::S_IFREG | mode,
                        libc::DT_UNKNOWN as u32,
                    );
                    entry.insert(node.clone());
                    Ok(node.attr)
                }
            },
        }
    }

    pub fn iter_dir<'a>(&'a self, dir: &'a DirEntry) -> impl Iterator<Item = (u64, DirEntry)> + '_ {
        use dashmap::try_result::TryResult;
        use std::iter;
        debug!(
            "Iterating over {} with {} children",
            dir.name,
            dir.children.len()
        );
        // DashMap interator iterates over the values of the map,
        // which are inodes in this case.
        dir.children
            .iter()
            .filter_map(move |ino| match self.map.try_get(&ino) {
                TryResult::Present(entry) => {
                    let mut child = entry.clone();
                    if child.ino == dir.ino {
                        child.name = ".".to_string()
                    } else if child.ino == dir.parent_ino {
                        child.name = "..".to_string()
                    }
                    Some((*ino, child))
                }
                TryResult::Absent => None,
                TryResult::Locked => None,
            })
    }
}
