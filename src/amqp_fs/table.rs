use libc::stat;
use std::collections::hash_map::{Entry, HashMap, RandomState};
use std::ops::Deref;
use std::time::UNIX_EPOCH;
use std::{
    mem,
    sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
#[allow(unused_imports)]
use tracing::{debug, error, info, warn};

use super::Rabbit;

pub type Ino = u64;
pub type FileName = String;

const ROOT_INO: u64 = 1;

#[derive(Clone, Debug)]
pub(crate) struct EntryInfo {
    pub name: String,
    pub ino: Ino,
    pub typ: u32,
}

#[derive(Clone)]
pub(crate) struct DirEntry {
    info: EntryInfo,
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
        let mut r = Self {
            info: EntryInfo{
                name: ".".to_string(),
                ino: ROOT_INO,
                typ: libc::DT_DIR as u32,
            },
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
        // r.children.insert(".".to_string(), r.ino());
        r.atime_to_now(0);
        r.mtime_to_now();
        r
    }

    /// Create a new file or directory as a child of this node
    pub fn new_child(&mut self, ino: Ino, name: &str, mode: u32, typ: u32) -> Self {
        let mut child = Self {
            info: EntryInfo{
                name: name.to_string(),
                ino,
                typ,
            },
            parent_ino: self.ino(),
            children: DashMap::with_hasher(RandomState::new()),
            attr: {
                let mut attr = unsafe { mem::zeroed::<libc::stat>() };
                attr.st_ino = ino;
                attr.st_nlink = 1;
                attr.st_mode = mode;
                attr.st_mtime = self.attr.st_mtime;
                attr
            },
        };
        self.attr.st_nlink += 1;
        self.children.insert(child.name(), child.ino());
        child.atime_to_now(0);
        child
    }

    /// Remove a child node from this entry
    pub fn remove_child(&mut self, name: &str) -> Option<(String, Ino)> {
        match self.children.remove(name) {
            Some(ent) => {
                self.attr.st_nlink = self.attr.st_nlink.saturating_sub(1);
                Some(ent)
            }
            None => None
        }
    }

    pub fn num_children(&self) -> usize {
        self.children.len()
    }

    /// Inode of entry
    pub fn ino(&self) -> Ino {
        self.info.ino
    }

    pub fn name(&self) -> String {
        self.info.name.clone()
    }

    pub fn typ(&self) -> u32 {
        self.info.typ
    }

    pub fn info(&self) -> &EntryInfo {
        &self.info
    }

    /// Lookup a child entry's inode by name
    pub fn lookup(&self, name: &str) -> Option<Ino> {
        self.children.get(&name.to_string()).map(|ino_ref| *ino_ref)
    }

    /// Attributes of self, as returned by stat(2)
    pub fn attr(&self) -> &libc::stat {
        &self.attr
    }

    pub fn attr_mut(&mut self) -> &mut libc::stat {
        &mut self.attr
    }

    /// Vector of inodes container in this directory
    pub fn child_inos(&self) -> Vec<Ino> {
        self.children.iter().map(|ino| *ino).collect()
    }

    /// Update the entries atime to now. Panics if this somehow isn't
    /// possible. Returns the time set
    fn time_to_now(time: &mut i64) -> i64 {
        let now = std::time::SystemTime::now();
        let timestamp = now.duration_since(UNIX_EPOCH).expect("no such time").as_secs() as i64;
        *time = timestamp;
        timestamp
    }

    /// Maybe update the files atime based on the flags.
    ///
    /// Returns the new value of atime. If flags contains O_NOATIME
    /// this function does nothing.
    pub fn atime_to_now(&mut self, flags: u32) -> i64 {
        if flags & libc::O_NOATIME as u32 == 0 {
            DirEntry::time_to_now(&mut self.attr.st_atime)
        } else {
            self.attr.st_atime
        }
    }

    /// Unconidtionally sets the mtime to the current time. Panics if
    /// that isn't possible
    pub fn mtime_to_now(&mut self) -> i64 {
        DirEntry::time_to_now(&mut self.attr.st_mtime)
    }
}

/// One-deep table of directories and files.
///
/// Directories can contain files, but not other directories
impl DirectoryTable {
    pub fn new(root: &DirEntry) -> Self {
        let map = DashMap::with_hasher(RandomState::new());
        let dir_names = Vec::<&str>::new();
        let tbl = Self {
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

    /// Get the next available inode number. Inodes are promised to be
    /// unique within this table
    fn next_ino(&self) -> Ino {
        self.next_ino.fetch_add(1, Ordering::SeqCst)
    }

    /// Make a directory in the root. Note that subdirectories are not
    /// allowed and so no parent is passed into this
    ///
    /// # Panics
    /// Panics if the acquired inode already exists
    pub fn mkdir(&self, name: &str, uid: u32, gid: u32) -> Result<libc::stat, libc::c_int> {
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
                info!("Directory {} has {} children", dir.name(), dir.children.len());
                // Add the default child entries pointing to the itself and to its parent
                // dir.children.insert(".".to_string(), dir.ino());
                // dir.children.insert("..".to_string(), ROOT_INO);
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

    /// Create a new regular file in the parent inode
    ///
    /// # Errors
    ///  [libc::ENOENT] if the parent directory does not exist
    ///
    /// # Panics
    /// Panics if the inode value acquired for this file already exist
    ///
    /// * `name` : Name of file to be created
    /// * `mode` : Unix mode of file, e.g 0700
    /// * `parent_ino` : Inode of the directory holding this file.
    ///                Must exist in the current table or an error
    ///                will be returned
    pub fn mknod(&self, name: &str, mode: u32, parent_ino: Ino) -> Result<libc::stat, libc::c_int> {
        let ino = self.next_ino();
        info!("Creating node {} with inode {}", name, ino);
        use dashmap::mapref::entry::Entry;
        match self.map.entry(ino) {
            Entry::Occupied(..) => panic!("duplicate inode error"),
            Entry::Vacant(entry) => match self.map.entry(parent_ino) {
                Entry::Vacant(..) => Err(libc::ENOENT),
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
}
