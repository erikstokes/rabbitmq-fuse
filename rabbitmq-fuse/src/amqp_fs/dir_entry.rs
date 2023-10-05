//! Metadata entries in the filesystem table.

use std::time::UNIX_EPOCH;
use std::{collections::hash_map::RandomState, mem::zeroed, sync::Arc};

use dashmap::DashMap;

use super::table::DirectoryTable;
use super::{dir_iter::DirIterator, table::Error, Ino};

/// Entry info to store in directories holding the node as a child,
/// for fast lookup of the type
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EntryInfo {
    /// Inode number
    pub ino: Ino,
    /// File type. Either DT_DIR or DT_REG or DT_UNKNOWN
    pub typ: u8,
}

/// A file or directory entry in the filesystem
#[derive(Clone)]
pub(crate) struct DirEntry {
    /// Inode and type of the entry
    info: EntryInfo,
    /// Parent inode
    pub parent_ino: Ino,
    /// Names of child entries and their inodes.
    ///
    /// The rest of the child's attributes are not stored here.
    /// Instead [EntryInfo::ino] should be used in
    /// [DirectoryTable::get] to access the main entry. Storing the
    /// same entry with multiple names is allowed and will create
    /// hardlinks. Take care that the value of `st_nlinks` remains
    /// consistant.
    ///
    /// This should always be empty unless [DirEntry::info.typ] is `DT_DIR`
    children: DashMap<super::FileName, EntryInfo, RandomState>,

    /// Attributes for `stat(2)`
    attr: libc::stat,

    /// The parent filesystem table this entry belongs to
    #[doc(hidden)]
    table: Arc<super::table::DirectoryTable>,
}

impl DirEntry {
    /// Create a new root Inode entry.
    /// A given filesystem table may only have a single such root
    ///```
    /// let root = DirEntry::root(0, 0, 0o700);
    /// !assert_eq(root.ino, ROOT_INO)
    ///```
    #[allow(clippy::similar_names)]
    pub fn root(table: &Arc<DirectoryTable>, uid: u32, gid: u32, mode: u32) -> Self {
        Self {
            // name: ".".to_string(),
            info: EntryInfo {
                ino: table.root_ino(),
                typ: libc::DT_DIR,
            },
            parent_ino: table.root_ino(),
            attr: {
                let mut attr = unsafe { std::mem::zeroed::<libc::stat>() };
                attr.st_ino = table.root_ino();
                attr.st_nlink = 2;
                attr.st_mode = libc::S_IFDIR | mode;
                attr.st_blocks = 8;
                attr.st_size = 4096;
                attr.st_gid = gid;
                attr.st_uid = uid;
                attr
            },
            children: DashMap::with_hasher(RandomState::new()),
            table: table.clone(),
        }
    }

    /// Create a new file or directory as a child of this node
    pub fn new_child(&mut self, ino: Ino, name: &str, mode: u32, typ: u8) -> Result<Self, Error> {
        use dashmap::mapref::entry::Entry;
        let mut child = Self {
            // name: name.to_string(),
            info: EntryInfo { ino, typ },
            parent_ino: self.info().ino,
            children: DashMap::with_hasher(RandomState::new()),
            attr: {
                let mut attr = unsafe { zeroed::<libc::stat>() };
                attr.st_ino = ino;
                attr.st_nlink = 1;
                attr.st_mode = mode;
                attr.st_mtime = self.attr.st_mtime;
                attr
            },
            table: self.table.clone(),
        };
        child.atime_to_now(0);
        child.mtime_to_now();
        DirEntry::time_to_now(&mut child.attr.st_ctime);
        match self.children.entry(name.to_string()) {
            Entry::Occupied(_) => Err(Error::Exists),
            Entry::Vacant(entry) => {
                entry.insert(child.info().clone());
                self.attr.st_nlink += 1;
                Ok(child)
            }
        }
        // self.children.insert(child.name().to_string(), child.info().clone());
        // child.atime_to_now(0);
        // child
    }

    /// Insert child into entry.
    ///
    /// Returns the previous value if there was one
    pub fn insert_child(&mut self, name: &str, child: &EntryInfo) -> Option<EntryInfo> {
        let result = self.children.insert(name.to_string(), child.clone());
        self.attr.st_nlink += 1;
        result
    }

    /// Remove a child node from this entry
    pub fn remove_child(&mut self, name: &str) -> Option<(String, EntryInfo)> {
        self.remove_child_if(name, |_key, _val| true)
    }

    /// Remove a child if the predicate function evaluates as true on it.
    ///
    /// Returns the value if an entry was removed
    pub fn remove_child_if(
        &mut self,
        name: &str,
        f: impl FnOnce(&super::FileName, &EntryInfo) -> bool,
    ) -> Option<(String, EntryInfo)> {
        match self.children.remove_if(name, f) {
            Some((name, entry)) => {
                self.attr.st_nlink = self.attr.st_nlink.saturating_sub(1);
                Some((name, entry))
            }
            None => None,
        }
    }

    /// Number of children in this entry.
    ///
    /// Will always return if [`Self::typ`] is not `DT_DIR`
    pub fn num_children(&self) -> usize {
        self.children.len()
    }

    // /// Inode of entry
    // pub fn ino(&self) -> Ino {
    //     self.info.ino
    // }

    // /// The entry's name
    // pub fn name(&self) -> &str {
    //     &self.name
    // }

    /// The entry's type.
    pub fn typ(&self) -> u8 {
        self.info.typ
    }

    /// Type and inode of the entry
    pub fn info(&self) -> &EntryInfo {
        &self.info
    }

    /// Lookup a child entry's inode by name
    pub fn lookup(&self, name: &str) -> Option<Ino> {
        self.children.get(&name.to_string()).map(|info| info.ino)
    }

    /// Return the name of a child of this entry, or None of the inode
    /// is not a child. If self is not a directory, this will always
    /// return None
    pub fn get_child_name(&self, ino: Ino) -> Option<String> {
        for (name, info) in self.iter() {
            if info.ino == ino {
                return Some(name);
            }
        }
        None
    }

    /// Attributes of self, as returned by stat(2)
    pub fn attr(&self) -> &libc::stat {
        &self.attr
    }

    /// Mutable reference to the file's attributes
    pub fn attr_mut(&mut self) -> &mut libc::stat {
        &mut self.attr
    }

    /// Vector of inodes container in this directory
    pub fn child_iter(&self) -> dashmap::iter::Iter<'_, std::string::String, EntryInfo> {
        self.children.iter()
    }

    /// Update the entries atime to now. Panics if this somehow isn't
    /// possible. Returns the time set
    pub fn time_to_now(time: &mut i64) -> i64 {
        let now = std::time::SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .expect("no such time")
            .as_secs()
            .try_into()
            .expect("Unable to parse time");
        *time = timestamp;
        timestamp
    }

    /// Maybe update the files atime based on the flags.
    ///
    /// Returns the new value of atime. If flags contains `O_NOATIME`
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

    /// Get a reference to the dir entry's table.
    #[must_use]
    pub(crate) fn table(&self) -> &DirectoryTable {
        self.table.as_ref()
    }

    /// Iterate of the children of a directory.
    ///
    /// If the entry is not of type `DT_DIR`, the iteration immediatly ends
    pub fn iter(&self) -> impl Iterator<Item = (String, EntryInfo)> + '_ {
        DirIterator::new(self)
    }
}

#[cfg(test)]
mod test {
    use crate::amqp_fs::table::{DirectoryTable, Error};
    use std::ffi::OsStr;
    use std::path::PathBuf;

    #[test]
    fn root_path() -> Result<(), Error> {
        let table = DirectoryTable::new(0, 0, 0o700);
        let path = table.real_path(table.root_ino())?;
        assert_eq!(path.to_str().unwrap(), "");
        Ok(())
    }

    #[test]
    fn file_path() -> Result<(), Error> {
        let table = DirectoryTable::new(0, 0, 0o700);
        let dir_ino = table.mkdir(OsStr::new("a"), 0, 0)?;
        let file_ino = table.mknod(OsStr::new("b"), 0o700, dir_ino.st_ino)?.st_ino;
        let path = table.real_path(file_ino)?;
        let real_path: PathBuf = ["a", "b"].iter().collect();
        assert_eq!(path, real_path);
        Ok(())
    }
}
