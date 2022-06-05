use dashmap::mapref::entry::OccupiedEntry;
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

#[derive(Debug)]
enum Error {
    /// Requested a entry, but it does not exist
    NotExist,
    /// Entry exists, but is cannot be accesed
    Unavailable,
    /// Entry already exists
    Exists
}

/// Entry info to store in directories holding the node as a child,
/// for fast lookup of the type
#[derive(Clone, Debug)]
pub(crate) struct EntryInfo {
    pub ino: Ino,
    pub typ: u8,
}

#[derive(Clone)]
pub(crate) struct DirEntry {
    name: String,
    info: EntryInfo,
    pub parent_ino: Ino,
    // Names of child entries and their inodes.
    children: DashMap<FileName, EntryInfo, RandomState>,
    attr: libc::stat,
}

// type RegularFile = Vec<u8>;

pub(crate) struct DirectoryTable {
    pub map: DashMap<Ino, DirEntry, RandomState>,
    root_ino: Ino,
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
            name: ".".to_string(),
            info: EntryInfo{
                ino: ROOT_INO,
                typ: libc::DT_DIR,
            },
            parent_ino: ROOT_INO,
            attr: {
                let mut attr = unsafe { mem::zeroed::<libc::stat>() };
                attr.st_ino = ROOT_INO;
                attr.st_nlink = 2;
                attr.st_mode = libc::S_IFDIR | mode;
                attr.st_blocks = 8;
                attr.st_size = 4096;
                attr.st_gid = gid;
                attr.st_uid = uid;
                attr
            },
            children: DashMap::with_hasher(RandomState::new()),
        };
        // r.children.insert(".".to_string(), r.ino());
        r.atime_to_now(0);
        r.mtime_to_now();
        DirEntry::time_to_now(&mut r.attr.st_ctime);
        r
    }

    /// Create a new file or directory as a child of this node
    fn new_child(&mut self, ino: Ino, name: &str, mode: u32, typ: u8) -> Result<Self, Error> {
        let mut child = Self {
            name: name.to_string(),
            info: EntryInfo{
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
        child.atime_to_now(0);
        child.mtime_to_now();
        DirEntry::time_to_now(&mut child.attr.st_ctime);
        use dashmap::mapref::entry::Entry;

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

    /// Remove a child node from this entry
    pub fn remove_child(&mut self, name: &str) -> Option<(String, EntryInfo)> {
        // match self.children.remove(name) {
        //     Some(ent) => {
        //         self.attr.st_nlink = self.attr.st_nlink.saturating_sub(1);
        //         Some(ent)
        //     }
        //     None => None
        // }
        self.remove_child_if(name, |_key,_val| true)
    }

    pub fn remove_child_if(&mut self, name: &str, f: impl FnOnce(&FileName, &EntryInfo) -> bool) -> Option<(String, EntryInfo)> {
        match self.children.remove_if(name, f) {
            Some((name, entry)) => {
                self.attr.st_nlink = self.attr.st_nlink.saturating_sub(1);
                Some( (name, entry) )
            },
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn typ(&self) -> u8 {
        self.info.typ
    }

    pub fn info(&self) -> &EntryInfo {
        &self.info
    }

    /// Lookup a child entry's inode by name
    pub fn lookup(&self, name: &str) -> Option<Ino> {
        self.children.get(&name.to_string()).map(|info| info.ino)
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
        self.children.iter().map(|info| info.ino).collect()
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
    pub fn new(root: DirEntry) -> Self {
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

    /// Return the inode number of the table's root.
    pub fn root_ino(&self) -> Ino {
        self.root_ino
    }

    /// Get the next available inode number. Inodes are promised to be
    /// unique within this table
    fn next_ino(&self) -> Ino {
        self.next_ino.fetch_add(1, Ordering::SeqCst)
    }

    /// Lookup the inode of the entry with the given name in the parent directory
    pub fn lookup(&self, parent_ino: Ino, name: &str) -> Option<Ino> {
        let parent = self.get(parent_ino).unwrap();
        parent.value().lookup(name)
    }

    fn get(&self, ino: Ino) -> Result<dashmap::mapref::one::Ref<Ino, DirEntry>, Error> {
        use dashmap::try_result::TryResult;
        match self.get_mut(ino) {
            Ok(entry) => Ok(entry.downgrade()),
            Err(err) => Err(err),
        }
    }

    fn get_mut(&self, ino: Ino) -> Result<dashmap::mapref::one::RefMut<Ino, DirEntry>, Error> {
        use dashmap::try_result::TryResult;
        match self.map.try_get_mut(&ino) {
            TryResult::Present(entry) => Ok(entry),
            TryResult::Absent => Err(Error::NotExist),
            TryResult::Locked => Err(Error::Unavailable),
        }
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
                if let Ok(mut dir) = parent.value_mut().new_child(
                    ino,
                    name,
                    libc::S_IFDIR | 0o700,
                    libc::DT_DIR,
                ){
                    dir.attr_mut().st_uid = uid;
                    dir.attr_mut().st_gid = gid;
                    dir.attr_mut().st_blocks = 8;
                    dir.attr_mut().st_size = 4096;
                    dir.attr_mut().st_nlink = if name != "." { 2 } else { 0 };
                    info!("Directory {} has {} children", dir.name(), dir.children.len());
                    // Add the default child entries pointing to the itself and to its parent
                    // dir.children.insert(".".to_string(), dir.ino());
                    // dir.children.insert("..".to_string(), ROOT_INO);
                    entry.insert(dir.clone());
                    dir.attr
                } else {
                    return Err(libc::EEXIST);
                }
            }
        };
        self.map
            .entry(ROOT_INO)
            .and_modify(|root| root.attr.st_nlink += 1);
        self.map
            .get_mut(&ROOT_INO)
            .unwrap()
            .children
            .insert(name.to_string(), EntryInfo{ino, typ:libc::DT_DIR});
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
                    if let Ok(node) = parent.get_mut().new_child(
                        ino,
                        name,
                        libc::S_IFREG | mode,
                        libc::DT_UNKNOWN,
                    ){
                        entry.insert(node.clone());
                        Ok(node.attr)
                    } else {
                        Err(libc::EEXIST)
                    }
                }
            },
        }
    }

    /// Remove a empty directory from the table
    pub fn rmdir(&self, ino: Ino) -> Result<(), libc::c_int> {
        // Remove the directory from the table first, this prevents
        // anyone from trying to modify it. If it turns out we can't
        // remove it, we re-insert, which will be safe because we
        // don't reuse inode numbers
        let (dir_ino, dir) = match self.map.remove(&ino) {
            Some(entry) => entry,
            None => { return Err(libc::ENOENT);}
        };
        assert_eq!(dir_ino, ino);
        // To rmdir we need the node to exist, be a DT_DIR and have no children
        if dir.typ() != libc::DT_DIR {
            self.map.insert(ino, dir);
            return Err(libc::ENOTDIR);
        }
        if dir.num_children() != 0 {
            self.map.insert(ino, dir);
            return Err(libc::ENOTEMPTY);
        }

        // node is a directory and is empty. Ok to remove it. The
        // parent must exist, otherwise panic because the table is
        // corrupt.
        let mut parent = self.get_mut(dir.parent_ino).unwrap();
        parent.remove_child(&dir.name());
        Ok(())
    }


    /// Remove a file from a directory
    pub fn unlink(&self, parent_ino: Ino, name: &str) -> Result<(), libc::c_int> {
        let info = match self.get_mut(parent_ino) {
            Ok(mut parent) => {
                assert_eq!(parent.typ() , libc::DT_DIR);
                match parent.remove_child_if(name, |_name,info| {info.typ != libc::DT_DIR }) {
                    None => {return Err(libc::ENOENT);},
                    Some((_name, ino)) => ino,
                }
            }
            Err(_) => {return Err(libc::EIO);}
        };

        // The file is now gone from the parent's child list, so reduce the link count
        let nlink = match self.get_mut(info.ino) {
            Ok(mut node) => {
                let nlink = node.attr_mut().st_nlink;
                node.attr_mut().st_nlink = nlink.saturating_sub(1);
                node.attr().st_nlink
            },
            Err(Error::NotExist) => {
                warn!("File vanished while unlinking");
                0
            },
            Err(_) => {return Err(libc::EIO);}
        };

        if nlink == 0 {
            self.map.remove(&info.ino);
        }

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::{DirEntry, DirectoryTable};

    #[test]
    fn root() -> Result<(), super::Error>{
        let root = DirEntry::root(0,0,0o700);
        assert_eq!(root.ino(), super::ROOT_INO);
        let table = DirectoryTable::new(root);
        assert_eq!(table.root_ino(), super::ROOT_INO);
        let rt = table.get(table.root_ino())?;
        assert_eq!(rt.ino(), super::ROOT_INO);
        assert_eq!(rt.attr().st_nlink, 2);
        Ok(())
    }

    fn root_table() -> DirectoryTable {
        let root = DirEntry::root(0,0,0o700);
        DirectoryTable::new(root)
    }

    #[test]
    fn mkdir() -> Result<(), libc::c_int> {
        let table = root_table();
        let stat = table.mkdir("test", 0, 0)?;
        assert_eq!(stat.st_nlink, 2);
        let dir = table.get(stat.st_ino).unwrap();
        assert_eq!(*dir.attr(), stat);
        assert_eq!(dir.name(), "test");
        Ok(())
    }

    #[test]
    fn mknod() -> Result<(), libc::c_int> {
        let table = root_table();
        let mode = 0o700;
        let parent_ino = table.mkdir("test", 0, 0)?.st_ino;
        let child_stat = table.mknod("file", mode, parent_ino)?;

        let parent = table.get(parent_ino).unwrap();
        let child = table.get(child_stat.st_ino).unwrap();
        assert_eq!(parent.attr().st_nlink, 3);
        assert_eq!(child.attr().st_nlink, 1);
        assert_eq!(child.parent_ino, parent.ino());
        assert_eq!(child.attr().st_mode,   libc::S_IFREG | mode);
        assert_eq!(child.typ(), child.info().typ);
        assert_eq!(child.info().typ, libc::DT_UNKNOWN);
        assert_eq!(parent.num_children(), 1);
        assert_eq!(child.name(), "file");
        assert_ne!(child.attr().st_atime, 0);
        Ok(())
    }

    #[test]
    fn mknod_duplicate() -> Result<(), libc::c_int> {
        let table = root_table();
        let mode = 0o700;
        let parent_ino = table.mkdir("test", 0, 0)?.st_ino;
        // attempt to make the file twice, the second should fail and
        // leave us with one child
        table.mknod("file", mode, parent_ino)?;
        let result = table.mknod("file", mode, parent_ino);
        assert!(result.is_err());
        let parent = table.get(parent_ino).unwrap();
        assert_eq!(parent.num_children(), 1);

        Ok(())
    }

    #[test]
    fn rmdir_nonempty() {
        let table = root_table();
        let parent_ino = table.mkdir("test_dir", 0, 0).unwrap().st_ino;
        let _ = table.mknod("file", 0o700, parent_ino);
        let result = std::panic::catch_unwind(move || {
            table.rmdir(parent_ino).unwrap();
        });
        assert!(result.is_err());
    }

    #[test]
    fn rmdir_empty() -> Result<(), libc::c_int> {
        let table = root_table();
        let parent_ino = table.mkdir("test_dir", 0, 0)?.st_ino;
        assert_eq!(table.get(table.root_ino()).unwrap().num_children(), 1);
        table.rmdir(parent_ino)?;
        assert!(table.get(parent_ino).is_err());
        assert_eq!(table.get(table.root_ino()).unwrap().num_children(), 0);
        Ok(())
    }

    #[test]
    fn unlink_exists() -> Result<(), libc::c_int> {
        let table = root_table();
        let parent_ino = table.mkdir("test_dir", 0,0)?.st_ino;
        let child_ino = table.mknod("test_file", 0o700, parent_ino)?.st_ino;
        if let Ok(parent) = table.get(parent_ino) {
            assert_eq!(parent.value().num_children(), 1);
        }
        table.unlink(parent_ino, "test_file")?;
        if let Ok(parent) = table.get(parent_ino) {
            assert_eq!(parent.value().num_children(), 0);
        }
        let child = table.get(child_ino);
        assert!(child.is_err());

        Ok(())
    }

    #[test]
    fn unlink_no_exist() -> Result<(), libc::c_int> {
        let table = root_table();
        let parent_ino = table.mkdir("test_dir", 0,0)?.st_ino;
        let result = table.unlink(parent_ino, "fake_name");
        assert!(result.is_err());
        let parent = table.get(parent_ino).unwrap();
        assert_eq!(parent.num_children(), 0);
        assert_eq!(parent.attr().st_nlink, 2);
        Ok(())
    }

    #[test]
    fn unliknk_dir() -> Result<(), libc::c_int> {
        let table = root_table();
        table.mkdir("test_dir", 0,0)?;
        let result = table.unlink(table.root_ino(), "test_dir");
        assert!(result.is_err());
        let root = table.get(table.root_ino()).unwrap();
        assert_eq!(root.num_children(), 1);
        Ok(())
    }
}
