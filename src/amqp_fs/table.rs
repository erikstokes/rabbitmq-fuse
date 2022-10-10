//! File and directory entires

use std::collections::hash_map::RandomState;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use thiserror::Error;

#[allow(unused_imports)]
use tracing::{debug, error, info, warn};

pub(crate) use super::dir_entry::{EntryInfo, DirEntry};
use super::Ino;


/// Inode of the root entry in the mountpoint
const ROOT_INO: u64 = 1;

#[derive(Error, Debug)]
pub enum Error {
    /// Requested a entry, but it does not exist
    #[error("Node does not exist")]
    NotExist,
    /// Entry exists, but is cannot be accesed
    #[error("Node is currently unavailable")]
    Unavailable,
    /// Entry already exists
    #[error("Node already exists")]
    Exists,
    #[error("The directory is not empty")]
    NotEmpty,
    #[error("Operation on wrong node type {typ}")]
    WrongType{
        typ: u8,
        expected: u8
    },
    #[error("The given name is invalid")]
    InvalidName,
}

// type RegularFile = Vec<u8>;

/// Table mapping inodes to [DirEntry].
///
/// Allows for safely creating, removing and retrieving entries. Calls
/// to this may deadlock if you hold multiple entries at the same
/// time.
pub(crate) struct DirectoryTable {
    pub map: DashMap<Ino, DirEntry, RandomState>,
    root_ino: Ino,
    next_ino: AtomicU64,
}

/// One-deep table of directories and files.
///
/// Directories can contain files, but not other directories. The
/// arguments are the default UID, GID and mode of files created in
/// the mount
impl DirectoryTable {
    pub fn new(uid:u32, gid: u32, mode:u32) -> Arc<Self> {
        let map = DashMap::with_hasher(RandomState::new());
        let dir_names = Vec::<&str>::new();
        let tbl = Arc::new(Self {
            map,
            root_ino: ROOT_INO,
            next_ino: AtomicU64::new(ROOT_INO + 1),
        });
        let root = {
            let mut root = DirEntry::root(&tbl, uid, gid, mode);
            // Set all 3 timestamps for the root node. 'created' time is
            // the time it was mounted.
            root.atime_to_now(0);
            root.mtime_to_now();
            DirEntry::time_to_now(&mut root.attr_mut().st_ctime);
            root
        };
        tbl.map.insert(ROOT_INO, root.clone());
        for name in dir_names.iter() {
            // If we can't make the root directory, the world is
            // broken. Panic immediatly.
            let osname: OsString = name.into();
            tbl.mkdir(osname.as_os_str(), root.attr().st_uid, root.attr().st_gid).unwrap();
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

    /// Get a reference to the given entry
    ///
    /// May deadlock if holding any other reference to the table
    pub fn get(&self, ino: Ino) -> Result<dashmap::mapref::one::Ref<Ino, DirEntry>, Error> {
        match self.get_mut(ino) {
            Ok(entry) => Ok(entry.downgrade()),
            Err(err) => Err(err),
        }
    }

    /// Get a mutable reference to the given entry
    ///
    /// May deadlock if holding any other reference to the table
    pub fn get_mut(&self, ino: Ino) -> Result<dashmap::mapref::one::RefMut<Ino, DirEntry>, Error> {
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
    pub fn mkdir(&self, name: &OsStr, uid: u32, gid: u32) -> Result<libc::stat, Error> {

        let name = name.to_str().ok_or_else(|| Error::InvalidName)?;

        let ino = self.next_ino();
        info!("Creating directory {} with inode {}", name, ino);
        let dir = {
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
                    info!("Directory {} has {} children", dir.info().ino, dir.num_children());
                    // Add the default child entries pointing to the itself and to its parent
                    // dir.children.insert(".".to_string(), dir.ino());
                    // dir.children.insert("..".to_string(), ROOT_INO);
                    // entry.insert(dir.clone());
                    dir
                } else {
                    return Err(Error::Exists)
                }
        };
        let old = self.map.insert(ino, dir.clone());

        // should be impossible to have an entry with the same inode
        // already in the table
        assert!(old.is_none());
        self.map
            .entry(ROOT_INO)
            .and_modify(|root| root.attr_mut().st_nlink += 1);
        self.map
            .get_mut(&ROOT_INO)
            .unwrap()
            // .children
            .insert_child(&name.to_string(), &EntryInfo{ino, typ:libc::DT_DIR});
        info!("Filesystem contains {} directories", self.map.len());
        Ok(*dir.attr())
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
    pub fn mknod(&self, name: &OsStr, mode: u32, parent_ino: Ino) -> Result<libc::stat, Error> {

        let name = name.to_str().ok_or_else(|| Error::InvalidName)?;

        let ino = self.next_ino();
        info!("Creating node {} with inode {} in parent {}",
              name, ino, parent_ino);

        let result = {
            // block to make sure the parent is dropped before we add
            // the child to the inode table. Otherwise we might get a deadlock
            let mut parent = match self.get_mut(parent_ino) {
                Ok(parent) => parent,
                Err(err) => {
                    return Err(err);
                }
            };
            if let Ok(node) = parent.new_child(
                ino,
                name,
                libc::S_IFREG | mode,
                libc::DT_UNKNOWN,
            ){
                // entry.insert(node.clone());
                Ok(node)
            } else {
                Err(Error::Exists)
            }
        };

        // Insert the child into the inode table. There's a momemt
        // where it exists in the directory, but can't be stat'd
        match result {
            Ok(child) => {
                // The generated inode was unique, this can't fail
                self.map.insert(ino, child);
            },
            Err(err) => {
                return Err(err)
            }
        };

        // If the child somehow doesn't exist, we must have messed up
        // the inodes and probably can't recover
        Ok(*self.get(ino).unwrap().attr())


    }

    /// Remove a empty directory from the table
    pub fn rmdir(&self, parent_ino: Ino, name: &OsStr) -> Result<(), Error> {
        // Remove the directory from the table first, this prevents
        // anyone from trying to modify it. If it turns out we can't
        // remove it, we re-insert, which will be safe because we
        // don't reuse inode numbers

        let name = name.to_str().ok_or_else(|| Error::InvalidName)?;

        let ino = self.lookup(parent_ino, name).ok_or(Error::NotExist)?;
        let (dir_ino, dir) = self.map.remove(&ino).ok_or(Error::NotExist)?;
        assert_eq!(dir_ino, ino);
        // To rmdir we need the node to exist, be a DT_DIR and have no children
        if dir.typ() != libc::DT_DIR {
            let err = Error::WrongType{typ:dir.typ(), expected: libc::DT_DIR};
            self.map.insert(ino, dir);
            return Err(err);
        }
        if dir.num_children() != 0 {
            self.map.insert(ino, dir);
            return Err(Error::NotEmpty);
        }

        // node is a directory and is empty. Ok to remove it. The
        // parent must exist, otherwise panic because the table is
        // corrupt.
        let mut parent = self.get_mut(dir.parent_ino).unwrap();
        parent.remove_child(name);
        Ok(())
    }


    /// Remove a file from a directory
    pub fn unlink(&self, parent_ino: Ino, name: &OsStr) -> Result<(), Error> {

        let name = name.to_str().ok_or_else(|| Error::InvalidName)?;

        let mut parent = self.get_mut(parent_ino)?;
        assert_eq!(parent.typ() , libc::DT_DIR);
        let info = match parent.remove_child_if(name, |_name,info| {info.typ != libc::DT_DIR }) {
            None => {return Err(Error::NotExist);},
            Some((_name, ino)) => ino,
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
            Err(err) => {return Err(err);}
        };

        if nlink == 0 {
            self.map.remove(&info.ino);
        }

        Ok(())
    }
}

impl Error {
    pub fn raw_os_error(&self) -> libc::c_int {
        match self {
            Error::NotExist => libc::ENOENT,
            Error::Unavailable => libc::EWOULDBLOCK,
            Error::Exists => libc::EEXIST,
            Error::NotEmpty => libc::ENOTEMPTY,
            Error::WrongType{typ, expected} => {
                match (*typ, *expected) {
                    (libc::DT_DIR, _) => libc::EISDIR,
                    (libc::DT_REG, libc::DT_DIR) => libc::ENOTDIR,
                    (_,_) => libc::EIO,
                }
            },
            Error::InvalidName => libc::EINVAL,
        }

    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{DirectoryTable, Error};
    use std::ffi::{OsStr, OsString};

    #[test]
    fn root() -> Result<(), super::Error>{
        // let root = DirEntry::root(0,0,0o700);
        // assert_eq!(root.ino(), super::ROOT_INO);
        let table = DirectoryTable::new(0,0,0o700);
        assert_eq!(table.root_ino(), super::ROOT_INO);
        let rt = table.get(table.root_ino())?;
        assert_eq!(rt.ino(), super::ROOT_INO);
        assert_eq!(rt.attr().st_nlink, 2);
        Ok(())
    }

    fn root_table() ->  Arc<DirectoryTable> {
        DirectoryTable::new(0,0,0o700)
    }

    /// from https://stackoverflow.com/a/66805203
    fn get_random_string(len: usize) -> OsString {
        use rand::Rng;
        let s:String = rand::thread_rng()
            .sample_iter::<char, _>(rand::distributions::Standard)
            .take(len)
            .collect();
        let mut out = OsString::new();
        out.push(s);
        out
    }

    #[test]
    fn mkdir() -> Result<(), Error> {
        let table = root_table();
        let stat = table.mkdir(OsStr::new("test"), 0, 0)?;
        assert_eq!(stat.st_nlink, 2);
        assert_eq!(*table.get(stat.st_ino).unwrap().attr() ,stat);
        let root = table.get(table.root_ino())?;
        assert_eq!(root.get_child_name(stat.st_ino).unwrap_or_default(), "test");
        Ok(())
    }

    #[test]
    fn mknod() -> Result<(), Error> {
        let table = root_table();
        let mode = 0o700;

        for j in 1..100{
            let parent_ino = table.mkdir(&get_random_string(j), 0, 0)?.st_ino;
            for i in 1..100 {
                let name = get_random_string(1+i/10);
                let child_stat = table.mknod(&name, mode, parent_ino)?;
                {
                    let parent = table.get(parent_ino).unwrap();
                    assert_eq!(parent.attr().st_nlink as usize, 2+i);
                    assert_eq!(parent.num_children(), i);
                    assert_eq!(parent.get_child_name(child_stat.st_ino).unwrap_or_default(),
                               name.to_string_lossy());
                }
                let child = table.get(child_stat.st_ino).unwrap();
                assert_eq!(child.attr().st_nlink, 1);
                assert_eq!(child.parent_ino, parent_ino);
                assert_eq!(child.attr().st_mode,   libc::S_IFREG | mode);
                assert_eq!(child.typ(), child.info().typ);
                assert_eq!(child.info().typ, libc::DT_UNKNOWN);
                assert_ne!(child.attr().st_atime, 0);

            }
        }
        Ok(())
    }

    #[test]
    fn mknod_duplicate() -> Result<(), Error> {
        let table = root_table();
        let mode = 0o700;
        let parent_ino = table.mkdir(OsStr::new("test"), 0, 0)?.st_ino;
        // attempt to make the file twice, the second should fail and
        // leave us with one child
        table.mknod(OsStr::new("file"), mode, parent_ino)?;
        let result = table.mknod(OsStr::new("file"), mode, parent_ino);
        assert!(result.is_err());
        let parent = table.get(parent_ino).unwrap();
        assert_eq!(parent.num_children(), 1);

        Ok(())
    }

    #[test]
    #[should_panic]
    fn rmdir_nonempty() {
        let table = root_table();
        let parent_ino = table.mkdir(OsStr::new("test_dir"), 0, 0).unwrap().st_ino;
        let _ = table.mknod(OsStr::new("file"), 0o700, parent_ino);
        table.rmdir(table.root_ino(), OsStr::new("test_dir")).unwrap();
    }

    #[test]
    fn rmdir_empty() -> Result<(), Error> {
        let table = root_table();
        let parent_ino = table.mkdir(OsStr::new("test_dir"), 0, 0)?.st_ino;
        assert_eq!(table.get(table.root_ino()).unwrap().num_children(), 1);
        table.rmdir(table.root_ino(), OsStr::new("test_dir"))?;
        assert!(table.get(parent_ino).is_err());
        assert_eq!(table.get(table.root_ino()).unwrap().num_children(), 0);
        Ok(())
    }

    #[test]
    fn unlink_exists() -> Result<(), Error> {
        let table = root_table();
        let parent_ino = table.mkdir(OsStr::new("test_dir"), 0,0)?.st_ino;
        let child_ino = table.mknod(OsStr::new("test_file"), 0o700, parent_ino)?.st_ino;
        eprintln!("Running test");
        if let Ok(parent) = table.get(parent_ino) {
            eprintln!("Added one child");
            assert_eq!(parent.value().num_children(), 1);
        } else {
            panic!();
        }
        table.unlink(parent_ino, OsStr::new("test_file"))?;
        if let Ok(parent) = table.get(parent_ino) {
            eprintln!("child removed");
            assert_eq!(parent.value().num_children(), 0);
        } else {
            panic!();
        }

        let child = table.get(child_ino);
        assert!(child.is_err());

        Ok(())
    }

    #[test]
    fn unlink_no_exist() -> Result<(), Error> {
        let table = root_table();
        let parent_ino = table.mkdir(OsStr::new("test_dir"), 0,0)?.st_ino;
        let result = table.unlink(parent_ino, OsStr::new("fake_name"));
        assert!(result.is_err());
        let parent = table.get(parent_ino).unwrap();
        assert_eq!(parent.num_children(), 0);
        assert_eq!(parent.attr().st_nlink, 2);
        Ok(())
    }

    #[test]
    fn unliknk_dir() -> Result<(), Error> {
        let table = root_table();
        table.mkdir(OsStr::new("test_dir"), 0,0)?;
        let result = table.unlink(table.root_ino(), OsStr::new("test_dir"));
        assert!(result.is_err());
        let root = table.get(table.root_ino()).unwrap();
        assert_eq!(root.num_children(), 1);
        Ok(())
    }


    #[test]
    fn readdir() -> Result<(), Error> {
        let table = root_table();
        let mode = 0o700;
        let parent_ino = table.mkdir(OsStr::new("test"), 0, 0)?.st_ino;
        let child_ino = table.mknod(OsStr::new("file"), mode, parent_ino)?.st_ino;

        let correct_entries = vec![
            (".",    super::EntryInfo {ino: parent_ino,       typ: libc::DT_DIR},),
            ("..",   super::EntryInfo {ino: table.root_ino(), typ: libc::DT_DIR},),
            ("file", super::EntryInfo {ino: child_ino,        typ: libc::DT_UNKNOWN},),
        ];

        let dir = table.get(parent_ino).unwrap();

        for (i, (name, ent)) in dir.iter().enumerate() {
            assert_eq!(name, correct_entries[i].0);
            assert_eq!(ent, correct_entries[i].1);
        }

        Ok(())
    }
}
