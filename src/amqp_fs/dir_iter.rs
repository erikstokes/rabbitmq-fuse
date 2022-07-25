//! Step through the files in a directory

use std::ops::Deref;
use std::sync::Arc;

use super::table::DirEntry;
use super::table::DirectoryTable;
use super::table::EntryInfo;
use super::table::Ino;
use super::table::Error;


/// Iterator that steps through the children of a directory
///
/// "." and ".." are the first and second entries, after that the
/// order is random. The list of children is frozen when the iteration
/// begins. Another thread calling `mknod` will not affect the output
/// of the iterator
pub(in crate::amqp_fs) struct DirIterator<'a> {
    #[doc(hidden)]
    // child_inos: Vec<(String, EntryInfo)>,
    child_iter: dashmap::iter::Iter<'a, std::string::String, EntryInfo>,

    #[doc(hidden)]
    table: &'a DirectoryTable,

    #[doc(hidden)]
    dir: &'a DirEntry,

    #[doc(hidden)]
    position: usize,
}

impl<'a> DirIterator<'a> {
    /// Create a new iterator from a directory and a table
    pub fn new(dir: &'a DirEntry) -> Self {
        Self {
            // child_inos: dir.child_inos(),
            child_iter: dir.child_iter(),
            table: dir.table(),
            dir,
            position: 0,
        }
    }
}

impl<'a> Iterator for DirIterator<'a> {
    type Item = (String, EntryInfo);

    /// Get the next file in the entry. Regular file entries
    /// immediatly return None
    fn next(&mut self) -> Option<Self::Item> {
        if self.dir.typ() != libc::DT_DIR {
            return None;
        }
        // if self.position >= self.child_inos.len() + 2 {
        //     return None;
        // }
        let next_dir = match self.position {
            // first return "."
            0 => Some((".".to_string(), self.dir.info().clone())),
            // then return ".."
            1 => {
                let parent = self.table.map.get(&self.dir.parent_ino).unwrap();
                Some(("..".to_string(), parent.value().info().clone()))
            }
            // The everything else
            _ => {
                // let value = self
                //     .table
                //     .map
                //     .get(&self.child_inos[self.position - 2])
                //     .unwrap();
                // Some((value.name().to_string(), value.info().clone()))
                self.child_iter.next().map(
                    |item| (item.key().clone(), item.value().clone())
                )
            }
        };
        self.position += 1;
        next_dir
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::amqp_fs::{dir_iter::DirIterator, table::EntryInfo};

    use super::{DirEntry, DirectoryTable, Error};
    use std::ffi::{OsStr, OsString};

    fn root_table() -> Arc<DirectoryTable> {
        DirectoryTable::new(0,0, 0o700)
    }

    #[test]
    fn ls_dir() -> Result<(), Error> {
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

        for (i, (name, ent)) in DirIterator::new(&dir).enumerate() {
            assert_eq!(name, correct_entries[i].0);
            assert_eq!(ent, correct_entries[i].1);
        }

        Ok(())
    }
}
