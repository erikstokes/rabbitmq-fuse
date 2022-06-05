use std::ops::Deref;

use super::table::DirEntry;
use super::table::EntryInfo;
use super::table::DirectoryTable;
use super::table::Ino;

pub(in crate::amqp_fs) struct DirIterator<'a> {
    child_inos: Vec<Ino>,
    table: &'a DirectoryTable,
    dir: &'a DirEntry,
    position: usize,
}


impl<'a> DirIterator<'a> {
    pub fn new(table: &'a DirectoryTable, dir: &'a DirEntry) -> Self {
            Self {
                child_inos: dir.child_inos(),
                table,
                dir,
                position: 0,
            }
    }
}

impl<'a> Iterator for DirIterator<'a> {
    type Item = (String, EntryInfo);

    /// Step through the children of the directory. "." and ".." are
    /// the first and second entries, after that the order is random.
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.child_inos.len()+2 {
            return None;
        }
        let next_dir = match self.position {
            // first return "."
            0 =>  Some(( ".".to_string(), self.dir.info().clone() )),
            // then return ".."
            1 => {
                let parent = self.table.map.get(&self.dir.parent_ino).unwrap();
                Some(( "..".to_string(), parent.value().info().clone() ))
            },
            // The everything else
            _ => {
                let value = self.table.map.get(&self.child_inos[self.position-2]).unwrap();
                Some((value.name().to_string(), value.info().clone() ))
            },
        };
        self.position += 1;
        next_dir
    }
}

#[cfg(test)]
mod test {

    use crate::amqp_fs::{table::EntryInfo, dir_iter::DirIterator};

    use super::{DirEntry, DirectoryTable};

    fn root_table() -> DirectoryTable {
        let root = DirEntry::root(0,0,0o700);
        DirectoryTable::new(root)
    }

    #[test]
    fn ls_dir() -> Result<(), libc::c_int>{
        let table = root_table();
        let mode = 0o700;
        let parent_ino = table.mkdir("test", 0, 0)?.st_ino;
        let child_ino = table.mknod("file", mode, parent_ino)?.st_ino;

        let correct_entries = vec!((".",    super::EntryInfo{ino: parent_ino,       typ: libc::DT_DIR}),
                                   ("..",   super::EntryInfo{ino: table.root_ino(), typ: libc::DT_DIR}),
                                   ("file", super::EntryInfo{ino: child_ino,        typ: libc::DT_UNKNOWN}),
        );

        let dir =  table.get(parent_ino).unwrap();

        for  (i,(name, ent)) in DirIterator::new(&table, &dir).enumerate() {
            assert_eq!(name, correct_entries[i].0);
            assert_eq!(ent, correct_entries[i].1);
        }

        Ok(())

    }
}
