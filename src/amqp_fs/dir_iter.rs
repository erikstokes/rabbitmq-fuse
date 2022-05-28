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
    type Item = EntryInfo;

    /// Step through the children of the directory. "." and ".." are
    /// the first and second entries, after that the order is random.
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.child_inos.len()+2 {
            return None;
        }
        let next_dir = match self.position {
            // first return "."
            0 =>  EntryInfo{name: ".".to_string(), ino: self.dir.ino(), typ: self.dir.typ() },
            // then return ".."
            1 => {
                let parent = self.table.map.get(&self.dir.parent_ino).unwrap();
                EntryInfo{name: "..".to_string(), ino:parent.value().ino(), typ:parent.value().typ()}
            },
            // The everything else
            _ => self.table.map.get(&self.child_inos[self.position-2]).unwrap().value().info().clone(),
        };
        self.position += 1;
        Some(next_dir)
    }
}
