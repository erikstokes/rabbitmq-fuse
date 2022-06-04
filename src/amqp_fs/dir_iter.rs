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
