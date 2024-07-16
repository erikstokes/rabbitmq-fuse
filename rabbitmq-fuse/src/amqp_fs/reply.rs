use polyfuse::reply;

pub(crate) enum Reply<'a> {
    AttrOut(reply::AttrOut),
    BmapOut(reply::BmapOut),
    EntryOut(reply::EntryOut),
    FileAttr(reply::FileAttr),
    FileLock(reply::FileLock),
    LkOut(reply::LkOut),
    OpenOut(reply::OpenOut),
    PollOut(reply::PollOut),
    ReaddirOut(reply::ReaddirOut),
    Statfs(reply::Statfs),
    StatfsOut(reply::StatfsOut),
    WriteOut(reply::WriteOut),
    XattrOut(reply::XattrOut),
    ReadOut(&'a [u8]),
    None(()),
}

/// Cast each fuse reply type into the correspond enum variant2
macro_rules! from_polyfuse_reply {
    ($t:ident) => {
        impl<'a> ::std::convert::From<::polyfuse::reply::$t> for Reply<'a> {
            fn from(val: ::polyfuse::reply::$t) -> Self {
                Reply::$t(val)
            }
        }
    };
}

from_polyfuse_reply!(AttrOut);
from_polyfuse_reply!(BmapOut);
from_polyfuse_reply!(EntryOut);
from_polyfuse_reply!(FileAttr);
from_polyfuse_reply!(FileLock);
from_polyfuse_reply!(LkOut);
from_polyfuse_reply!(OpenOut);
from_polyfuse_reply!(PollOut);
from_polyfuse_reply!(ReaddirOut);
from_polyfuse_reply!(Statfs);
from_polyfuse_reply!(StatfsOut);
from_polyfuse_reply!(WriteOut);
from_polyfuse_reply!(XattrOut);

impl<'a> From<()> for Reply<'a> {
    fn from(_: ()) -> Self {
        Self::None(())
    }
}

impl<'a> From<&'a [u8]> for Reply<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::ReadOut(value)
    }
}
