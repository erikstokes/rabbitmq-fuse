use polyfuse::{reply, Request};

pub(crate) enum Reply {
    AttrOut(reply::AttrOut),
    BmapOut(reply::BmapOut),
    EntryOut(reply::EntryOut),
    LkOut(reply::LkOut),
    OpenOut(reply::OpenOut),
    PollOut(reply::PollOut),
    ReaddirOut(reply::ReaddirOut),
    StatfsOut(reply::StatfsOut),
    WriteOut(reply::WriteOut),
    XattrOut(reply::XattrOut),
    ReadOut,
    None(()),
}

impl Reply {
    /// Try to send the result of the FUSE operation. If the result is
    /// [`std::io::ErrorKind::NotFound`], ignore it since it means the calling
    /// process is already gone. # Panics Will panic if any IO error other
    /// than "NotFound" is returned when sending the error code back the
    /// calling process
    pub fn reply(&self, request: &Request) {
        let result = match self {
            Reply::AttrOut(out) => request.reply(out),
            Reply::BmapOut(out) => request.reply(out),
            Reply::EntryOut(out) => request.reply(out),
            Reply::LkOut(out) => request.reply(out),
            Reply::OpenOut(out) => request.reply(out),
            Reply::PollOut(out) => request.reply(out),
            Reply::ReaddirOut(out) => request.reply(out),
            Reply::StatfsOut(out) => request.reply(out),
            Reply::WriteOut(out) => request.reply(out),
            Reply::XattrOut(out) => request.reply(out),
            Reply::ReadOut => todo!(),
            Reply::None(out) => request.reply(out),
        };
        if let Err(e) = result {
            if e.kind() == std::io::ErrorKind::NotFound {
                tracing::debug!("Calling process disconnected before reply could be sent");
            } else {
                panic!("Unexpected error {e} when sending reply");
            }
        }
    }
}

/// Cast each fuse reply type into the correspond enum variant2
macro_rules! from_polyfuse_reply {
    ($t:ident) => {
        impl ::std::convert::From<::polyfuse::reply::$t> for Reply {
            fn from(val: ::polyfuse::reply::$t) -> Self {
                Reply::$t(val)
            }
        }
    };
}

from_polyfuse_reply!(AttrOut);
from_polyfuse_reply!(BmapOut);
from_polyfuse_reply!(EntryOut);
from_polyfuse_reply!(LkOut);
from_polyfuse_reply!(OpenOut);
from_polyfuse_reply!(PollOut);
from_polyfuse_reply!(ReaddirOut);
from_polyfuse_reply!(StatfsOut);
from_polyfuse_reply!(WriteOut);
from_polyfuse_reply!(XattrOut);

impl From<()> for Reply {
    fn from(_: ()) -> Self {
        Self::None(())
    }
}

// impl<'a> From<&'a [u8]> for Reply<'a> {
//     fn from(value: &'a [u8]) -> Self {
//         Self::ReadOut(value)
//     }
// }
