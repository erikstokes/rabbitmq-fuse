use std::sync::Arc;

use anyhow::{Error, Result};
use tokio::sync::{Mutex, OwnedMutexGuard,
                  mpsc::{Sender, Receiver, channel} };
use deadpool::{async_trait, managed::{self, RecycleError}};
use tracing::debug;
use slab::Slab;

type BufferPermit = usize;
type RequestBuffer = Vec<u8>;

pub(crate) struct BufferPool {
    send: Sender<RequestBuffer>,
    recv: Mutex<Receiver<RequestBuffer>>,
    // pool: Vec<Arc<Mutex<Vec<u8>>>>,
}

impl BufferPool {
    pub async fn new(slab: &Slab) -> Self {
        let (send, recv) = channel(size);
        // let pool = Vec::<Mutex<Vec<u8>>>::new();
        for key in slab.iter {
            debug!("Inserting new request buffer");
            send.send(key).await.unwrap();
            // pool.push(Mutex::new(Vec::new()));
        }
        Self{send,
             recv: Mutex::new(recv),
             // pool,
        }
    }
}

#[async_trait]
impl managed::Manager for BufferPool {
    type Type = RequestBuffer;
    type Error = Error;

    async fn create(&self) -> Result<RequestBuffer> {
        debug!("Getting request buffer");
        Ok( self.recv.lock().await.recv().await.unwrap() )
    }

    async fn recycle(&self, buffer: &mut RequestBuffer) -> managed::RecycleResult<Error> {
        debug!("Buffer returned");
        match self.send.send(buffer.clone()).await {
            Err(_) => Err(RecycleError::Message("Failed to recycle buffer".to_string())),
            Ok(()) => Ok(())
        }
    }
}
