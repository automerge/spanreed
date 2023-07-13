use std::sync::{Arc, Mutex};

use futures::{future::BoxFuture, FutureExt, TryFutureExt};

use crate::{DocumentId, Storage, StorageError};

/// A wrapper around [`crate::fs_store::FsStore`] that implements [`crate::Storage`]
pub struct FsStorage {
    inner: Arc<Mutex<crate::fs_store::FsStore>>,
    handle: tokio::runtime::Handle,
}

impl FsStorage {
    /// Create a new [`FsStorage`] from a [`std::path::PathBuf`]
    ///
    /// # Errors
    ///
    /// This will attempt to create the root directory and throw an error if
    /// it does not exist.
    ///
    /// # Panics
    ///
    /// If there is not a tokio runtime available
    pub fn open(root: std::path::PathBuf) -> Result<Self, std::io::Error> {
        let handle = tokio::runtime::Handle::current();
        let inner = Arc::new(Mutex::new(crate::fs_store::FsStore::open(root)?));
        Ok(Self { inner, handle })
    }
}

impl Storage for FsStorage {
    fn get(
        &self,
        id: crate::DocumentId,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().get(&inner_id))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error reading chunks from filesystem");
                StorageError::Error
            })
            .boxed()
    }

    fn list_all(&self) -> BoxFuture<'static, Result<Vec<DocumentId>, StorageError>> {
        let inner = Arc::clone(&self.inner);
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().list())
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, "error listing all documents");
                StorageError::Error
            })
            .boxed()
    }

    fn append(
        &self,
        id: crate::DocumentId,
        changes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().append(&inner_id, &changes))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error appending chunk to filesystem");
                StorageError::Error
            })
            .boxed()
    }

    fn compact(
        &self,
        id: crate::DocumentId,
        full_doc: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().compact(&inner_id, &full_doc))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error compacting chunk to filesystem");
                StorageError::Error
            })
            .boxed()
    }
}

fn handle_joinerror<T>(
    result: Result<Result<T, crate::fs_store::Error>, tokio::task::JoinError>,
) -> Result<T, Error> {
    match result {
        Ok(r) => r.map_err(Error::FsStorage),
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                Err(Error::SpawnBlockingCancelled)
            }
        }
    }
}

enum Error {
    SpawnBlockingCancelled,
    FsStorage(crate::fs_store::Error),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SpawnBlockingCancelled => write!(f, "tokio spawn_blocking cancelled"),
            Error::FsStorage(e) => e.fmt(f),
        }
    }
}
