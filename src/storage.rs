use std::{
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
};

use automerge as am;
use futures::Future;

use super::DocumentId;

pub trait Storage: Clone + Debug {
    type Error: std::error::Error + Send + 'static;
    type GetFuture: Future<Output = Result<Option<am::Automerge>, Self::Error>>;
    type ListFuture: Future<Output = Result<Vec<DocumentId>, Self::Error>>;
    type AppendFuture: Future<Output = Result<(), Self::Error>>;
    type CompactFuture: Future<Output = Result<(), Self::Error>>;

    fn get(&self, id: DocumentId) -> Self::GetFuture;
    fn list(&self) -> Self::ListFuture;
    fn append(&self, id: DocumentId, changes: &[u8]) -> Self::AppendFuture;
    fn compact(&self, id: DocumentId) -> Self::CompactFuture;
}

#[derive(Clone, Debug)]
pub struct InMemory {
    docs: Arc<Mutex<HashMap<DocumentId, am::Automerge>>>,
}

impl InMemory {
    pub fn new() -> Self {
        Self {
            docs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Storage for InMemory {
    type Error = std::convert::Infallible;
    type GetFuture = futures::future::Ready<Result<Option<am::Automerge>, Self::Error>>;
    type ListFuture = futures::future::Ready<Result<Vec<DocumentId>, Self::Error>>;
    type AppendFuture = futures::future::Ready<Result<(), Self::Error>>;
    type CompactFuture = futures::future::Ready<Result<(), Self::Error>>;

    fn get(&self, id: DocumentId) -> Self::GetFuture {
        let docs = self.docs.lock().unwrap();
        futures::future::ready(Ok(docs.get(&id).cloned()))
    }

    fn list(&self) -> Self::ListFuture {
        let docs = self.docs.lock().unwrap();
        futures::future::ready(Ok(docs.keys().cloned().collect()))
    }

    fn append(&self, id: DocumentId, changes: &[u8]) -> Self::AppendFuture {
        let mut docs = self.docs.lock().unwrap();
        let doc = docs.entry(id).or_insert_with(|| am::Automerge::new());
        doc.load_incremental(changes).unwrap();
        futures::future::ready(Ok(()))
    }

    fn compact(&self, id: DocumentId) -> Self::CompactFuture {
        // nothing to do as we're not actually storing things
        futures::future::ready(Ok(()))
    }
}

#[derive(Clone, Debug)]
pub struct FsStorage {
    path: std::path::PathBuf,
}

impl FsStorage {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}

impl Storage for FsStorage {
    type Error = std::io::Error;
    type GetFuture =
        Pin<Box<dyn Future<Output = Result<Option<am::Automerge>, Self::Error>> + Send>>;
    type ListFuture = Pin<Box<dyn Future<Output = Result<Vec<DocumentId>, Self::Error>> + Send>>;
    type AppendFuture = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;
    type CompactFuture = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn get(&self, id: DocumentId) -> Self::GetFuture {
        todo!()
    }

    fn list(&self) -> Self::ListFuture {
        todo!()
    }

    fn append(&self, id: DocumentId, changes: &[u8]) -> Self::AppendFuture {
        todo!()
    }

    fn compact(&self, id: DocumentId) -> Self::CompactFuture {
        todo!()
    }
}
