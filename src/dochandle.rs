use crate::interfaces::{DocumentId, RepoId};
use crate::repo::{new_repo_future_with_resolver, RepoError, RepoEvent, RepoFuture};
use automerge::Automerge;
use crossbeam_channel::Sender;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A wrapper around a document shared between a handle and the repo.
#[derive(Clone, Debug)]
pub(crate) struct SharedDocument {
    pub automerge: Automerge,
}

#[derive(Debug)]
/// A handle to a document, held by the client(s).
pub struct DocHandle {
    /// Document used by the handle for local editing.
    shared_document: Arc<RwLock<SharedDocument>>,
    /// Ref count for handles.
    handle_count: Arc<AtomicUsize>,
    /// Channel used to send events back to the repo.
    repo_sender: Sender<RepoEvent>,
    document_id: DocumentId,
    local_repo_id: RepoId,
}

impl Clone for DocHandle {
    fn clone(&self) -> Self {
        // Increment handle count.
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        DocHandle::new(
            self.repo_sender.clone(),
            self.document_id.clone(),
            self.shared_document.clone(),
            self.handle_count.clone(),
            self.local_repo_id.clone(),
        )
    }
}

impl Drop for DocHandle {
    fn drop(&mut self) {
        // Close the document when the last handle drops.
        // TODO: turn this into a `delete` concept,
        // based on an explicit method call(not drop),
        // which would clear storage as well?
        if self.handle_count.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.repo_sender
                .send(RepoEvent::DocClosed(self.document_id.clone()))
                .expect("Failed to send doc close event.");
        }
    }
}

impl DocHandle {
    pub(crate) fn new(
        repo_sender: Sender<RepoEvent>,
        document_id: DocumentId,
        shared_document: Arc<RwLock<SharedDocument>>,
        handle_count: Arc<AtomicUsize>,
        local_repo_id: RepoId,
    ) -> Self {
        DocHandle {
            shared_document,
            repo_sender,
            document_id,
            handle_count,
            local_repo_id,
        }
    }

    pub fn local_repo_id(&self) -> RepoId {
        self.local_repo_id.clone()
    }

    pub fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    /// Run a closure over a mutable reference to the document,
    /// returns the result of calling the closure.
    /// Important: if `save` is called on the document inside the closure,
    /// no saving via the storage adapter will be triggered.
    pub fn with_doc_mut<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut Automerge) -> T,
    {
        let res = {
            let mut state = self.shared_document.write();
            f(&mut state.automerge)
        };
        self.repo_sender
            .send(RepoEvent::DocChange(self.document_id.clone()))
            .expect("Failed to send doc change event.");
        res
    }

    /// Run a closure over a immutable reference to the document,
    /// returns the result of calling the closure.
    pub fn with_doc<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&Automerge) -> T,
    {
        let res = {
            let state = self.shared_document.read();
            f(&state.automerge)
        };
        res
    }

    /// Returns a future that will resolve when the document has changed,
    /// either via another handle, or by applying a sync messsage.
    /// TODO: check sync message and docs following mutable calls,
    /// and only resolve the future when there was an actual change.
    pub fn changed(&self) -> RepoFuture<Result<(), RepoError>> {
        let (fut, observer) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::AddChangeObserver(
                self.document_id.clone(),
                observer,
            ))
            .expect("Failed to send doc change event.");
        fut
    }
}
