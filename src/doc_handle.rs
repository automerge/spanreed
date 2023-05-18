use automerge as am;
use futures::Stream;

use crate::DocumentId;

/// A handle to a document managed by the repository
///
/// This is effectively an async mutex around an automerge document
#[derive(Clone)]
pub struct DocHandle {}

impl DocHandle {
    pub fn id(&self) -> DocumentId {
        todo!()
    }

    /// Wait for this document to be unlocked and then modify it
    ///
    /// Any changes made to the document by `F` will be enqueue to send to all
    /// connected peers who are sharing this document
    pub async fn modify<O, E, F: FnOnce(&am::Automerge) -> Result<O, E>>(
        &mut self,
        f: F,
    ) -> Result<O, E> {
        todo!()
    }

    /// Wait for a document to be unlocked and get a read only ref to it
    pub async fn read(&self) -> am::Automerge {
        todo!()
    }

    /// Wait for a change to occur to this document and return a read only reference to the
    /// document if it did change
    pub async fn changed(&self) -> &am::Automerge {
        todo!()
    }

    /// A stream of ephemeral messages related to this document
    ///
    /// There is no standard for ephemeral messages so each item is just the 
    /// raw bytes of the message
    pub fn ephemera(&self) -> impl Stream<Item=Vec<u8>> {
        futures::stream::empty()
    }
}
