//! # Spanreed
//!
//! A library for synchronizing collections of automerge documents
//!
//! Automerge presents a way to manage concurrent changes without a central
//! server and includes a sync protocol to synchronize changes between two peers
//! who are making concurrent changes to a document. This still leaves a lot up
//! to the user though. Typical applications will need to manage many documents,
//! and synchronize them with many peers. `spanreed` implements all this
//! boilerplate for you.
//!
//! The core abstraction in `spanreed` is a [`Repo`]. A [`Repo`] represents a
//! collection of documents stored by a [`Storage`]. [`Repo`]s communicate with
//! each other via a [`futures::Stream`] and [`futures::Sink`] of [`Message`]s.
//! So anywhere you can obtain such a stream+sink connecting you to another
//! machine you can run use [`Repo`].
//!
//! Once you have a [`Repo`] and you are connected to other [`Repo`]s you can
//! obtain and create [`DocHandle`]s. A [`DocHandle`] represents a single
//! automerge document - identified by a [`DocumentId`] - and allows you to
//! make changes to the document and wait on updates to the document.
//!
//! `SharePolicy` represents decisions about whether to share a document with
//! a given peer. The defulat [`ShareAll`] policy shares all documents with all
//! peers.

use automerge as am;
use futures::{Sink, Stream};
use runtime::RuntimeHandle;

pub mod config;
pub use config::RepoConfig;
mod doc_handle;
pub mod runtime;
pub use doc_handle::DocHandle;
pub mod storage;
pub use storage::Storage;
mod share_policy;
pub use share_policy::{ShareAll, ShareDecision, SharePolicy};

/// The ID of a document, must be globally unique
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct DocumentId([u8; 32]);

impl DocumentId {
    pub fn random() -> Self {
        todo!()
    }
}

/// The id of a remote peer
#[derive(Debug)]
pub struct PeerId([u8; 32]);

/// The different channels a message can apply to
pub enum ChannelId {
    /// Sync messages for a document
    Document(DocumentId),
    /// Ephemeral messages for a document
    Ephemeral(DocumentId),
    /// Messages relating to the entire connection
    Sync,
}

/// A [`Repo`] represents a collection of automerge documents which is
/// synchronized with a set of connected peers.
///
/// Create a [`Repo`] with [`Repo::run`], which starts a subroutine which you
/// must drive. Once a [`Repo`] is created you pass connections to it using
/// [`Self::connect`].
///
/// You can obtain [`DocHandle`]s from a repository using [`Repo::load`],
/// [`Repo::create`] or [`Repo::request`].
///
/// When you are done with a [`Repo`] you can shut it down using [`Repo::shutdown`].
///
/// ## [`Storage`], [`Runtime`] and [`SharePolicy`]
///
/// To create a [`Repo`] you will need an instance of [`Storage`], [`Runtime`]
/// and [`SharePolicy`]. There is a built in storage implementation which uses
/// the filesystem but the trait is simple enough that you can implement it
/// yourself if you want to store documents in a database or something.
///
/// [`Runtime`] is a trait which represents the ability to spawn subroutines,
/// there are feature flagged implementations for `tokio` and `async-std`.
///
#[derive(Clone)]
pub struct Repo<Store: Storage, Share: SharePolicy> {
    _phantom: std::marker::PhantomData<(Store, Share)>,
}

/// The return value of [`Repo::run`]
pub struct Running<Store: Storage, Share: SharePolicy, R: RuntimeHandle> {
    /// The handle to the running repository
    pub repo: Repo<Store, Share>,
    /// A subroutine which manages storage tasks and background sync tasks and
    /// must be driven until the repository is shut down
    pub background_driver: R::JoinFuture<Result<(), error::Background<Store::Error, Share::Error>>>,
}

impl<Store: Storage, Share: SharePolicy> Repo<Store, Share> {
    /// Create a new [`Repo`]
    ///
    /// This will spawn a new task onto `runtime` which will manage periodic
    /// storage compaction tasks and background sync tasks. A [`R::JoinFuture`]
    /// representing the spawned task is returned in `Running::storage_driver`
    /// and must be driven to completion.
    async fn run<R: runtime::RuntimeHandle>(
        runtime: R,
        config: config::RepoConfig<Store, Share>,
    ) -> Running<Store, Share, R> {
        // Here we set up periodic storage tasks to compact documents
        todo!()
    }

    #[cfg(feature = "tokio")]
    pub async fn run_tokio(
        config: config::RepoConfig<Store, Share>,
    ) -> Running<Store, Share, tokio::runtime::Handle> {
        let runtime = tokio::runtime::Handle::current();
        Self::run(runtime, config).await
    }

    /// Connect a stream and sink of `crate::Message`
    ///
    /// Driving this future will drive the connection until there is an error or one or the other
    /// end drops
    ///
    /// The `direction` argument determines which side will send the initial `crate::Message::Join`
    /// message. If the direction is `ConnDirection::Outgoing` then this side will send the
    /// message, otherwise this side will wait for the other side to send the join message.
    ///
    /// # Arguments
    ///
    /// * source - An identifier for the source of the connection. Only
    ///   currently used for logging and typically is a
    ///   [`std::net::SocketAddr`]
    /// * stream - The stream + sink of messages representing the connection
    /// * direction - Which side of the connection `stream` is
    ///
    /// # Sharing
    ///
    /// Once the connection is established and we know the other ends peer ID
    /// the repo will query all the documents it knows about using `Store::list` and
    /// then send sync messages for every document for which
    /// `Share::should_share` returns `ShareDecision::Share`
    pub async fn connect<RecvErr, SendErr, Source>(
        self,
        source: Source,
        stream: Store,
        direction: ConnDirection,
    ) -> Result<(), error::RunConnection<SendErr, RecvErr>>
    where
        RecvErr: std::error::Error,
        SendErr: std::error::Error,
        Store: Sink<Message, Error = SendErr> + Stream<Item = Result<Message, RecvErr>>,
        Source: std::fmt::Debug + Eq,
    {
        todo!()
    }

    /// Connect a tokio io object
    ///
    /// This implements a simple length prefixed framing protocol and is intended for use with
    /// stream oriented transports
    #[cfg(feature = "tokio")]
    pub async fn connect_tokio_io<Io, Source>(
        self,
        source: Source,
        io: Io,
        direction: ConnDirection,
    ) -> Result<(), error::RunConnection<error::CodecError, error::CodecError>>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    {
        todo!()
    }

    /// Attempt to load the document given by `id` from storage
    ///
    /// If the document is not found in storage then `Ok(None)` is returned.
    ///
    /// Note that this _does not_ attempt to fetch the document from the
    /// network.
    pub async fn load(&self, id: DocumentId) -> Result<Option<DocHandle>, Store::Error> {
        todo!()
    }

    /// Request a document from a peer in the network
    ///
    /// This will connect to the peers specified by `peers` and run the sync
    /// protocol for the given document ID. Once the sync protocol has run (or
    /// not as the case may be) this function will return the document handle.
    ///
    /// This can be used both to fetch a document you don't have and to request
    /// updates you think another peer has but which you haven't been sent yet.
    ///
    /// # Arguments
    ///
    /// * id - The ID of the document to request
    /// * peer - The peer(s) to request the document from.
    pub async fn request(
        &self,
        id: DocumentId,
        peers: PeerSpec,
    ) -> Result<Option<DocHandle>, error::Request> {
        todo!()
    }

    /// Create a new document, store it, and return a `DocHandle` for it
    ///
    /// In addition to creating the document this will also enqueue sync messages
    /// to every connected peer for whom `Share::should_share` returns `ShareDecision::Share`
    ///
    /// # Errors
    ///
    /// * If a document with this ID already exists in storage
    pub async fn create(&self, doc: am::Automerge) -> Result<DocHandle, error::Create<Store>> {
        todo!()
    }

    /// Announce that we have a given document and wait for the peers specified by `ack_by` to sync
    /// with us
    ///
    /// # Errors
    ///
    /// * If the given document does not exist
    /// * If there is no connection to the peers given in `ack_by`
    pub async fn announce(&self, doc: DocumentId, ack_by: PeerSpec) -> Result<(), error::Announce> {
        todo!()
    }

    /// Shut down the repository
    ///
    /// This will signal to the background task that it should shutdown and
    /// then wait for it to complete
    ///
    /// If the background task has already shutdown this returns immediately. If
    /// another task has already signalled the background task to shutdown then
    /// this will wait for shutdown to complete.
    pub async fn shutdown(self) -> Result<(), error::Shutdown> {
        todo!()
    }

    /// Get the currently connected peers
    pub async fn peers(&self) -> Vec<PeerInfo> {
        todo!()
    }

    /// A stream of events about the peers connected to this repo
    pub fn peer_events(&self) -> impl Stream<Item = PeerEvent> {
        futures::stream::empty()
    }

    /// Wait for the peers specified by `peers` to be connected
    pub async fn peers_connected(&self, peers: PeerSpec) {}
}

/// The messages of the multi-document sync protocol
///
/// The multi-doc sync protocol works like this:
///
/// 1. The connecting peer sends a `Message::Join` containing its peer ID
/// 2. The accepting peer sends a `Message::Peer` containing its peer ID
/// 3. Any time a peer wishes to send a sync for a particular document it sends
///    a `Message::Message` with a channel ID of `ChannelId::Document`
/// 3. Any time a peer wishes to send ephemeral data for a particular document
///    it sends a `Message::Message` with a channel ID of `ChannelId::Ephemeral`
pub enum Message {
    /// Sent by the connecting peer on opening a connection to tell the other
    /// end their peer ID
    Join {
        /// Must always be `ChannelId::Sync`
        channel_id: ChannelId,
        /// The peer ID of the sender
        sender_id: PeerId,
    },
    /// A sync message for a particular document
    Message {
        channel_id: ChannelId,
        sender_id: PeerId,
        target_id: PeerId,
        /// The data of the sync message
        data: Vec<u8>,
    },
    /// Sent by the accepting peer after having received [`Join`] to tell the
    /// connecting peer their peer ID.
    Peer {
        /// The PeerId of the accepting peer
        sender_id: PeerId,
    },
}

/// Which direction a connection passed to [`Repo::connect`] is going
pub enum ConnDirection {
    Incoming,
    Outgoing,
}

pub struct PeerInfo {
    id: PeerId,
    connected_at: std::time::Instant,
    last_seen: std::time::Instant,
}

/// What peers to fetch a document from in [`Repo::request`]
pub enum PeerSpec {
    /// Wait until all connected peers have synced the document
    All,
    /// Wait until at least one peer has synced the document
    Any,
    /// Wait until a particular peer has synced the document
    This(PeerId),
}

pub enum PeerEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
}

mod error {
    #[derive(Debug, thiserror::Error)]
    pub enum RunConnection<SendErr, RecvErr> {
        #[error("failed to receive message: {0}")]
        RecvMessage(RecvErr),
        #[error("failed to send message: {0}")]
        SendMessage(SendErr),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Request {
        #[error("no connection to peer {0:?}")]
        NoConnection(super::PeerId),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Create<S: super::Storage> {
        #[error(transparent)]
        Storage(S::Error),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Background<Store: std::error::Error + 'static, Share: std::error::Error + 'static> {
        #[error(transparent)]
        Storage(Store),
        #[error(transparent)]
        Share(Share),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Shutdown {}

    #[derive(Debug, thiserror::Error)]
    pub enum CodecError {}

    #[derive(Debug, thiserror::Error)]
    pub enum Announce {
        #[error("no connection to peer {0:?}")]
        NoConnection(super::PeerId),
    }
}
