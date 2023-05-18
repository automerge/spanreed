use std::fmt::Debug;

use futures::Future;

use super::{DocHandle, PeerId};

pub enum ShareDecision {
    Share,
    DontShare,
}

/// A policy for determining whether to share a document with a peer
pub trait SharePolicy: Clone + Debug {
    type Error: std::error::Error + Send + 'static;
    type DecisionFuture: Future<Output = Result<ShareDecision, Self::Error>>;

    /// Determine whether to share a document with a peer
    fn should_share(&self, doc: DocHandle, peer: PeerId) -> Self::DecisionFuture;
}

#[derive(Clone, Debug)]
pub struct ShareAll;

impl ShareAll {
    pub fn new() -> Self {
        Self {}
    }
}

impl SharePolicy for ShareAll {
    type Error = std::convert::Infallible;
    type DecisionFuture = futures::future::Ready<Result<ShareDecision, Self::Error>>;

    fn should_share(&self, _doc: DocHandle, _peer: PeerId) -> Self::DecisionFuture {
        futures::future::ready(Ok(ShareDecision::Share))
    }
}
