use automerge_repo::{NetworkError, RepoId, RepoMessage};
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Network<T> {
    buffer: Arc<Mutex<VecDeque<T>>>,
    stream_waker: Arc<Mutex<Option<Waker>>>,
    outgoing: Arc<Mutex<VecDeque<T>>>,
    sink_waker: Arc<Mutex<Option<Waker>>>,
    sender: Sender<(RepoId, RepoId)>,
    closed: Arc<Mutex<bool>>,
}

impl<T> Network<T> {
    pub fn new(sender: Sender<(RepoId, RepoId)>) -> Self {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let stream_waker = Arc::new(Mutex::new(None));
        let sink_waker = Arc::new(Mutex::new(None));
        let outgoing = Arc::new(Mutex::new(VecDeque::new()));
        Network {
            buffer,
            stream_waker,
            outgoing,
            sender,
            sink_waker,
            closed: Arc::new(Mutex::new(false)),
        }
    }

    pub fn receive_incoming(&self, event: T) {
        self.buffer.lock().push_back(event);
        if let Some(waker) = self.stream_waker.lock().take() {
            waker.wake();
        }
    }

    pub fn take_outgoing(&self) -> T {
        let message = self.outgoing.lock().pop_front().unwrap();
        if let Some(waker) = self.sink_waker.lock().take() {
            waker.wake();
        }
        message
    }

    pub fn closed(&self) -> bool {
        *self.closed.lock()
    }
}

impl Stream for Network<RepoMessage> {
    type Item = Result<RepoMessage, NetworkError>;
    fn poll_next(
        self: Pin<&mut Network<RepoMessage>>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RepoMessage, NetworkError>>> {
        *self.stream_waker.lock() = Some(cx.waker().clone());
        if let Some(event) = self.buffer.lock().pop_front() {
            Poll::Ready(Some(Ok(event)))
        } else {
            Poll::Pending
        }
    }
}

impl Sink<RepoMessage> for Network<RepoMessage> {
    type Error = NetworkError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: RepoMessage) -> Result<(), Self::Error> {
        let (from_repo_id, to_repo_id) = match &item {
            RepoMessage::Sync {
                from_repo_id,
                to_repo_id,
                ..
            } => (from_repo_id.clone(), to_repo_id.clone()),
            _ => todo!(),
        };

        self.outgoing.lock().push_back(item);
        if self
            .sender
            .blocking_send((from_repo_id, to_repo_id))
            .is_err()
        {
            return Err(NetworkError::Error);
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        *self.closed.lock() = true;
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
