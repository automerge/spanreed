use crate::interfaces::{Message, NetworkError, RepoId, RepoMessage};
use crate::repo::RepoHandle;
use futures::{Sink, SinkExt, Stream, StreamExt};

/// Which direction a connection passed to [`crate::RepoHandle::new_remote_repo`] is going
pub enum ConnDirection {
    Incoming,
    Outgoing,
}

impl RepoHandle {
    pub async fn connect_stream<S, SendErr, RecvErr>(
        &self,
        stream: S,
        direction: ConnDirection,
    ) -> Result<(), NetworkError>
    where
        SendErr: std::error::Error + Send + Sync + 'static,
        RecvErr: std::error::Error + Send + Sync + 'static,
        S: Sink<Message, Error = SendErr>
            + Stream<Item = Result<Message, RecvErr>>
            + Send
            + 'static,
    {
        let (mut sink, mut stream) = stream.split();

        let other_id = self.handshake(&mut stream, &mut sink, direction).await?;
        tracing::trace!(?other_id, repo_id=?self.get_repo_id(), "Handshake complete");

        let stream = stream.map({
            let repo_id = self.get_repo_id().clone();
            move |msg| match msg {
                Ok(Message::Repo(repo_msg)) => {
                    tracing::trace!(?repo_msg, repo_id=?repo_id, "Received repo message");
                    Ok(repo_msg)
                }
                Ok(m) => {
                    tracing::warn!(?m, repo_id=?repo_id, "Received non-repo message");
                    Err(NetworkError::Error)
                }
                Err(e) => {
                    tracing::error!(?e, repo_id=?repo_id, "Error receiving repo message");
                    Err(NetworkError::Error)
                }
            }
        });

        let sink = sink
            .with_flat_map::<RepoMessage, _, _>(|msg| match msg {
                RepoMessage::Sync { .. } => futures::stream::iter(vec![Ok(Message::Repo(msg))]),
                _ => futures::stream::iter(vec![]),
            })
            .sink_map_err(|e| {
                tracing::error!(?e, "Error sending repo message");
                NetworkError::Error
            });

        self.new_remote_repo(other_id, Box::new(stream), Box::new(sink));

        Ok(())
    }

    async fn handshake<Str, Snk, SendErr, RecvErr>(
        &self,
        stream: &mut Str,
        sink: &mut Snk,
        direction: ConnDirection,
    ) -> Result<RepoId, NetworkError>
    where
        SendErr: std::error::Error + Send + Sync + 'static,
        RecvErr: std::error::Error + Send + Sync + 'static,
        Str: Stream<Item = Result<Message, RecvErr>> + Unpin,
        Snk: Sink<Message, Error = SendErr> + Unpin,
    {
        match direction {
            ConnDirection::Incoming => {
                if let Some(msg) = stream.next().await {
                    let other_id = match msg {
                        Ok(Message::Join(other_id)) => other_id,
                        _ => return Err(NetworkError::Error),
                    };
                    let msg = Message::Peer(self.get_repo_id().clone());
                    sink.send(msg).await.map_err(|_| NetworkError::Error)?;
                    Ok(other_id)
                } else {
                    Err(NetworkError::Error)
                }
            }
            ConnDirection::Outgoing => {
                let msg = Message::Join(self.get_repo_id().clone());
                sink.send(msg).await.map_err(|_| NetworkError::Error)?;
                if let Some(Ok(Message::Peer(other_id))) = stream.next().await {
                    Ok(other_id)
                } else {
                    Err(NetworkError::Error)
                }
            }
        }
    }
}
