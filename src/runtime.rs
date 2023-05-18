use std::any::Any;

use futures::Future;

pub trait RuntimeHandle: Clone + Send + Sync + 'static {
    type JoinErr: JoinError + std::error::Error;
    type JoinFuture<O: Send + 'static>: Future<Output = Result<O, Self::JoinErr>>
        + Send
        + Unpin
        + 'static;
    type SleepFuture: Future<Output = ()> + Send;

    fn spawn<O, F>(&self, f: F) -> Self::JoinFuture<O>
    where
        O: Send + 'static,
        F: Future<Output = O> + Send + 'static;

    fn spawn_blocking<O, F>(&self, f: F) -> Self::JoinFuture<O>
    where
        O: Send + 'static,
        F: FnOnce() -> O + Send + 'static;

    fn sleep(&self, duration: std::time::Duration) -> Self::SleepFuture;
}

pub trait JoinError {
    fn is_panic(&self) -> bool;
    fn into_panic(self) -> Box<dyn Any + Send + 'static>;
}

#[cfg(feature = "tokio")]
mod tokio_runtime {
    use super::*;
    use tokio::task::JoinError as TokioJoinError;

    impl RuntimeHandle for tokio::runtime::Handle {
        type JoinErr = tokio::task::JoinError;
        type JoinFuture<O: Send + 'static> = tokio::task::JoinHandle<O>;
        type SleepFuture = tokio::time::Sleep;

        fn spawn<O, F>(&self, f: F) -> Self::JoinFuture<O>
        where
            O: Send + 'static,
            F: Future<Output = O> + Send + 'static,
        {
            self.spawn(f)
        }

        fn spawn_blocking<O, F>(&self, f: F) -> Self::JoinFuture<O>
        where
            O: Send + 'static,
            F: FnOnce() -> O + Send + 'static,
        {
            self.spawn_blocking(f)
        }

        fn sleep(&self, duration: std::time::Duration) -> Self::SleepFuture {
            let _guard = self.enter();
            tokio::time::sleep(duration)
        }
    }

    impl JoinError for TokioJoinError {
        fn is_panic(&self) -> bool {
            self.is_panic()
        }

        fn into_panic(self) -> Box<dyn Any + Send + 'static> {
            self.into_panic()
        }
    }
}
