use automerge::transaction::Transactable;
use automerge::{AutoSerde, ReadDoc};
use automerge_repo::{ConnDirection, Repo, RepoHandle, Storage};
use automerge_repo::{DocumentId, StorageError};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_macros::debug_handler;
use clap::Parser;
use futures::future::{BoxFuture, TryFutureExt};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot::{channel as oneshot, Sender as OneShot};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    http_run_ip: String,
    #[arg(long)]
    tcp_run_ip: Option<String>,
    #[arg(long)]
    other_ip: Option<String>,
}

struct AppState {
    repo_handle: RepoHandle,
}

#[debug_handler]
async fn request_doc(State(state): State<Arc<AppState>>, Json(document_id): Json<DocumentId>) {
    state
        .repo_handle
        .request_document(document_id)
        .await
        .unwrap();
}

#[debug_handler]
async fn new_doc(State(state): State<Arc<AppState>>) -> Json<DocumentId> {
    let doc_handle = state.repo_handle.new_document();
    let our_id = state.repo_handle.get_repo_id();
    doc_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "repo_id", format!("{}", our_id))
            .expect("Failed to change the document.");
        tx.commit();
    });
    let doc_id = doc_handle.document_id();
    Json(doc_id)
}

async fn get_doc(
    State(state): State<Arc<AppState>>,
    Path(doc_id): Path<DocumentId>,
) -> (StatusCode, Json<serde_json::Value>) {
    let doc_handle = state.repo_handle.request_document(doc_id).await.unwrap();
    let value = doc_handle.with_doc(|doc| serde_json::to_value(AutoSerde::from(doc)).unwrap());
    (StatusCode::OK, Json(value))
}

#[derive(Debug)]
enum StorageRequest {
    Load(DocumentId, OneShot<Option<Vec<u8>>>),
    Append(DocumentId, Vec<u8>, OneShot<()>),
    Compact(DocumentId, Vec<u8>, OneShot<()>),
    ListAll(OneShot<Vec<DocumentId>>),
}

#[derive(Clone, Debug)]
pub struct AsyncInMemoryStorage {
    chan: Sender<StorageRequest>,
}

impl AsyncInMemoryStorage {
    pub fn new() -> Self {
        let mut documents: HashMap<DocumentId, Vec<u8>> = Default::default();
        let (doc_request_sender, mut doc_request_receiver) = channel::<StorageRequest>(1);
        tokio::spawn(async move {
            loop {
                while let Some(request) = doc_request_receiver.recv().await {
                    match request {
                        StorageRequest::ListAll(sender) => {
                            let result = documents.keys().cloned().collect();
                            let _ = sender.send(result);
                        }
                        StorageRequest::Load(doc_id, sender) => {
                            let result = documents.get(&doc_id).cloned();
                            let _ = sender.send(result);
                        }
                        StorageRequest::Append(doc_id, mut data, sender) => {
                            let entry = documents.entry(doc_id).or_insert_with(Default::default);
                            entry.append(&mut data);
                            let _ = sender.send(());
                        }
                        StorageRequest::Compact(doc_id, data, sender) => {
                            let _entry = documents
                                .entry(doc_id)
                                .and_modify(|entry| *entry = data)
                                .or_insert_with(Default::default);
                            let _ = sender.send(());
                        }
                    }
                }
            }
        });
        AsyncInMemoryStorage {
            chan: doc_request_sender,
        }
    }
}

impl Storage for AsyncInMemoryStorage {
    fn get(&self, id: DocumentId) -> BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Load(id, tx))
            .unwrap();
        rx.map_err(|_| StorageError::Error).boxed()
    }

    fn list_all(&self) -> BoxFuture<'static, Result<Vec<DocumentId>, StorageError>> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::ListAll(tx))
            .unwrap();
        rx.map_err(|_| StorageError::Error).boxed()
    }

    fn append(
        &self,
        id: DocumentId,
        changes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Append(id, changes, tx))
            .unwrap();
        rx.map_err(|_| StorageError::Error).boxed()
    }

    fn compact(
        &self,
        id: DocumentId,
        full_doc: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Compact(id, full_doc, tx))
            .unwrap();
        rx.map_err(|_| StorageError::Error).boxed()
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.tcp_run_ip;
    let other_ip = args.other_ip;

    // Create a repo.
    let repo = Repo::new(None, Box::new(AsyncInMemoryStorage::new()));
    let repo_handle = repo.run();
    let repo_handle_clone = repo_handle.clone();

    let app_state = Arc::new(AppState {
        repo_handle: repo_handle.clone(),
    });
    let app = Router::new()
        .route("/new_doc", get(new_doc))
        .route("/request_doc", post(request_doc))
        .route("/docs/:id", get(get_doc))
        .with_state(app_state);
    let serve =
        axum::Server::bind(&args.http_run_ip.parse().unwrap()).serve(app.into_make_service());

    if let Some(run_ip) = run_ip {
        // Start a server.
        let handle = Handle::current();
        let repo_clone = repo_handle.clone();
        handle.spawn(async move {
            let listener = TcpListener::bind(run_ip).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((socket, addr)) => {
                        repo_clone
                            .connect_tokio_io(addr, socket, ConnDirection::Incoming)
                            .await
                            .unwrap();
                    }
                    Err(e) => println!("couldn't get client: {:?}", e),
                }
            }
        });
    } else {
        // Start a client.
        // Spawn a task connecting to the other peer.
        let other_ip = other_ip.unwrap();
        let stream = loop {
            // Try to connect to a peer
            let res = TcpStream::connect(other_ip.clone()).await;
            if res.is_err() {
                continue;
            }
            break res.unwrap();
        };
        repo_handle
            .connect_tokio_io(other_ip, stream, ConnDirection::Outgoing)
            .await
            .unwrap();
    }

    tokio::select! {
        _ = serve.fuse() => {},
        _ = tokio::signal::ctrl_c().fuse() => {
            let synced_docs = repo_handle_clone.list_all().await.unwrap();
            for doc_id in synced_docs {
                let doc = repo_handle_clone
                    .request_document(doc_id.clone())
                    .await
                    .unwrap();
                doc.with_doc(|doc| {
                    let val = doc
                        .get(automerge::ROOT, "repo_id")
                        .expect("Failed to read the document.")
                        .unwrap();
                    let val = val.0.to_str().clone().unwrap();
                    println!("Synced: {:?} to {:?}", doc_id, val);
                });
            }
            Handle::current()
                .spawn_blocking(|| {
                    repo_handle.stop().unwrap();
                })
                .await
                .unwrap();
        }
    }
}
