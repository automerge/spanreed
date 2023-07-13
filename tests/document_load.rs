extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{DocumentId, Repo};
use std::collections::HashMap;
use test_utils::storage_utils::{AsyncInMemoryStorage, InMemoryStorage};
use tokio::sync::mpsc::channel;

#[test]
fn test_loading_document_found_immediately() {
    let storage = InMemoryStorage::default();
    // Create one repo.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    let expected_repo_id = repo_handle.get_repo_id().clone();
    let doc_data = document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
        doc.save()
    });

    // Add the doc to storage, out of band.
    let doc_id = document_handle.document_id();
    storage.add_document(document_handle.document_id(), doc_data);
    drop(document_handle);

    // Shut down the repo.
    repo_handle.stop().unwrap();

    // Create another repo.
    let repo = Repo::new(None, Box::new(storage));
    let repo_handle = repo.run();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let load_fut = repo_handle.load(doc_id);
    rt.spawn(async move {
        let equals = load_fut.await.unwrap().unwrap().with_doc(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            val.0.to_str().unwrap() == format!("{}", expected_repo_id)
        });
        if equals {
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
    // Shut down the repo.
    repo_handle.stop().unwrap();
}

#[test]
fn test_loading_document_found_async() {
    let storage = InMemoryStorage::default();
    // Create one repo.
    let repo = Repo::new(None, Box::new(storage));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    let expected_repo_id = repo_handle.get_repo_id().clone();
    let doc_data = document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
        doc.save()
    });

    // Add the doc to storage, out of band.
    let doc_id = document_handle.document_id();
    let mut docs = HashMap::new();
    docs.insert(doc_id.clone(), doc_data);

    // Shut down the repo.
    drop(document_handle);
    repo_handle.stop().unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Create another repo, using the async storage.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let async_storage = AsyncInMemoryStorage::new(docs, false);
        let repo = Repo::new(None, Box::new(async_storage));
        let repo_handle = repo.run();

        // Spawn a task that awaits the requested doc handle.
        tokio::spawn(async move {
            let equals = repo_handle
                .load(doc_id)
                .await
                .unwrap()
                .unwrap()
                .with_doc(|doc| {
                    let val = doc
                        .get(automerge::ROOT, "repo_id")
                        .expect("Failed to read the document.")
                        .unwrap();
                    val.0.to_str().unwrap() == format!("{}", expected_repo_id)
                });
            if equals {
                // Shut down the repo.
                let _ = tokio::task::spawn_blocking(|| {
                    repo_handle.stop().unwrap();
                })
                .await;
                done_sync_sender.send(()).await.unwrap();
            }
        })
        .await
        .unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_loading_document_immediately_not_found() {
    // Empty storage.
    let storage = InMemoryStorage::default();

    // Create a repo.
    let repo = Repo::new(None, Box::new(storage));
    let repo_handle = repo.run();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let doc_id = DocumentId(String::from("Test"));
    let load_fut = repo_handle.load(doc_id);
    rt.spawn(async move {
        let not_found = load_fut.await.unwrap().is_none();
        if not_found {
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
    // Shut down the repo.
    repo_handle.stop().unwrap();
}

#[test]
fn test_loading_document_not_found_async() {
    // Empty docs.
    let docs = HashMap::new();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Create a repo, using the async storage.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let async_storage = AsyncInMemoryStorage::new(docs, false);
        let repo = Repo::new(None, Box::new(async_storage));
        let repo_handle = repo.run();

        // Spawn a task that awaits the requested doc handle.
        tokio::spawn(async move {
            let doc_id = DocumentId(String::from("Test"));
            let not_found = repo_handle.load(doc_id).await.unwrap().is_none();
            if not_found {
                // Shut down the repo.
                let _ = tokio::task::spawn_blocking(|| {
                    repo_handle.stop().unwrap();
                })
                .await;
                done_sync_sender.send(()).await.unwrap();
            }
        })
        .await
        .unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();
}
