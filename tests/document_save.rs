extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::Repo;
use test_utils::storage_utils::AsyncInMemoryStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_simple_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default(), false);
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let repo_handle = repo.run();

            // Create a document for one repo.
            let document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document.
            let expected_value = format!("{}", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "repo_id", expected_value.clone())
                    .expect("Failed to change the document.");
                tx.commit();
            });

            drop(document_handle);
            // Shut down the repo.
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_save_on_shutdown() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        // Step by step storage.
        let storage = AsyncInMemoryStorage::new(Default::default(), true);
        // Create one repo.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        // Create a document for one repo.
        let document_handle = repo_handle.new_document();

        // Edit the document.
        let expected_value = format!("{}", repo_handle.get_repo_id());
        document_handle.with_doc_mut(|doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "repo_id", expected_value.clone())
                .expect("Failed to change the document.");
            tx.commit();
        });
        drop(document_handle);

        use tokio::sync::oneshot::channel as oneshot;

        let (tx, rx) = oneshot();

        tokio::spawn(async move {
            rx.await.unwrap();
            // Allow storage to save.
            storage.process_next_result().await;
        });

        // Shut down the repo.
        tokio::task::spawn_blocking(|| {
            // Not completely deterministic, but good enough.
            tx.send(()).unwrap();
            repo_handle.stop().unwrap();
        })
        .await
        .unwrap();

        done_sync_sender.send(()).await.unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_multiple_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default(), false);
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let repo_handle = repo.run();

            // Create a document for one repo.
            let document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document, once.
            document_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::1", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                tx.commit();
            });

            // Edit the document, twice.
            let expected_value = format!("{}::2", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "repo_id", expected_value.clone())
                    .expect("Failed to change the document.");
                tx.commit();
            });

            // Shut down the repo.
            drop(document_handle);
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_compact_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default(), false);
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let repo_handle = repo.run();

            // Create a document for one repo.
            let document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document, once.
            let change_fut = document_handle.changed();
            document_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::1", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                tx.commit();
            });
            change_fut.await.unwrap();

            // Edit the document, again.
            let change_fut = document_handle.changed();
            document_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::2", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                tx.commit();
            });
            change_fut.await.unwrap();

            // Edit the document, enough times to trigger a compact.
            let change_fut = document_handle.changed();
            let expected_value = format!("{}::11", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                for i in 0..12 {
                    let mut tx = doc.transaction();
                    tx.put(
                        automerge::ROOT,
                        "repo_id",
                        format!("{}::{}", repo_handle.get_repo_id(), i),
                    )
                    .expect("Failed to change the document.");
                    tx.commit();
                }
            });
            change_fut.await.unwrap();

            drop(document_handle);
            // Shut down the repo.
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}
