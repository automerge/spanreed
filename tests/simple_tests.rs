extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{Repo, RepoId, RepoMessage};
use std::collections::HashMap;
use test_utils::network_utils::Network;
use test_utils::storage_utils::SimpleStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_repo_stop() {
    // Create the repo.
    let repo = Repo::new(None, Box::new(SimpleStorage));

    // Run the repo in the background.
    let repo_handle = repo.run();

    // Stop the repo.
    repo_handle.stop().unwrap();
}

#[test]
fn test_simple_sync() {
    let (sender, mut network_receiver) = channel(1);
    let mut repo_handles = vec![];
    let mut documents = vec![];
    let mut peers = HashMap::new();
    let (done_sync_sender, mut done_sync_receiver) = channel(1);

    for _ in 1..10 {
        // Create the repo.
        let repo = Repo::new(None, Box::new(SimpleStorage));
        let repo_handle = repo.run();

        // Create a document.
        let doc_handle = repo_handle.new_document();
        doc_handle.with_doc_mut(|doc| {
            let mut tx = doc.transaction();
            tx.put(
                automerge::ROOT,
                "repo_id",
                format!("{}", repo_handle.get_repo_id()),
            )
            .expect("Failed to change the document.");
            tx.commit();
        });
        documents.push(doc_handle);

        repo_handles.push(repo_handle);
    }

    let repo_handles_clone = repo_handles.clone();

    let repo_ids: Vec<RepoId> = repo_handles
        .iter()
        .map(|handle| handle.get_repo_id().clone())
        .collect();
    for repo_handle in repo_handles.iter() {
        for id in repo_ids.iter() {
            // Create the network adapter.
            let network = Network::new(sender.clone());
            repo_handle.new_remote_repo(
                id.clone(),
                Box::new(network.clone()),
                Box::new(network.clone()),
            );
            let entry = peers
                .entry(repo_handle.get_repo_id().clone())
                .or_insert(HashMap::new());
            entry.insert(id.clone(), network);
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.spawn(async move {
        // A router of network messages.
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
                       let peers = peers.get_mut(&from_repo_id).unwrap();
                       let peer = peers.get_mut(&to_repo_id).unwrap();
                       peer.take_outgoing()
                   };
                   match incoming {
                       RepoMessage::Sync {
                           from_repo_id,
                           to_repo_id,
                           document_id,
                           message,
                       } => {
                           let peers = peers.get_mut(&to_repo_id).unwrap();
                           let peer = peers.get_mut(&from_repo_id).unwrap();
                          peer.receive_incoming(RepoMessage::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                               });
                       },
                       _ => todo!(),
                   }
               },
            }
        }
    });

    rt.spawn(async move {
        let mut synced = 0;
        for doc_handle in documents {
            for repo_handle in repo_handles_clone.iter() {
                repo_handle
                    .request_document(doc_handle.document_id())
                    .await
                    .unwrap();
                synced += 1;
            }
        }
        assert_eq!(synced, 81);
        let _ = done_sync_sender.try_send(());
    });

    done_sync_receiver.blocking_recv().unwrap();

    // Stop repo.
    for handle in repo_handles.into_iter() {
        handle.stop().unwrap();
    }
}
