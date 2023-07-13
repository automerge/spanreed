extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{Repo, RepoMessage};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use test_utils::network_utils::Network;
use test_utils::storage_utils::SimpleStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_sinks_closed_on_shutdown() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Add network adapters
    let mut peers = HashMap::new();
    let (sender, mut network_receiver) = channel(1);
    let network_1 = Network::new(sender.clone());
    let network_2 = Network::new(sender);
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
        Box::new(network_1.clone()),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
        Box::new(network_2.clone()),
    );
    peers.insert(repo_handle_2.get_repo_id().clone(), network_1);
    peers.insert(repo_handle_1.get_repo_id().clone(), network_2);

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        assert_eq!(
            doc_handle_future.await.unwrap().document_id(),
            document_handle_1.document_id()
        );
        done_sync_sender.send(()).await.unwrap();
    });

    let peers_clone = peers.clone();
    rt.spawn(async move {
        // A router of network messages.
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
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
                           let peer = peers.get_mut(&from_repo_id).unwrap();
                          peer.receive_incoming(RepoMessage::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                               });
                       }
                       _ => todo!(),
                   }
               },
            }
        }
    });

    done_sync_receiver.blocking_recv().unwrap();

    let stopping = std::thread::spawn(|| {
        // Stop the repos.
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    });

    let stop_started = Instant::now();
    loop {
        if stopping.is_finished() {
            break;
        }
        if stop_started.elapsed().as_secs() > 5 {
            panic!("Repo stop timed out.");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    stopping.join().unwrap();

    // Assert all sinks were closed on shutdown.
    assert!(!peers_clone.is_empty());
    for (_, network) in peers_clone.into_iter() {
        assert!(network.closed());
    }
}

#[test]
fn test_sinks_closed_on_replacement() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Add network adapters
    let mut peers = HashMap::new();
    let (sender, mut network_receiver) = channel(1);
    let network_1 = Network::new(sender.clone());
    let network_2 = Network::new(sender);
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
        Box::new(network_1.clone()),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
        Box::new(network_2.clone()),
    );
    peers.insert(repo_handle_2.get_repo_id().clone(), network_1);
    peers.insert(repo_handle_1.get_repo_id().clone(), network_2);

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let doc_id = document_handle_1.document_id();
    rt.spawn(async move {
        assert_eq!(doc_handle_future.await.unwrap().document_id(), doc_id,);
        done_sync_sender.send(()).await.unwrap();
    });

    let mut peers_clone = peers.clone();
    rt.spawn(async move {
        // A router of network messages.
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
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
                           let peer = peers.get_mut(&from_repo_id).unwrap();
                          peer.receive_incoming(RepoMessage::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                               });
                       }
                       _ => todo!(),
                   }
               },
            }
        }
    });

    done_sync_receiver.blocking_recv().unwrap();

    let old_peers: HashMap<_, _> = peers_clone.drain().collect();

    // Replace the peers.
    let (sender, mut network_receiver) = channel(1);
    let network_1 = Network::new(sender.clone());
    let network_2 = Network::new(sender);
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
        Box::new(network_1.clone()),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
        Box::new(network_2.clone()),
    );
    peers_clone.insert(repo_handle_2.get_repo_id().clone(), network_1);
    peers_clone.insert(repo_handle_1.get_repo_id().clone(), network_2);

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());
    let peers_clone_2 = peers_clone.clone();
    rt.spawn(async move {
        // A router of network messages.
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
                       let peer = peers_clone.get_mut(&to_repo_id).unwrap();
                       peer.take_outgoing()
                   };
                   match incoming {
                       RepoMessage::Sync {
                           from_repo_id,
                           to_repo_id,
                           document_id,
                           message,
                       } => {
                           let peer = peers_clone.get_mut(&from_repo_id).unwrap();
                          peer.receive_incoming(RepoMessage::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                               });
                       }
                       _ => todo!(),
                   }
               },
            }
        }
    });

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        assert_eq!(
            doc_handle_future.await.unwrap().document_id(),
            document_handle_1.document_id()
        );
        done_sync_sender.send(()).await.unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();

    // Assert all sinks were closed when replaced.
    assert!(!old_peers.is_empty());
    for (_, network) in old_peers.into_iter() {
        assert!(network.closed());
    }

    // Assert all sinks were closed on shutdown.
    assert!(!peers_clone_2.is_empty());
    for (_, network) in peers_clone_2.into_iter() {
        assert!(network.closed());
    }
}

#[test]
fn test_streams_chained_on_replacement() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Add network adapters
    let mut peers = HashMap::new();
    let (sender, mut network_receiver) = channel(1);
    let network_1 = Network::new(sender.clone());
    let network_2 = Network::new(sender);
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
        Box::new(network_1.clone()),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
        Box::new(network_2.clone()),
    );
    peers.insert(repo_handle_2.get_repo_id().clone(), network_1.clone());
    peers.insert(repo_handle_1.get_repo_id().clone(), network_2.clone());

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Channel used to pause the network router.
    let (pause_sender, mut pause_receiver) = channel(1);

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let doc_id = document_handle_1.document_id();
    rt.spawn(async move {
        assert_eq!(doc_handle_future.await.unwrap().document_id(), doc_id,);
        done_sync_sender.send(()).await.unwrap();
    });

    let mut peers_clone = peers.clone();
    rt.spawn(async move {
        // A router of network messages.

        // Don't route until told to.
        pause_receiver.recv().await.unwrap();
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
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

                           let peer = peers.get_mut(&from_repo_id).unwrap();
                          peer.receive_incoming(RepoMessage::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                               });
                       }
                       _ => todo!(),
                   }
               },
            }
        }
    });

    let old_peers: HashMap<_, _> = peers_clone.drain().collect();

    // Replace the peers.
    // TODO: replace the peers with different networks to check chaining.
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
        Box::new(network_1),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
        Box::new(network_2),
    );

    // Start routing messages
    pause_sender.blocking_send(()).unwrap();
    done_sync_receiver.blocking_recv().unwrap();

    // Assert all sinks were closed when replaced.
    assert!(!old_peers.is_empty());
    for (_, network) in old_peers.into_iter() {
        assert!(network.closed());
    }

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();
}
