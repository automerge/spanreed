#![cfg(feature = "tokio")]
use std::net::SocketAddr;

use automerge as am;
use automerge::{transaction::Transactable, ReadDoc};
use futures::Future;
use futures::{
    select,
    stream::{FuturesUnordered, StreamExt},
    FutureExt, Stream,
};
use spanreed::{ConnDirection, DocumentId, PeerSpec, Running};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // Start a server socket
    let server = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    // Run a spanreed server on the socket
    let server_task = run_server(server);

    let server_addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    // Run two client processes in succession
    let clients_task = async move {
        // On one client create a document and wait for it to be published to the server
        let doc = run_client(server_addr, |repo| async move {
            // Wait for the server to finish handshake
            repo.peers_connected(PeerSpec::Any).await;

            // create a new document
            let mut doc = am::Automerge::new();
            doc.transact::<_, _, am::AutomergeError>(|d| {
                d.put(am::ROOT, "bird", "goldfinsh");
                Ok(())
            })
            .unwrap();

            // Add t he handle to the repo
            let handle = repo.create(doc).await.unwrap();
            // Announce the new doc and wait for sync with any peer to complete
            // (note that the announcement may already have happened in `create`, this is 
            // just a way of explicitly forcing it to happen and waiting for it to complete)
            repo.announce(handle.id(), PeerSpec::Any).await.unwrap();

            handle.id()
        })
        .await;

        // Now on the second client get the document and check it's what we expected
        run_client(server_addr, |repo| async move {
            // request the handle
            let handle = repo.request(doc, PeerSpec::Any).await.unwrap().unwrap();

            assert_eq!(
                handle
                    .read()
                    .await
                    .get(am::ROOT, "bird")
                    .unwrap()
                    .unwrap()
                    .0,
                "goldfinch".into()
            );
        })
        .await;
    };

    select! {
        _ = server_task.fuse() => {
            panic!("server exited");
        },
        _ = clients_task.fuse() => {
            println!("clients exited");
        }
    };
}

async fn run_server(socket: TcpListener) {
    let Running {
        repo,
        background_driver,
    } = spanreed::Repo::run_tokio(spanreed::config::builder().build()).await;
    let mut connections = FuturesUnordered::new();

    let mut background_driver = background_driver.fuse();
    loop {
        futures::select! {
            incoming = socket.accept().fuse() => {
                let Ok((stream, addr)) = incoming  else {
                    println!("incoming connection failed");
                    break;
                };
                let conn_driver = repo.clone().connect_tokio_io(addr, stream, ConnDirection::Incoming);
                connections.push(conn_driver);
            },
            connection_finished = connections.next().fuse() => {
                println!("conection completed");
            },
            background = background_driver => {
                println!("background driver completed");
                break;
            }

        }
    }
}

async fn run_client<
    O,
    Fut: Future<Output = O>,
    F: FnOnce(spanreed::Repo<spanreed::storage::InMemory, spanreed::ShareAll>) -> Fut,
>(
    server: SocketAddr,
    f: F,
) -> O {
    // Create a repo
    let Running {
        repo,
        background_driver,
    } = spanreed::Repo::run_tokio(spanreed::config::builder().build()).await;

    let outgoing = TcpStream::connect(server).await.unwrap();

    // spawn the background tasks
    let conn = tokio::spawn(background_driver);
    let conn_driver = tokio::spawn(repo.clone().connect_tokio_io(
        server,
        outgoing,
        ConnDirection::Outgoing,
    ));

    f(repo).await
}
