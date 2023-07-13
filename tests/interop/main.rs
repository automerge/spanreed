use std::{panic::catch_unwind, path::PathBuf, process::Child, thread::sleep, time::Duration};

use automerge::{transaction::Transactable, ReadDoc};
use automerge_repo::{ConnDirection, Repo};
use test_utils::storage_utils::InMemoryStorage;
//use test_log::test;

const INTEROP_SERVER_PATH: &str = "interop-test-server";
const PORT: u16 = 8099;

#[test]
fn interop_test() {
    env_logger::init();
    tracing::trace!("we're starting up");
    let mut server_process = start_js_server();
    let result = catch_unwind(|| sync_two_repos(PORT));
    server_process.kill().unwrap();
    match result {
        Ok(()) => {}
        Err(e) => {
            std::panic::resume_unwind(e);
        }
    }
}

fn sync_two_repos(port: u16) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let storage1 = Box::new(InMemoryStorage::default());
        let repo1 = Repo::new(None, storage1);
        let repo1_handle = repo1.run();
        let (conn, _) = tokio_tungstenite::connect_async(format!("ws://localhost:{}", port))
            .await
            .unwrap();
        repo1_handle
            .connect_tungstenite(conn, ConnDirection::Outgoing)
            .await
            .unwrap();

        let doc_handle_repo1 = repo1_handle.new_document();
        doc_handle_repo1
            .with_doc_mut(|doc| {
                doc.transact(|tx| {
                    tx.put(automerge::ROOT, "key", "value")?;
                    Ok::<_, automerge::AutomergeError>(())
                })
            })
            .unwrap();

        let storage2 = Box::new(InMemoryStorage::default());
        let repo2 = Repo::new(None, storage2);
        let repo2_handle = repo2.run();

        let (conn2, _) = tokio_tungstenite::connect_async(format!("ws://localhost:{}", port))
            .await
            .unwrap();

        repo2_handle
            .connect_tungstenite(conn2, ConnDirection::Outgoing)
            .await
            .unwrap();

        //tokio::time::sleep(Duration::from_secs(2)).await;

        println!("Requesting");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let doc_handle_repo2 = repo2_handle
            .request_document(doc_handle_repo1.document_id())
            .await
            .unwrap();
        doc_handle_repo2.with_doc(|doc| {
            assert_eq!(
                doc.get::<_, &str>(automerge::ROOT, "key")
                    .unwrap()
                    .unwrap()
                    .0
                    .into_string()
                    .unwrap()
                    .as_str(),
                "value"
            );
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        repo1_handle.stop().unwrap();
        repo2_handle.stop().unwrap();
    })
}

fn start_js_server() -> Child {
    println!(
        "Building and starting JS interop server in {}",
        interop_server_path().display()
    );
    npm_install();
    npm_build();
    let child = std::process::Command::new("node")
        .args(["server.js", &PORT.to_string()])
        .env("DEBUG", "WebsocketServer,automerge-repo:*")
        .current_dir(interop_server_path())
        .spawn()
        .unwrap();

    // Wait for the server to start up
    loop {
        match reqwest::blocking::get(format!("http://localhost:{}/", PORT)) {
            Ok(r) => {
                if r.status().is_success() {
                    break;
                } else {
                    println!("Server not ready yet, got status code {}", r.status());
                }
            }
            Err(e) => {
                println!("Error connecting to server: {}", e);
            }
        }
        sleep(Duration::from_millis(100));
    }
    child
}

fn npm_build() {
    println!("npm run build");
    let mut cmd = std::process::Command::new("npm");
    cmd.args(&["run", "build"]);
    cmd.current_dir(interop_server_path());
    let status = cmd.status().expect("npm run build failed");
    assert!(status.success());
}

fn npm_install() {
    println!("npm install");
    let mut cmd = std::process::Command::new("npm");
    cmd.arg("install");
    cmd.current_dir(interop_server_path());
    let status = cmd.status().expect("npm install failed");
    assert!(status.success());
}

fn interop_server_path() -> PathBuf {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push(INTEROP_SERVER_PATH);
    d
}
