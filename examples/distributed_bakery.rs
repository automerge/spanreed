use automerge_repo::{ConnDirection, DocHandle, DocumentId, Repo, Storage};
use autosurgeon::{hydrate, reconcile, Hydrate, Reconcile};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::time::{sleep, Duration};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    http_run_ip: String,
    #[arg(long)]
    get_doc_ip: Option<String>,
    #[arg(long)]
    tcp_run_ip: Option<String>,
    #[arg(long)]
    other_ip: Option<String>,
    #[arg(long)]
    customer_id: String,
}

struct AppState {
    doc_handle: DocHandle,
    customer_id: String,
}

async fn get_doc_id(State(state): State<Arc<AppState>>) -> Json<DocumentId> {
    Json(state.doc_handle.document_id())
}

async fn increment(State(state): State<Arc<AppState>>) -> Json<u32> {
    // Enter the critical section.
    run_bakery_algorithm(&state.doc_handle, &state.customer_id).await;
    println!("Entered critical section.");

    // Increment the output
    let output = increment_output(&state.doc_handle, &state.customer_id).await;
    println!("Incremented output to {:?}.", output);

    // Exit the critical section.
    start_outside_the_bakery(&state.doc_handle, &state.customer_id).await;
    println!("Exited critical section.");

    Json(output)
}

async fn increment_output(doc_handle: &DocHandle, customer_id: &str) -> u32 {
    let latest = doc_handle.with_doc_mut(|doc| {
        let mut bakery: Bakery = hydrate(doc).unwrap();
        bakery.output += 1;
        bakery
            .output_seen
            .insert(customer_id.to_string(), bakery.output);
        let mut tx = doc.transaction();
        reconcile(&mut tx, &bakery).unwrap();
        tx.commit();
        bakery.output
    });
    // Wait for all peers to have acknowlegded the new output.
    loop {
        if doc_handle.changed().await.is_err() {
            // Shutdown.
            break;
        }
        let acked_by_all = doc_handle.with_doc(|doc| {
            let bakery: Bakery = hydrate(doc).unwrap();
            bakery.output_seen.values().fold(
                true,
                |acc, output| {
                    if !acc {
                        acc
                    } else {
                        output == &latest
                    }
                },
            )
        });
        if acked_by_all {
            break;
        }
    }
    latest
}

async fn run_bakery_algorithm(doc_handle: &DocHandle, customer_id: &String) {
    let our_number = doc_handle.with_doc_mut(|doc| {
        // Pick a number that is higher than all others.
        let mut bakery: Bakery = hydrate(doc).unwrap();
        let customers_with_number = bakery
            .customers
            .clone()
            .iter()
            .map(|(id, c)| (id.clone(), c.number))
            .collect();
        let highest_number = bakery.customers.values().map(|c| c.number).max().unwrap();
        let our_number = highest_number + 1;
        let our_info = bakery.customers.get_mut(customer_id).unwrap();
        our_info.views_of_others = customers_with_number;
        our_info.number = our_number;
        let mut tx = doc.transaction();
        reconcile(&mut tx, &bakery).unwrap();
        tx.commit();
        our_number
    });

    loop {
        if doc_handle.changed().await.is_err() {
            // Shutdown.
            break;
        }
        let entered_cs = doc_handle.with_doc(|doc| {
            let bakery: Bakery = hydrate(doc).unwrap();

            // Wait for all peers to have acknowlegded our number.
            let acked_by_all = bakery
                .customers
                .iter()
                .filter(|(id, _)| id != &customer_id)
                .fold(true, |acc, (_, c)| {
                    if !acc {
                        acc
                    } else {
                        let view_of_our_number = c.views_of_others.get(customer_id).unwrap();
                        view_of_our_number == &our_number
                    }
                });

            if !acked_by_all {
                return false;
            }

            // Lowest non-negative number.
            let has_lower = bakery
                .customers
                .iter()
                .filter_map(|(id, c)| {
                    if c.number == 0 || id == customer_id {
                        None
                    } else {
                        Some((id, c.number))
                    }
                })
                .min_by_key(|(_, num)| *num);

            // Everyone else is at zero.
            if has_lower.is_none() {
                return true;
            }

            let (id, lowest_number) = has_lower.unwrap();

            if lowest_number == our_number {
                // Break tie by customer id.
                return customer_id < id;
            }

            lowest_number > our_number
        });
        if entered_cs {
            return;
        }
    }
}

async fn acknowlegde_changes(doc_handle: DocHandle, customer_id: String) {
    let (mut our_view, mut output_seen) = doc_handle.with_doc(|doc| {
        let bakery: Bakery = hydrate(doc).unwrap();
        let our_info = bakery.customers.get(&customer_id).unwrap();
        let output_seen = bakery.output_seen.get(&customer_id).unwrap();
        (our_info.views_of_others.clone(), *output_seen)
    });
    loop {
        if doc_handle.changed().await.is_err() {
            // Shutdown.
            break;
        }
        let (customers_with_number, new_output): (HashMap<String, u32>, u32) =
            doc_handle.with_doc(|doc| {
                let bakery: Bakery = hydrate(doc).unwrap();
                let numbers = bakery
                    .customers
                    .iter()
                    .map(|(id, c)| (id.clone(), c.number))
                    .collect();
                (numbers, bakery.output)
            });

        // Only change the doc if something needs acknowledgement.
        if customers_with_number == our_view && output_seen == new_output {
            continue;
        }

        (our_view, output_seen) = doc_handle.with_doc_mut(|doc| {
            let mut bakery: Bakery = hydrate(doc).unwrap();
            let customers_with_number: HashMap<String, u32> = bakery
                .customers
                .clone()
                .iter()
                .map(|(id, c)| (id.clone(), c.number))
                .collect();
            let our_info = bakery.customers.get_mut(&customer_id).unwrap();
            // Ack changes made by others.
            our_info.views_of_others = customers_with_number.clone();

            // Ack any new output.
            bakery
                .output_seen
                .insert(customer_id.clone(), bakery.output);

            let mut tx = doc.transaction();
            reconcile(&mut tx, &bakery).unwrap();
            tx.commit();
            (customers_with_number, bakery.output)
        });
    }
}

async fn start_outside_the_bakery(doc_handle: &DocHandle, customer_id: &String) {
    doc_handle.with_doc_mut(|doc| {
        let mut bakery: Bakery = hydrate(doc).unwrap();
        let our_info = bakery.customers.get_mut(customer_id).unwrap();
        our_info.number = 0;
        let mut tx = doc.transaction();
        reconcile(&mut tx, &bakery).unwrap();
        tx.commit();
    });

    // Wait for acks from peers.
    loop {
        if doc_handle.changed().await.is_err() {
            // Shutdown.
            break;
        }
        let synced = doc_handle.with_doc(|doc| {
            let bakery: Bakery = hydrate(doc).unwrap();
            bakery.customers.iter().fold(true, |acc, (_, c)| {
                if !acc {
                    acc
                } else {
                    let view_of_our_number = c.views_of_others.get(customer_id).unwrap();
                    view_of_our_number == &0
                }
            })
        });
        if synced {
            break;
        }
    }
}

async fn request_increment(doc_handle: DocHandle, customer_id: String, customers: Vec<String>) {
    let client = reqwest::Client::new();
    let mut last = 0;
    let other_customers: Vec<String> = customers
        .into_iter()
        .filter(|id| id != &customer_id)
        .collect();
    loop {
        if doc_handle.changed().await.is_err() {
            // Shutdown.
            break;
        }
        for id in other_customers.iter() {
            sleep(Duration::from_millis(500)).await;
            let url = format!("http://0.0.0.0:300{}/increment", id);
            if let Ok(new) = client.get(url).send().await {
                let new = new.json().await.unwrap();
                println!("Got new increment: {:?}, versus old one: {:?}", new, last);
                assert!(new > last);
                last = new;
            }
        }
    }
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
struct Customer {
    pub number: u32,
    pub views_of_others: HashMap<String, u32>,
}

#[derive(Default, Debug, Clone, Reconcile, Hydrate, PartialEq)]
struct Bakery {
    pub customers: HashMap<String, Customer>,
    pub output: u32,
    pub output_seen: HashMap<String, u32>,
}

struct NoStorage;

impl Storage for NoStorage {}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.tcp_run_ip;
    let http_ip = args.http_run_ip;
    let other_ip = args.other_ip;
    let get_doc_ip = args.get_doc_ip;

    // Hardcoded customer Ids.
    let customers: Vec<String> = vec!["1", "2", "3"]
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    // Create a repo.
    let repo = Repo::new(None, Box::new(NoStorage));
    let repo_handle = repo.run();

    let handle = Handle::current();
    let doc_handle = if let Some(run_ip) = run_ip {
        // Start a server.
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

        // The initial bakery.
        let mut bakery: Bakery = Bakery {
            output: 0,
            ..Default::default()
        };
        bakery.output = 0;
        for customer_id in customers.clone() {
            let customer = Customer {
                // Start with anything but 0,
                // so that peers block on acks
                // until all others are up and running.
                number: u32::MAX,
                views_of_others: customers
                    .clone()
                    .into_iter()
                    .map(|id| (id, u32::MAX))
                    .collect(),
            };
            bakery.customers.insert(customer_id.to_string(), customer);
            bakery.output_seen.insert(customer_id.to_string(), 0);
        }

        // Create the initial document.
        let doc_handle = repo_handle.new_document();
        doc_handle.with_doc_mut(|doc| {
            let mut tx = doc.transaction();
            reconcile(&mut tx, &bakery).unwrap();
            tx.commit();
        });
        doc_handle
    } else {
        // Connect to a remote.
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

        // Get the shared document id.
        let client = reqwest::Client::new();
        let url = format!("http://{}/get_doc_id", get_doc_ip.unwrap());
        let doc_id = client.get(url).send().await.unwrap().json().await.unwrap();

        // Get the document.
        repo_handle.request_document(doc_id).await.unwrap()
    };

    let app_state = Arc::new(AppState {
        doc_handle: doc_handle.clone(),
        customer_id: args.customer_id.clone(),
    });

    // Do this in a task, so that the server immediatly starts running.
    let customer_id = args.customer_id.clone();
    let doc_handle_clone = doc_handle.clone();
    handle.spawn(async move {
        // Start the algorithm "outside the bakery".
        // The acks makes this wait for all others to be up and running.
        start_outside_the_bakery(&doc_handle_clone, &customer_id).await;

        // Continuously requests a new increment.
        request_increment(doc_handle_clone, customer_id, customers).await;
    });

    // A task that continuously acknowledges changes made by others.
    handle.spawn(async move {
        acknowlegde_changes(doc_handle, args.customer_id.clone()).await;
    });

    let app = Router::new()
        .route("/get_doc_id", get(get_doc_id))
        .route("/increment", get(increment))
        .with_state(app_state);
    let serve = axum::Server::bind(&http_ip.parse().unwrap()).serve(app.into_make_service());
    tokio::select! {
        _ = serve.fuse() => {},
        _ = tokio::signal::ctrl_c().fuse() => {
            Handle::current()
                .spawn_blocking(|| {
                    repo_handle.stop().unwrap();
                })
                .await
                .unwrap();
        }
    }
}
