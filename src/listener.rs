use crate::demuxer::new_demuxer;
use crate::playlist::Playlists;
use bytes::Bytes;
use discovery::Nodes;
use futures::{future, stream, SinkExt, StreamExt};
use srt_tokio::{options::*, SrtListener, SrtSocket};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Duration};
use tracing::{debug, error, info};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const SRT_NEW: &str = "SRT:NEW";
const SRT_UP: &str = "SRT:UP";
const SRT_DOWN: &str = "SRT:DOWN";

pub async fn start_srt_listener(
    addr: SocketAddr,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
    lan: Arc<Nodes>,
    dns: Option<Arc<Nodes>>,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();

    let srv = async move {
        match SrtListener::builder().bind(addr).await {
            Ok((_, mut incoming)) => {
                info!("SRT Multiplex Server is listening at: {addr}");

                if let Err(e) = up_tx.send(()) {
                    error!("Failed to send startup signal: {:?}", e);
                    return Err(Box::<dyn Error + Send + Sync>::from(
                        "Failed to send startup signal",
                    ));
                }

                let playlists = playlists.clone();

                let lan_nodes = Arc::clone(&lan);
                let dns_nodes = dns.clone();

                loop {
                    tokio::select! {
                        // Handle shutdown
                        _ = shutdown_rx.changed() => {
                            info!("Received shutdown signal, exiting...");
                            break;
                        }

                        // Handle incoming connections
                        Some(request) = incoming.incoming().next() => {
                            let mut stream_id: u64 = 0;
                            if let Some(id) = &request.stream_id() {
                                stream_id = const_xxh3(&id.as_bytes());
                                info!("{} {} {}", SRT_NEW, id, stream_id );
                            }

                            // Create forward sockets for LAN nodes
                            let forward_lan_addresses = lan_nodes.all().into_iter().map(|n| n.ip() ).collect::<Vec<_>>();
                            let forward_lan_sockets_futures = forward_lan_addresses
                                .iter()
                                .map(|addr| SrtSocket::builder().call(addr.to_string() + ":8001", None));

                            let forward_lan_sockets_results = future::join_all(forward_lan_sockets_futures).await;
                            let forward_lan_sockets = Arc::new(Mutex::new(
                                forward_lan_sockets_results
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(index, result)| match result {
                                        Ok(socket) => Some(socket),
                                        Err(e) => {
                                            error!(
                                                "Failed to connect to LAN node {}: {:?}",
                                                forward_lan_addresses[index], e
                                            );
                                            None
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                            ));

                            // Create forward sockets for DNS nodes if available
                            let forward_dns_sockets = if let Some(dns_nodes) = &dns_nodes {
                                let forward_dns_addresses = dns_nodes.all().into_iter().map(|n| n.ip() ).collect::<Vec<_>>();
                                let forward_dns_sockets_futures = forward_dns_addresses
                                    .iter()
                                    .map(|addr| SrtSocket::builder().call(addr.to_string() + ":8000", None));

                                let forward_dns_sockets_results = future::join_all(forward_dns_sockets_futures).await;
                                Some(Arc::new(Mutex::new(
                                    forward_dns_sockets_results
                                        .into_iter()
                                        .enumerate()
                                        .filter_map(|(index, result)| match result {
                                            Ok(socket) => Some(socket),
                                            Err(e) => {
                                                error!(
                                                    "Failed to connect to DNS node {}: {:?}",
                                                    forward_dns_addresses[index], e
                                                );
                                                None
                                            }
                                        })
                                        .collect::<Vec<_>>(),
                                )))
                            } else {
                                None
                            };

                            match request.accept(None).await {
                                Ok(srt_socket) => {
                                    let forward_lan_sockets_clone = Arc::clone(&forward_lan_sockets);
                                    let forward_dns_sockets_clone = forward_dns_sockets.clone();
                                    let playlists_clone = playlists.clone();
                                    tokio::spawn(async move {
                                        handle_client(stream_id, srt_socket, forward_lan_sockets_clone, forward_dns_sockets_clone, playlists_clone, min_part_ms).await;
                                    });
                                }
                                Err(e) => {
                                    error!("Error accepting connection: {:?}", e);
                                }
                            }
                        }

                        // Handle case when stream ends
                        else => break,
                    }
                }

                if let Err(e) = fin_tx.send(()) {
                    error!("Failed to send finish signal: {:?}", e);
                }
            }
            Err(e) => {
                error!("Failed to bind SRT listener: {:?}", e);
                return Err(Box::new(e) as Box<dyn Error + Send + Sync>);
            }
        }

        Ok::<(), Box<dyn Error + Send + Sync>>(())
    };

    tokio::spawn(srv);

    Ok((up_rx, fin_rx, shutdown_tx))
}

async fn handle_client(
    stream_id: u64,
    mut srt_socket: SrtSocket,
    forward_lan_sockets: Arc<Mutex<Vec<SrtSocket>>>,
    forward_dns_sockets: Option<Arc<Mutex<Vec<SrtSocket>>>>,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
) {
    let (mut fin_rx, tx_demux) = new_demuxer(stream_id, playlists.clone(), min_part_ms);

    let mut i: u32 = 0;

    loop {
        select! {
            Some(packet) = srt_socket.next() => {
                match packet {
                    Ok(data) => {
                        let bytes = Bytes::from(data.1);
                        let settings = srt_socket.settings();
                        let default = String::from("foo");
                        let key = settings.stream_id.as_ref().unwrap_or(&default);
                        i = i.wrapping_add(1);
                        if i%400 == 0 {
                            info!("{} key={} id={} addr={}", SRT_UP, key, stream_id, settings.remote);
                        }

                        // Forward to LAN sockets
                        let mut lan_sockets = forward_lan_sockets.lock().await;
                        for (index, forward_socket) in lan_sockets.iter_mut().enumerate() {
                            if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                error!("Error forwarding to LAN socket {}: {:?}", index, e);
                            }
                        }

                        // Forward to DNS sockets if available
                        if let Some(dns_sockets) = &forward_dns_sockets {
                            let mut dns_sockets = dns_sockets.lock().await;
                            for (index, forward_socket) in dns_sockets.iter_mut().enumerate() {
                                if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                    error!("Error forwarding to DNS socket {}: {:?}", index, e);
                                }
                            }
                        }

                        if let Err(e) = tx_demux.send(bytes).await {
                            error!("Error sending to demuxer: {:?}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        info!("Error receiving packet: {:?}", e);
                        break;
                    }
                }
            }
            _ = fin_rx.changed() => {
                info!("{} id={} rejected - playlist slots full", SRT_DOWN, stream_id);
                break;
            }
            else => {
                // Exit the loop when there are no more packets
                break;
            }
        }
    }

    info!("\nClient disconnected");
    playlists.fin(stream_id);

    info!("srt stream {} ended: shutdown complete", stream_id);
}
