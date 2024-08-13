use crate::demuxer::{new_demuxer, AccessUnit};
use crate::playlist::Playlists;
use bytes::Bytes;
use futures::{future, stream, SinkExt, StreamExt};
use srt_tokio::{options::*, SrtListener, SrtSocket};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot, watch};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

use tracing::{debug, error, info};

pub async fn start_srt_listener(
    port: u16,
    forward_addresses: Vec<SocketAddr>,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
        mpsc::Receiver<AccessUnit>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();
    let (tx, rx) = mpsc::channel::<AccessUnit>(16);

    let srv = async move {
        match SrtListener::builder().bind(port).await {
            Ok((_, mut incoming)) => {
                info!("SRT Multiplex Server is listening on port: {port}");

                if let Err(e) = up_tx.send(()) {
                    error!("Failed to send startup signal: {:?}", e);
                    return Err(Box::<dyn Error + Send + Sync>::from(
                        "Failed to send startup signal",
                    ));
                }

                let playlists = playlists.clone();
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
                            }

                            let forward_sockets_futures = forward_addresses
                                .iter()
                                .map(|addr| SrtSocket::builder().call(addr.to_string(), None));

                            let forward_sockets_results = future::join_all(forward_sockets_futures).await;
                            let forward_sockets = Arc::new(Mutex::new(
                                forward_sockets_results
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(index, result)| match result {
                                        Ok(socket) => Some(socket),
                                        Err(e) => {
                                            error!(
                                                "Failed to connect to {}: {:?}",
                                                forward_addresses[index], e
                                            );
                                            None
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                            ));

                            let playlists = playlists.clone();
                            match request.accept(None).await {
                                Ok(srt_socket) => {
                                    let tx_clone = tx.clone();
                                    let forward_sockets_clone = Arc::clone(&forward_sockets);
                                    tokio::spawn(async move {
                                        handle_client(stream_id, srt_socket, tx_clone, forward_sockets_clone, playlists, min_part_ms).await;
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

    Ok((up_rx, fin_rx, shutdown_tx, rx))
}

async fn handle_client(
    stream_id: u64,
    mut srt_socket: SrtSocket,
    tx: mpsc::Sender<AccessUnit>,
    forward_sockets: Arc<Mutex<Vec<SrtSocket>>>,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
) {
    let client_desc = format!(
        "(ip_port: {}, sockid: {})",
        srt_socket.settings().remote,
        srt_socket.settings().remote_sockid.0
    );

    info!("\nNew client connected: {client_desc}");

    let tx_demux = new_demuxer(stream_id, playlists.clone(), min_part_ms);

    loop {
        tokio::select! {
            Some(packet) = srt_socket.next() => {
                match packet {
                    Ok(data) => {
                        let bytes = Bytes::from(data.1);

                         // Forward to all specified addresses
                        let mut sockets = forward_sockets.lock().await;
                        for (index, forward_socket) in sockets.iter_mut().enumerate() {
                            if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                error!("Error forwarding to socket {}: {:?}", index, e);
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
        }
    }

    info!("\nClient {client_desc} disconnected");
    playlists.fin(stream_id);

    info!("srt stream {} ended: shutdown complete", stream_id);
}
