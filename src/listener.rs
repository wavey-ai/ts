use crate::demuxer::new_demuxer;
use crate::playlist::Playlists;
use crate::streamkey::{Gatekeeper, Streamkey};
use bytes::Bytes;
use discovery::Nodes;
use futures::{SinkExt, StreamExt};
use srt_tokio::{SrtListener, SrtSocket};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, watch};
use tokio::time::{self, Duration};
use tracing::{debug, error, info};

const SRT_NEW: &str = "SRT:NEW";
const SRT_UP: &str = "SRT:UP";
const SRT_DOWN: &str = "SRT:DOWN";

pub async fn start_srt_listener(
    base64_encoded_pem_key: &str,
    addr: SocketAddr,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
    lan: Option<Arc<Nodes>>,
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

    let gatekeeper = Gatekeeper::new(base64_encoded_pem_key)?;

    let port = addr.port();

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

                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            info!("Received shutdown signal, exiting...");
                            break;
                        }

                        Some(request) = incoming.incoming().next() => {
                            let stream_key: Streamkey;
                            if let Some(id) = &request.stream_id() {
                                stream_key = gatekeeper.streamkey(id)?;
                                info!("{} {} {}", SRT_NEW, stream_key.key(), stream_key.id());
                            } else {
                                return Err(Box::<dyn Error + Send + Sync>::from(
                                    "Failed to retrieve stream key",
                                ));
                            }

                            let fwd_to_dns = stream_key.is_origin();

                            let fwd_to_lan = request.remote().ip().is_global();
                            if fwd_to_lan {
                                info!("srt connection from {} appears external; forwarding to local vlan", request.remote().ip())
                            } else {
                                info!("srt connection from {} appears local; doing nothing", request.remote().ip())
                            }

                            let stream_id_str = stream_key.id().to_string();

                            let forward_lan_sockets = Arc::new(Mutex::new(Vec::new()));
                            let forward_dns_sockets = Arc::new(Mutex::new(Vec::new()));
                            let playlists_clone = playlists.clone();
                            let lan_nodes_clone = lan.clone();
                            let dns_nodes_clone = dns.clone();


                            tokio::spawn(async move {
                                if let Ok(srt_socket) = request.accept(None).await {
                                    if fwd_to_lan {
                                        if let Some(lan_nodes) = lan_nodes_clone {
                                            let mut rx = lan_nodes.rx();
                                            while let Ok(ip) = rx.recv().await {
                                                match SrtSocket::builder().call(ip.to_string() + &format!(":{}", port), Some(&stream_id_str)).await {
                                                    Ok(socket) => {
                                                        forward_lan_sockets.lock().await.push(socket);
                                                        info!("Added new LAN node: {}", ip);
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to connect to new LAN node {}: {:?}", ip, e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if fwd_to_dns {
                                        if let Some(dns_nodes) = dns_nodes_clone {
                                            let mut rx = dns_nodes.rx();
                                            while let Ok(ip) = rx.recv().await {
                                                match SrtSocket::builder().call(ip.to_string() + &format!(":{}", port), Some(&stream_id_str)).await {
                                                    Ok(socket) => {
                                                        forward_dns_sockets.lock().await.push(socket);
                                                        info!("Added new DNS node: {}", ip);
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to connect to new DNS node {}: {:?}", ip, e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    handle_client(stream_key.id(), srt_socket, forward_lan_sockets, forward_dns_sockets, playlists_clone, min_part_ms).await;
                                } else {
                                    error!("Failed to accept SRT connection");
                                }
                            });
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
    forward_dns_sockets: Arc<Mutex<Vec<SrtSocket>>>,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
) {
    let (mut fin_rx, tx_demux) = new_demuxer(stream_id, playlists.clone(), min_part_ms);

    let mut i: u32 = 0;

    dbg!("in client");
    loop {
        select! {
            Some(packet) = srt_socket.next() => {
                match packet {
                    Ok(data) => {
                        dbg!(&data);
                        let bytes = Bytes::from(data.1);
                        let settings = srt_socket.settings();
                        let default = String::from("foo");
                        let key = settings.stream_id.as_ref().unwrap_or(&default);
                        i = i.wrapping_add(1);
                        if i%400 == 0 {
                            info!("{} key={} id={} addr={}", SRT_UP, key, stream_id, settings.remote);
                        }

                        // Forward to LAN sockets
                        {
                            let mut lan_sockets = forward_lan_sockets.lock().await;
                            for (index, forward_socket) in lan_sockets.iter_mut().enumerate() {
                                if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                    error!("Error forwarding to LAN socket {}: {:?}", index, e);
                                }
                            }
                        }

                        // Forward to DNS sockets if available
                        {
                            let mut dns_sockets = forward_dns_sockets.lock().await;
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
