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
use tokio::sync::{oneshot, watch};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info};

const SRT_NEW: &str = "SRT:NEW";
const SRT_UP: &str = "SRT:UP";
const SRT_FWD: &str = "SRT:FWD";
const SRT_BYE: &str = "SRT:BYE";

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

    let port = addr.port() + 1;

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

                            let remote_ip = request.remote().ip();

                            let fwd_to_dns = stream_key.is_origin() && !remote_ip.is_loopback();
                            if fwd_to_dns {
                                info!("srt connection from {} is new stream key; forwarding to dns regions", request.remote().ip())
                            } else {
                                info!("srt connection from {} has stream id; not forwarding to dns regions", request.remote().ip())
                            }

                            let fwd_to_lan = remote_ip.is_loopback() || remote_ip.is_global();
                            if fwd_to_lan {
                                info!("srt connection from {} appears external; forwarding to local vlan", request.remote().ip());

                                if let Some(lan) = &lan {
                                    for node in lan.all() {
                                        info!("forwarding to {}", node.ip());
                                    }
                                }
                            } else {
                                info!("srt connection from {} appears local; doing nothing", request.remote().ip())
                            }

                            let stream_id = stream_key.id().to_string();

                            let mut forward_lan_sockets: Vec<SrtSocket> = Vec::new();
                            let mut forward_dns_sockets: Vec<SrtSocket> = Vec::new();

                            let playlists = playlists.clone();
                            let lan_nodes = lan.clone();
                            let dns_nodes = dns.clone();

                            tokio::spawn(async move {
                                if let Ok(mut srt_socket) = request.accept(None).await {
                                    if fwd_to_lan {
                                        if let Some(nodes) = lan_nodes {
                                            for node in nodes.all() {
                                                match SrtSocket::builder().call(node.ip().to_string() + &format!(":{}", port), Some(&stream_id)).await {
                                                    Ok(socket) => {
                                                        forward_lan_sockets.push(socket);
                                                        info!("Added new LAN node: {}", node.ip());
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to connect to new LAN node {}: {:?}", node.ip(), e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let mut added_tags = std::collections::HashSet::new();

                                    if fwd_to_dns {
                                        if let Some(nodes) = dns_nodes {
                                            for node in nodes.all() {
                                                if let Some(tag) = node.tag() {
                                                    if added_tags.contains(tag) {
                                                        continue;
                                                    }
                                                }

                                                match SrtSocket::builder()
                                                    .call(node.ip().to_string() + &format!(":{}", port), Some(&stream_id))
                                                    .await
                                                {
                                                    Ok(socket) => {
                                                        forward_dns_sockets.push(socket);
                                                        info!("Added new DNS node: {}", node.ip());

                                                        if let Some(tag) = node.tag() {
                                                            added_tags.insert(tag.clone());
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to connect to new DNS node {}: {:?}", node.ip(), e);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let (mut fin_rx, tx_demux) = new_demuxer(stream_key.id(), playlists.clone(), min_part_ms);
                                    let mut i: u32 = 0;

                                    loop {
                                        select! {
                                            result = timeout(Duration::from_secs(5), srt_socket.next()) => {
                                                match result {
                                                    Ok(Some(Ok(data))) => {
                                                        let bytes = Bytes::from(data.1);
                                                        i = i.wrapping_add(1);
                                                        if i%400 == 0 {
                                                            info!("{} key={} id={} addr={}", SRT_UP, stream_key.key(), stream_key.id(), srt_socket.settings().remote);
                                                        }
                                                        // Forward to LAN sockets
                                                        {
                                                            for (index, forward_socket) in forward_lan_sockets.iter_mut().enumerate() {
                                                                if let Err(e) = forward_socket.send((Instant::now(), bytes.clone())).await {
                                                                    error!("Error forwarding to LAN socket {}: {:?}", index, e);
                                                                }
                                                                if i%400 == 0 {
                                                                    info!("{} key={} id={} addr={}", SRT_UP, stream_key.key(), stream_key.id(), forward_socket.settings().remote);
                                                                }
                                                            }
                                                        }

                                                        // Forward to DNS sockets if available
                                                        {
                                                            for (index, forward_socket) in forward_dns_sockets.iter_mut().enumerate() {
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
                                                    Ok(Some(Err(e))) => {
                                                        info!("Error receiving packet: {:?}", e);
                                                        break;
                                                    }
                                                    Ok(None) => {
                                                        info!("SRT connection closed");
                                                        break;
                                                    }
                                                    Err(_) => {
                                                        info!("Timeout reached, no packets received in the last 5 seconds, breaking the loop");
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = fin_rx.changed() => {
                                                info!("{} id={} rejected - playlist slots full", SRT_BYE, stream_id);
                                                break;
                                            }
                                            else => {
                                                // Exit the loop when there are no more packets
                                                break;
                                            }
                                        }
                                    }

                                    info!("Client disconnected stream_key={} stream_id={}", stream_key.key(), stream_key.id());
                                    playlists.fin(stream_key.id());

                                    info!("srt stream {} ended: shutdown complete", stream_key.id());
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
