use anyhow::Result;
use dashmap::DashMap;
use kv_node::{config::Config, network::ReplicationServer};
use std::{path::PathBuf, sync::Arc, time::SystemTime};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load_config(PathBuf::from("config.toml"))?;

    let store = Arc::new(DashMap::new());
    let peers = Arc::new(DashMap::new());

    for peer_addr in &config.peers {
        peers.insert(peer_addr.clone(), SystemTime::UNIX_EPOCH);
    }

    println!(
        "Node '{}' starting on {}",
        config.node_id, config.listen_address
    );

    let server = Arc::new(ReplicationServer {
        store: store,
        config: Arc::new(config),
        peers: peers,
    });

    let server_clone = server.clone();

    tokio::spawn(async move {
        if let Err(e) = server_clone.start_listener().await {
            eprintln!("server listener failed: {e}");
        }
    });

    server.create_and_gossip_batch().await?;

    Ok(())
}
