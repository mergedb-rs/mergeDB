use anyhow::Result;
use dashmap::DashMap;
use kv_node::{config::Config, network::ReplicationServer};
use std::{env, net::SocketAddr, path::Path, sync::Arc, time::SystemTime};
use std::io::Write;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let config = if args.contains(&"--interactive".to_string()) || args.contains(&"--i".to_string())
    {
        //for testing, allowing node_addr, peers, id to be entered by the node operator
        println!("@@Interative Mode@@");   
        get_interactive_cofig()?
    } else if args.len() > 1 {
        let path = Path::new(&args[1]);
        match Config::load_config(path.to_path_buf()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error: {e}");
                return Ok(());
            }
        }
    } else {
        eprintln!("usage: ");
        eprintln!("   file mode: ./kv-node <path_to_config>.toml");
        eprintln!("   interactive mode: ./kv-node --interactive");
        return Ok(());
    };

    let map = Arc::new(DashMap::new());
    let peers = Arc::new(DashMap::new());
    for peer_addr in &config.peers {
        peers.insert(peer_addr.clone(), SystemTime::UNIX_EPOCH);
    }

    let server = Arc::new(ReplicationServer {
        store: map.clone(),
        node_id: config.node_id.clone(),
        peers: peers,
    });

    println!("starting server on {}..", config.listen_address);

    let server_clone = server.clone();
    //listener runs in another thread
    tokio::spawn(async move {
        if let Err(e) = server_clone.start_listener(config).await {
            eprintln!("server listener failed: {}", e);
        }
    });

    //gossip loop runs here
    server.create_and_gossip_batch().await?;
    Ok(())
}

fn get_interactive_cofig() -> Result<Config> {
    let mut node_id = String::new();
    print!("enter the node id for this node: ");
    std::io::stdout().flush()?;
    std::io::stdin()
        .read_line(&mut node_id)
        .expect("failed to read line, restart node again");
    let node_id = node_id.trim().to_string();

    let mut node_addr = String::new();
    print!("enter node's address (egs: 127.0.0.1:8000): ");
    std::io::stdout().flush()?;
    std::io::stdin().read_line(&mut node_addr)?;
    let node_addr = node_addr.trim().to_string();

    if let Err(e) = node_addr.parse::<SocketAddr>() {
        return Err(e.into()); //invalid IPv4 addr format
    }

    let mut num_peers = String::new();
    print!("enter number of peers: ");
    std::io::stdout().flush()?;
    std::io::stdin()
        .read_line(&mut num_peers)
        .expect("failed to read line, restart node again");
    let num_peers = num_peers.trim().parse::<i32>()?;

    let mut peers_config = Vec::new(); //for the Config struct

    for i in 0..num_peers {
        let mut peer_addr = String::new();
        print!("enter the peer #{} addr (egs: 127.0.0.1:8000): ", i + 1);
        std::io::stdout().flush()?;
        std::io::stdin()
            .read_line(&mut peer_addr)
            .expect("failed to read line, restart node again");
        let peer_addr = peer_addr.trim().to_string();

        if let Err(e) = peer_addr.parse::<SocketAddr>() {
            return Err(e.into());
        }
        peers_config.push(peer_addr);
    }

    Ok(Config {
        node_id: node_id,
        listen_address: node_addr,
        peers: peers_config,
    })
}
