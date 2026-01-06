use anyhow::Result;
use dashmap::DashMap;
use kv_types::{aw_set::AWSet, pn_counter::PNCounter, Merge};
use rand::{rngs::SmallRng, seq::IndexedRandom, SeedableRng};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tonic::{transport::Channel, transport::Server, Request, Response};

use crate::{
    communication::{
        replication_service_client::ReplicationServiceClient,
        replication_service_server::{ReplicationService, ReplicationServiceServer},
        GossipBatchRequest, GossipBatchResponse, GossipChangesRequest, GossipChangesResponse,
        PnCounterMessage, PropagateDataRequest, PropagateDataResponse,
    },
    config::Config,
};

const K: usize = 3;
const BATCH_SIZE: usize = 1000;

#[derive(Debug)]
pub enum CRDTValue {
    Counter(PNCounter), //others later
    ASet(AWSet<String>),
}

#[derive(Debug)]
pub struct StoredValue {
    pub data: CRDTValue,
    pub last_updated: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ReplicationServer {
    pub store: Arc<DashMap<String, StoredValue>>,
    pub config: Arc<Config>,
    pub peers: Arc<DashMap<String, SystemTime>>,
}

// convert domain -> proto for sending
impl From<PNCounter> for PnCounterMessage {
    fn from(domain: PNCounter) -> Self {
        Self {
            p: domain.p,
            n: domain.n,
        }
    }
}

// convert proto -> domain for receiving
impl From<PnCounterMessage> for PNCounter {
    fn from(wire: PnCounterMessage) -> Self {
        Self {
            p: wire.p,
            n: wire.n,
        }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServer {
    async fn propagate_data(
        &self,
        request: tonic::Request<PropagateDataRequest>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        let req_inner = request.into_inner();

        let value_type = req_inner.valuetype;
        let key = req_inner.key;
        let raw_value_bytes = req_inner.value;
        let map = Arc::clone(&self.store);

        if value_type == "CSET" {
            //value shld be a u64
            let bytes: [u8; 8] = raw_value_bytes.try_into().map_err(|_| {
                tonic::Status::invalid_argument("invalid byte length for u64, expected 8 bytes")
            })?;

            let numeric_val: u64 = u64::from_be_bytes(bytes);

            println!("received valid CSET: {}", numeric_val);

            let counter = PNCounter {
                p: HashMap::from([(self.config.node_id.clone(), numeric_val)]),
                n: HashMap::from([(self.config.node_id.clone(), 0)]),
            };

            let new_pn: CRDTValue = CRDTValue::Counter(counter.clone());
            map.insert(
                key.clone(),
                StoredValue {
                    data: new_pn,
                    last_updated: SystemTime::now(),
                },
            );
            println!("Counter set!");

            match self.push(key, CRDTValue::Counter(counter)).await {
                Ok(_) => {}
                Err(_) => {}
            };

            //need to send an ack that the op has been done
            Ok(Response::new(PropagateDataResponse {
                success: true,
                response: Vec::new(),
            })) //send empty bytes for response
        } else if value_type == "CINC" {
            let bytes: [u8; 8] = raw_value_bytes.try_into().map_err(|_| {
                tonic::Status::invalid_argument("invalid byte length for u64, expected 8 bytes")
            })?;

            let numeric_val: u64 = u64::from_be_bytes(bytes);

            println!("received valid CINC, to increase by: {}", numeric_val);

            let mut val = match map.get_mut(&key) {
                Some(val) => val,
                None => {
                    return Err(tonic::Status::not_found("The requested key was not found!"));
                }
            };
            match &mut val.data {
                CRDTValue::Counter(local_counter) => {
                    local_counter.increment(self.config.node_id.clone(), numeric_val);
                    println!("Counter incremented by: {}", numeric_val);

                    match self
                        .push(key, CRDTValue::Counter(local_counter.clone()))
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };

                    return Ok(Response::new(PropagateDataResponse {
                        success: true,
                        response: Vec::new(),
                    }));
                }
                _ => println!("type mismatch: key exisits, but value is not of type PNCounter"),
            }
            Ok(Response::new(PropagateDataResponse {
                success: false,
                response: Vec::new(),
            }))
        } else if value_type == "CDEC" {
            let bytes: [u8; 8] = raw_value_bytes.try_into().map_err(|_| {
                tonic::Status::invalid_argument("invalid byte length for u64, expected 8 bytes")
            })?;

            let numeric_val: u64 = u64::from_be_bytes(bytes);

            println!("received valid CDEC, to decrease by: {}", numeric_val);

            let mut val = match map.get_mut(&key) {
                Some(val) => val,
                None => {
                    return Err(tonic::Status::not_found("The requested key was not found!"));
                }
            };
            match &mut val.data {
                CRDTValue::Counter(local_counter) => {
                    local_counter.decrement(self.config.node_id.clone(), numeric_val);
                    println!("Counter decremented by: {}", numeric_val);

                    match self
                        .push(key, CRDTValue::Counter(local_counter.clone()))
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    };

                    return Ok(Response::new(PropagateDataResponse {
                        success: true,
                        response: Vec::new(),
                    }));
                }
                _ => println!("type mismatch: key exisits, but value is not of type PNCounter"),
            }
            Ok(Response::new(PropagateDataResponse {
                success: false,
                response: Vec::new(),
            }))
        } else if value_type == "CGET" {
            //no need to resolve raw_value_bytes here, "CGET key"
            println!("received valid CGET, get value of key: {}", key);

            let val = match map.get_mut(&key) {
                Some(val) => val,
                None => {
                    return Err(tonic::Status::not_found("The requested key was not found!"));
                }
            };
            match &val.data {
                CRDTValue::Counter(local_counter) => {
                    let value = local_counter.value();
                    println!("value is {}", value);
                    return Ok(Response::new(PropagateDataResponse {
                        success: true,
                        response: value.to_be_bytes().to_vec(),
                    }));
                }
                _ => println!("type mismatch: key exisits, but value is not of type PNCounter"),
            }
            Ok(Response::new(PropagateDataResponse {
                success: false,
                response: Vec::new(),
            }))
        } else {
            println!("other types soon!");
            Ok(Response::new(PropagateDataResponse {
                success: false,
                response: Vec::new(),
            }))
        }
    }

    async fn gossip_changes(
        &self,
        changes: tonic::Request<GossipChangesRequest>,
    ) -> Result<tonic::Response<GossipChangesResponse>, tonic::Status> {
        let changes_inner = changes.into_inner();
        let key = changes_inner.key;
        let counter = match changes_inner.counter {
            Some(counter) => counter,
            None => return Ok(Response::new(GossipChangesResponse { success: false })),
        };
        let remote_counter = PNCounter::from(counter); //the actual PNCounter type

        //call merge now with the value corresponding to the same key in this node
        self.store
            .entry(key.clone())
            .and_modify(|current_value| {
                match &mut current_value.data {
                    CRDTValue::Counter(local_counter) => {
                        let old_state = local_counter.clone();

                        local_counter.merge(&mut remote_counter.clone());

                        if *local_counter != old_state {
                            println!("Merged NEW update for {}", key);
                            current_value.last_updated = SystemTime::now();
                        } else {
                            println!("Ignored redundant update for {}", key);
                        }
                    } //other types later
                    _ => println!("type mismatch: key exisits, but value is not of type PNCounter"),
                }

                current_value.last_updated = SystemTime::now()
            })
            .or_insert_with(|| StoredValue {
                data: CRDTValue::Counter(remote_counter.clone()),
                last_updated: SystemTime::now(),
            });

        Ok(Response::new(GossipChangesResponse { success: true }))
    }

    async fn gossip_batch(
        &self,
        batch: tonic::Request<GossipBatchRequest>,
    ) -> Result<tonic::Response<GossipBatchResponse>, tonic::Status> {
        let batch = batch.into_inner().batch;
        for (key, counter) in batch {
            let remote_counter = PNCounter::from(counter);

            self.store
                .entry(key.clone())
                .and_modify(|current_value| {
                    match &mut current_value.data {
                        CRDTValue::Counter(local_counter) => {
                            let old_state = local_counter.clone();

                            local_counter.merge(&mut remote_counter.clone());

                            if *local_counter != old_state {
                                println!("Merged NEW update for {}", key);
                                current_value.last_updated = SystemTime::now();
                            } else {
                                println!("Ignored redundant update for {}", key);
                            }
                        } //other types later
                        _ => println!(
                            "type mismatch: key exisits, but value is not of type PNCounter"
                        ),
                    }
                })
                .or_insert_with(|| StoredValue {
                    data: CRDTValue::Counter(remote_counter),
                    last_updated: SystemTime::now(),
                });
        }

        Ok(Response::new(GossipBatchResponse { success: (true) }))
    }
}

impl ReplicationServer {
    pub async fn start_listener(&self) -> Result<()> {
        let addr: SocketAddr = self.config.listen_address.as_str().parse()?;
        Server::builder()
            .add_service(ReplicationServiceServer::new(self.clone()))
            .serve(addr)
            .await?;

        Ok(())
    }

    pub async fn push(&self, key: String, value: CRDTValue) -> Result<()> {
        //send updates to k randomly chosen peers
        //first make sure to preconnect to 3 randomly chosen peer nodes
        //lots of things to think of, like what if a node goes down, how will this node reconnect to
        //some other node etc, will tackle these later

        let mut rng = SmallRng::from_os_rng();

        let chosen_peers: Vec<String> = {
            let peers: Vec<String> = self.peers.iter().map(|entry| entry.key().clone()).collect();
            peers.choose_multiple(&mut rng, K).cloned().collect()
        };

        for peer_addr in chosen_peers.iter() {
            match ReplicationServiceClient::connect((*peer_addr).clone()).await {
                Ok(mut peer_client) => match &value {
                    CRDTValue::Counter(inner) => {
                        let state = Request::new(GossipChangesRequest {
                            key: key.clone(),
                            counter: Some(PnCounterMessage::from(inner.clone())),
                        });

                        println!("connected to the peer with id: {}", peer_addr);
                        match peer_client.gossip_changes(state).await {
                            Ok(response) => {
                                println!("Response from peer: {:?}", response.into_inner())
                            }
                            Err(e) => println!("failed to send update to {}: {}", peer_addr, e),
                        }
                    }
                    _ => print!("other types soon!"),
                },
                Err(e) => {
                    println!("failed to connect to {}: {}", peer_addr, e);
                    continue;
                }
            }
        }

        Ok(())
    }

    pub async fn create_and_gossip_batch(&self) -> Result<()> {
        //a connection pool of rpc connections so as to not cause redundant ::connect's again if
        //a node has already been connected to in an earlier iteration

        let mut connection_pool: HashMap<String, ReplicationServiceClient<Channel>> =
            HashMap::new();

        loop {
            let mut chosen_peers: Vec<String> = Vec::new();
            for peers in self.peers.iter() {
                if peers.value().elapsed().unwrap_or(Duration::ZERO) > Duration::from_secs(2) {
                    chosen_peers.push(peers.key().clone());
                }
            }

            for peer_addr in &chosen_peers {
                if !connection_pool.contains_key(peer_addr) {
                    let endpoint = if peer_addr.starts_with("http") {
                        peer_addr.clone()
                    } else {
                        format!("http://{}", peer_addr)
                    };

                    match ReplicationServiceClient::connect(endpoint).await {
                        Ok(client) => {
                            connection_pool.insert(peer_addr.clone(), client);
                        }
                        Err(e) => {
                            println!("failed to connect to {}: {}", peer_addr, e);
                            continue;
                        }
                    }
                }

                //for each key in the current node, transfer each of the node states for merge
                if let Some(peer_client) = connection_pool.get_mut(peer_addr) {
                    let mut batch = HashMap::new();
                    let mut updates_sent = 0;

                    for mut key_val in self.store.iter_mut() {
                        let key = key_val.key().clone();
                        let value = key_val.value_mut();

                        if value.last_updated.elapsed().unwrap_or(Duration::ZERO)
                            < Duration::from_secs(2)
                        {
                            if let CRDTValue::Counter(inner) = &value.data {
                                batch.insert(key.clone(), PnCounterMessage::from(inner.clone()));
                            }

                            if batch.len() >= BATCH_SIZE {
                                let req = Request::new(GossipBatchRequest {
                                    batch: batch.clone(),
                                });
                                if let Err(e) = peer_client.gossip_batch(req).await {
                                    eprintln!("Failed to send batch to {}: {}", peer_addr, e);
                                } else {
                                    updates_sent += batch.len();
                                }
                                batch.clear();
                            }
                        }
                    }

                    if !batch.is_empty() {
                        let req = Request::new(GossipBatchRequest {
                            batch: batch.clone(),
                        });
                        if let Err(e) = peer_client.gossip_batch(req).await {
                            eprintln!("Failed to send final batch to {}: {}", peer_addr, e);
                        } else {
                            updates_sent += batch.len();
                        }
                    }

                    self.peers.insert(peer_addr.clone(), SystemTime::now());

                    if updates_sent > 0 {
                        println!("Synced {} items with {}", updates_sent, peer_addr);
                    }
                }
            }
            //wait for 2s before the next gossip round
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
        Ok(())
    }
}
