use anyhow::Result;
use dashmap::DashMap;
use mergedb_types::{
    aw_set::{AWSet, Dot},
    pn_counter::PNCounter,
    Merge,
};
use rand::{rngs::SmallRng, seq::IndexedRandom, SeedableRng};
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tonic::{transport::Channel, transport::Server, Request, Response};

use crate::{
    communication::{
        crdt_data::Data,
        replication_service_client::ReplicationServiceClient,
        replication_service_server::{ReplicationService, ReplicationServiceServer},
        AwSetMessage, CrdtData, GossipBatchRequest, GossipBatchResponse, GossipChangesRequest,
        GossipChangesResponse, PnCounterMessage, PropagateDataRequest, PropagateDataResponse,
        ProtoDot, ProtoDotSet,
    },
    config::Config,
};

const K: usize = 3;
const BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub enum CRDTValue {
    Counter(PNCounter),
    AWSet(AWSet),
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
    pub pool: Arc<DashMap<String, ReplicationServiceClient<Channel>>>,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    SetCounter, //CSET
    GetCounter, //CGET
    IncCounter, //CINC
    DecCounter, //CDEC
    SetAdd,     //SADD
    SetRemove,  //SREM
    GetSet,     //SGET
    Unknown,
}

impl FromStr for Command {
    type Err = ();

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        match input {
            "CSET" => Ok(Command::SetCounter),
            "CGET" => Ok(Command::GetCounter),
            "CINC" => Ok(Command::IncCounter),
            "CDEC" => Ok(Command::DecCounter),
            "SADD" => Ok(Command::SetAdd),
            "SREM" => Ok(Command::SetRemove),
            "SGET" => Ok(Command::GetSet),
            _ => Ok(Command::Unknown),
        }
    }
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

//same for AWSet

impl From<Dot> for ProtoDot {
    fn from(domain: Dot) -> Self {
        Self {
            node_id: domain.node_id,
            counter: domain.counter,
        }
    }
}

impl From<ProtoDot> for Dot {
    fn from(wire: ProtoDot) -> Self {
        Self {
            node_id: wire.node_id,
            counter: wire.counter,
        }
    }
}

impl From<AWSet> for AwSetMessage {
    fn from(domain: AWSet) -> Self {
        let convert_map = |input_map: HashMap<String, HashSet<Dot>>| {
            input_map
                .into_iter()
                .map(|(tag, dots)| {
                    let proto_dots = dots.into_iter().map(ProtoDot::from).collect();
                    (tag, ProtoDotSet { dots: proto_dots })
                })
                .collect()
        };
        Self {
            clock: domain.clock,
            add_tags: convert_map(domain.add_tags),
            remove_tags: convert_map(domain.remove_tags),
        }
    }
}

impl From<AwSetMessage> for AWSet {
    fn from(wire: AwSetMessage) -> Self {
        let convert_map = |input_map: HashMap<String, ProtoDotSet>| {
            input_map
                .into_iter()
                .map(|(tag, dot_set)| {
                    let domain_dots = dot_set.dots.into_iter().map(Dot::from).collect();
                    (tag, domain_dots)
                })
                .collect()
        };
        Self {
            clock: wire.clock,
            add_tags: convert_map(wire.add_tags),
            remove_tags: convert_map(wire.remove_tags),
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
        // let map = Arc::clone(&self.store);

        let command = Command::from_str(&value_type).unwrap_or(Command::Unknown);

        match command {
            Command::SetCounter => self.handle_set_counter(key, raw_value_bytes).await,
            Command::GetCounter => self.handle_get_counter(key).await,
            Command::IncCounter => self.handle_inc_counter(key, raw_value_bytes).await,
            Command::DecCounter => self.handle_dec_counter(key, raw_value_bytes).await,
            Command::SetAdd => self.handle_add_set(key, raw_value_bytes).await,
            Command::SetRemove => self.handle_rem_set(key, raw_value_bytes).await,
            Command::GetSet => self.handle_get_set(key).await,
            Command::Unknown => {
                println!("Unknown command received");
                Ok(tonic::Response::new(PropagateDataResponse {
                    success: false,
                    response: Vec::new(),
                }))
            }
            _ => {
                println!("Command {:?} not implemented yet", command);
                Ok(tonic::Response::new(PropagateDataResponse {
                    success: false,
                    response: Vec::new(),
                }))
            }
        }
    }

    async fn gossip_changes(
        &self,
        changes: tonic::Request<GossipChangesRequest>,
    ) -> Result<tonic::Response<GossipChangesResponse>, tonic::Status> {
        let changes_inner = changes.into_inner();
        let key = changes_inner.key;
        let crdt_data = match changes_inner.counter {
            Some(msg) => msg,
            None => return Ok(Response::new(GossipChangesResponse { success: false })),
        };
        // let remote_counter = PNCounter::from(counter); //the actual PNCounter type
        let remote_crdt = match crdt_data.data {
            Some(Data::PnCounter(wire)) => {
                //convert Proto -> Domain
                let domain_counter = PNCounter::from(wire);
                CRDTValue::Counter(domain_counter)
            }
            Some(Data::AwSet(wire)) => {
                //same thing, convert Proto -> Domain
                let domain_set = AWSet::from(wire);
                CRDTValue::AWSet(domain_set)
            }
            None => {
                println!("Received CRDTData but the oneof field was empty");
                return Ok(Response::new(GossipChangesResponse { success: false }));
            }
        };

        //call merge now with the value corresponding to the same key in this node
        self.store
            .entry(key.clone())
            .and_modify(|stored_value| {
                match (&mut stored_value.data, &remote_crdt) {
                    //match wrt both the values
                    (CRDTValue::Counter(local_counter), CRDTValue::Counter(remote_counter)) => {
                        let old_state = local_counter.clone();

                        local_counter.merge(&mut remote_counter.clone());

                        if *local_counter != old_state {
                            println!("Merged NEW update for {}", key);
                            stored_value.last_updated = SystemTime::now();
                        } else {
                            println!("Ignored redundant update for {}", key);
                        }
                    }

                    (CRDTValue::AWSet(local_set), CRDTValue::AWSet(remote_set)) => {
                        let old_state = local_set.clone();

                        local_set.merge(&mut remote_set.clone());

                        if *local_set != old_state {
                            println!("Merged NEW update for {}", key);
                            stored_value.last_updated = SystemTime::now();
                        } else {
                            println!("Ignored redundant update for {}", key);
                        }
                    }

                    _ => println!(
                        "type mismatch: key exisits, but value is not of type PNCounter or AWSet"
                    ),
                }

                stored_value.last_updated = SystemTime::now()
            })
            .or_insert_with(|| StoredValue {
                data: remote_crdt.clone(),
                last_updated: SystemTime::now(),
            });

        Ok(Response::new(GossipChangesResponse { success: true }))
    }

    async fn gossip_batch(
        &self,
        batch: tonic::Request<GossipBatchRequest>,
    ) -> Result<tonic::Response<GossipBatchResponse>, tonic::Status> {
        let batch = batch.into_inner().batch;
        for (key, crdt_data) in batch {
            let remote_crdt = match crdt_data.data {
                Some(Data::PnCounter(wire)) => {
                    let domain_counter = PNCounter::from(wire);
                    CRDTValue::Counter(domain_counter)
                }
                Some(Data::AwSet(wire)) => {
                    let domain_set = AWSet::from(wire);
                    CRDTValue::AWSet(domain_set)
                }
                None => {
                    println!("Received CRDTData but the oneof field was empty");
                    return Ok(Response::new(GossipBatchResponse { success: false }));
                }
            };

            self.store
                .entry(key.clone())
                .and_modify(|stored_value| {
                    match (&mut stored_value.data, &remote_crdt) {
                        (CRDTValue::Counter(local_counter), CRDTValue::Counter(remote_counter)) => {
                            let old_state = local_counter.clone();

                            local_counter.merge(&mut remote_counter.clone());

                            if *local_counter != old_state {
                                println!("Merged NEW update for {}", key);
                                stored_value.last_updated = SystemTime::now();
                            } else {
                                println!("Ignored redundant update for {}", key);
                            }
                        },

                        (CRDTValue::AWSet(local_set), CRDTValue::AWSet(remote_set)) => {
                            let old_state = local_set.clone();

                            local_set.merge(&mut remote_set.clone());

                            if *local_set != old_state {
                                println!("Merged NEW update for {}", key);
                                stored_value.last_updated = SystemTime::now();
                            }else {
                                println!("Ignored redundant update for {}", key);
                            }
                        },

                        _ => println!("type mismatch: key exisits, but value is not of type PNCounter or AWSet"),
                    }

                    stored_value.last_updated = SystemTime::now()
                })
                .or_insert_with(|| StoredValue {
                    data: remote_crdt.clone(),
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

    pub async fn handle_set_counter(
        &self,
        key: String,
        raw_value_bytes: Vec<u8>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
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
        self.store.insert(
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
    }

    pub async fn handle_get_counter(
        &self,
        key: String,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        println!("received valid CGET, get value of key: {}", key);

        let val = match self.store.get_mut(&key) {
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
    }

    pub async fn handle_inc_counter(
        &self,
        key: String,
        raw_value_bytes: Vec<u8>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        let bytes: [u8; 8] = raw_value_bytes.try_into().map_err(|_| {
            tonic::Status::invalid_argument("invalid byte length for u64, expected 8 bytes")
        })?;

        let numeric_val: u64 = u64::from_be_bytes(bytes);

        println!("received valid CINC, to increase by: {}", numeric_val);

        let mut val = match self.store.get_mut(&key) {
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
    }

    pub async fn handle_dec_counter(
        &self,
        key: String,
        raw_value_bytes: Vec<u8>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        let bytes: [u8; 8] = raw_value_bytes.try_into().map_err(|_| {
            tonic::Status::invalid_argument("invalid byte length for u64, expected 8 bytes")
        })?;

        let numeric_val: u64 = u64::from_be_bytes(bytes);

        println!("received valid CDEC, to decrease by: {}", numeric_val);

        let mut val = match self.store.get_mut(&key) {
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
    }

    pub async fn handle_add_set(
        &self,
        key: String,
        raw_value_bytes: Vec<u8>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        
        let tag = String::from_utf8(raw_value_bytes).map_err(|_| tonic::Status::invalid_argument("Invalid UTF-8 sequence for tag"))?;

        println!("received valid SADD, to add tag: {}", tag);

        let mut stored_val = self.store.entry(key.clone()).or_insert_with(|| {
            let set = AWSet {
                clock: 0,
                add_tags: HashMap::new(),
                remove_tags: HashMap::new(),
            };

            println!("Set set!");

            StoredValue {
                data: CRDTValue::AWSet(set),
                last_updated: SystemTime::now(),
            }
        });

        match &mut stored_val.data {
            CRDTValue::AWSet(set) => {
                set.add(tag, self.config.node_id.clone()); //finally add the tag

                match self.push(key, CRDTValue::AWSet(set.clone())).await {
                    //propagate
                    Ok(_) => {}
                    Err(_) => {}
                }

                return Ok(Response::new(PropagateDataResponse {
                    success: true,
                    response: Vec::new(),
                }));
            }
            _ => println!("type mismatch: key exisits, but value is not of type AWSet"),
        }

        Ok(Response::new(PropagateDataResponse {
            success: false,
            response: Vec::new(),
        }))
    }

    pub async fn handle_rem_set(
        &self,
        key: String,
        raw_value_bytes: Vec<u8>,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {

        let tag = String::from_utf8(raw_value_bytes).map_err(|_| tonic::Status::invalid_argument("Invalid UTF-8 sequence for tag"))?;

        println!("received valid SREM, to remove tag: {}", tag);

        //doesnt make sense to remove tag from key which does not exist
        let mut stored_val = match self.store.get_mut(&key) {
            Some(val) => val,
            None => {
                return Err(tonic::Status::not_found("The requested key was not found!"));
            }
        };

        match &mut stored_val.data {
            CRDTValue::AWSet(set) => {
                set.remove(tag); //remove the tag

                match self.push(key, CRDTValue::AWSet(set.clone())).await {
                    //propagate
                    Ok(_) => {}
                    Err(_) => {}
                }

                return Ok(Response::new(PropagateDataResponse {
                    success: true,
                    response: Vec::new(),
                }));
            }
            _ => println!("type mismatch: key exisits, but value is not of type AWSet"),
        }

        Ok(Response::new(PropagateDataResponse {
            success: false,
            response: Vec::new(),
        }))
    }

    pub async fn handle_get_set(
        &self,
        key: String,
    ) -> Result<tonic::Response<PropagateDataResponse>, tonic::Status> {
        let stored_val = match self.store.get_mut(&key) {
            Some(val) => val,
            None => {
                return Err(tonic::Status::not_found("The requested key was not found!"));
            }
        };
        match &stored_val.data {
            CRDTValue::AWSet(set) => {
                let value: Vec<_> = set.read().into_iter().collect();
                let response_bytes = serde_json::to_vec(&value).unwrap();
                return Ok(Response::new(PropagateDataResponse {
                    success: true,
                    response: response_bytes,
                }));
            }
            _ => println!("type mismatch: key exisits, but value is not of type AWSet"),
        }
        Ok(Response::new(PropagateDataResponse {
            success: false,
            response: Vec::new(),
        }))
    }

    pub async fn push(&self, key: String, value: CRDTValue) -> Result<()> {
        //send updates to k randomly chosen peers
        //first make sure to preconnect to 3 randomly chosen peer nodes
        //lots of things to think of, like what if a node goes down, how will this node reconnect to
        //some other node etc, will tackle these later

        println!("Receieved {}-{:#?} to {}", key, value, self.config.node_id);

        let mut rng = SmallRng::from_os_rng();

        let chosen_peers: Vec<String> = {
            let peers: Vec<String> = self.peers.iter().map(|entry| entry.key().clone()).collect();
            peers.choose_multiple(&mut rng, K).cloned().collect()
        };

        for peer_addr in chosen_peers.iter() {
            if !self.pool.contains_key(peer_addr) {
                let endpoint = if peer_addr.starts_with("http") {
                    peer_addr.clone()
                } else {
                    format!("http://{}", peer_addr)
                };

                match ReplicationServiceClient::connect(endpoint).await {
                    Ok(client) => {
                        self.pool.insert(peer_addr.clone(), client);
                    }
                    Err(e) => {
                        println!("failed to connect to {}: {}", peer_addr, e);
                        continue;
                    }
                }
            }

            if let Some(mut peer_client) = self.pool.get_mut(peer_addr) {
                match &value {
                    CRDTValue::Counter(inner) => {
                        let wire_counter = PnCounterMessage::from(inner.clone());
                        let oneof_type = Data::PnCounter(wire_counter);

                        let crdt_data = CrdtData {
                            data: Some(oneof_type),
                        };

                        let state = Request::new(GossipChangesRequest {
                            key: key.clone(),
                            counter: Some(crdt_data),
                        });

                        println!("connected to the peer with id: {}", peer_addr);
                        match peer_client.gossip_changes(state).await {
                            Ok(response) => {
                                println!("Response from peer: {:?}", response.into_inner())
                            }
                            Err(e) => println!("failed to send update to {}: {}", peer_addr, e),
                        }
                    }

                    CRDTValue::AWSet(inner) => {
                        let wire_counter = AwSetMessage::from(inner.clone());
                        let oneof_type = Data::AwSet(wire_counter);

                        let crdt_data = CrdtData {
                            data: Some(oneof_type),
                        };

                        let state = Request::new(GossipChangesRequest {
                            key: key.clone(),
                            counter: Some(crdt_data),
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
                }
            }
        }
        Ok(())
    }

    pub async fn create_and_gossip_batch(&self) -> Result<()> {
        //a connection pool of rpc connections so as to not cause redundant ::connect's again if
        //a node has already been connected to in an earlier iteration

        // let mut connection_pool: HashMap<String, ReplicationServiceClient<Channel>> =
        //     HashMap::new();

        loop {
            let mut chosen_peers: Vec<String> = Vec::new();
            for peers in self.peers.iter() {
                if peers.value().elapsed().unwrap_or(Duration::ZERO) > Duration::from_secs(2) {
                    chosen_peers.push(peers.key().clone());
                }
            }

            for peer_addr in &chosen_peers {
                if !self.pool.contains_key(peer_addr) {
                    let endpoint = if peer_addr.starts_with("http") {
                        peer_addr.clone()
                    } else {
                        format!("http://{}", peer_addr)
                    };

                    match ReplicationServiceClient::connect(endpoint).await {
                        Ok(client) => {
                            self.pool.insert(peer_addr.clone(), client);
                        }
                        Err(e) => {
                            println!("failed to connect to {}: {}", peer_addr, e);
                            continue;
                        }
                    }
                }

                //for each key in the current node, transfer each of the node states for merge
                if let Some(mut peer_client) = self.pool.get_mut(peer_addr) {
                    let mut batch = HashMap::new();
                    let mut updates_sent = 0;

                    for mut key_val in self.store.iter_mut() {
                        // let key = key_val.key().clone();
                        let value = key_val.value_mut();

                        if value.last_updated.elapsed().unwrap_or(Duration::ZERO)
                            < Duration::from_secs(2)
                        {
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
    }
}
