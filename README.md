# mergeDB
*A Distributed Key-Value Store Powered by CRDTs*  

---

## Note

To view the Criterion benchmarks: https://mergedb-rs.github.io/mergeDB/bench/report

---

## Overview  
`mergeDB` is a distributed **key-value store** that leverages **Conflict-free Replicated Data Types (CRDTs)** to ensure strong eventual consistency across nodes, without needing consensus protocols like Raft or Paxos.  

Built with modern Rust tooling, `mergeDB` is designed for scalability, fault tolerance, and developer ergonomics.  

---

## Architecture  

- [tokio](https://tokio.rs/) — Asynchronous runtime for fast, non-blocking I/O  
- [tonic](https://github.com/hyperium/tonic) — gRPC framework for inter-node communication  
- [tracing](https://github.com/tokio-rs/tracing) — Structured, high-performance logging  
- [axum](https://github.com/tokio-rs/axum) — API server for client interactions  

---

## Learning Resources  

- [CRDTs: The Hard Parts ~ Martin Kleppmann](https://www.youtube.com/watch?v=x7drE24geUw)  
- [How Key-Value Stores Work (Redis, DynamoDB, Memcached)? ~ ByteByteGo](https://www.youtube.com/watch?v=Dwt8R0KPu7k)  

---

## Future Directions  

- Multi-node replication and synchronization  
- Richer CRDT types beyond registers and maps  
- Benchmarks against Redis and DynamoDB  
- Client libraries in multiple languages  
