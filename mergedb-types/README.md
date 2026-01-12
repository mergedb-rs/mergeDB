# mergedb-types

[![Crates.io](https://img.shields.io/crates/v/mergedb-types.svg)](https://crates.io/crates/mergedb-types)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The core library of Conflict-free Replicated Data Types (CRDTs) powering **MergeDB**.

This crate provides the fundamental data structures that allow MergeDB nodes to diverge and converge automatically without a central coordinator. These types are designed to be state-based, eventually consistent, and partition-tolerant.

## Included CRDTs

### 1. PN-Counter (Positive-Negative Counter)
A distributed counter that supports both increment and decrement operations.
* **Consistency Model:** Strong Eventual Consistency.
* **Mechanism:** Maintains two internal maps (`P` for increments, `N` for decrements) per node ID.
* **Conflict Resolution:** Merges are handled by taking the maximum value for each node's entry in both maps. `Value = ΣP - ΣN`.

### 2. AW-Set (Add-Wins Set)
An Observed-Remove Set (OR-Set) optimized for an "Add-Wins" strategy.
* **Consistency Model:** Add-Wins (if an element is added and removed concurrently, the addition takes precedence).
* **Mechanism:** Uses unique "dots" (NodeID + Logical Clock) to track the provenance of every element.
* **Conflict Resolution:** An element is considered visible if it exists in the `add_tags` map and has not been fully covered by the `remove_tags` tombstones.

### 3. LWW-Register (Last-Write-Wins Register)
A simple register for storing string values (like keys in a standard KV store).
* **Consistency Model:** Last-Write-Wins.
* **Mechanism:** Stores a single value along with a timestamp (logical clock) and the writer's Node ID.
* **Conflict Resolution:**
    1.  Higher logical clock wins.
    2.  **Tie-Breaker:** If clocks are equal, the lexicographically higher Node ID wins.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
mergedb-types = "0.1.0"
```

## Example: Using the PN-Counter

```rust
use mergedb_types::{PNCounter, Merge};

fn main() {
    let node_a = "node_a".to_string();
    let node_b = "node_b".to_string();

    let mut counter_a = PNCounter::new();
    counter_a.increment(node_a.clone(), 10);

    let mut counter_b = PNCounter::new();
    counter_b.increment(node_b.clone(), 20);
    
    // Simulate network sync
    counter_a.merge(&mut counter_b);

    assert_eq!(counter_a.value(), 30);
}
```

## Testing & Coverage
This crate maintains a high standard of unit testing to ensure convergence properties (associativity, commutativity, and idempotence) are met.

### Running Tests
To run the standard test suite:

```bash
cargo test -p mergedb-types
```

### Checking Test Coverage
recommend to use cargo-tarpaulin to verify code coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate Coverage Report
cargo tarpaulin -p mergedb-types --out Html
```

This will generate a tarpaulin-report.html file that visualizes which lines of your CRDT logic are covered by tests.

# License
This project is licensed under the MIT License.