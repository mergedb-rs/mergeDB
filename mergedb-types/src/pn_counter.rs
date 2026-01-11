use super::Merge;
use std::collections::HashMap;
use std::cmp;
use crate::NodeId;

//Follows a (node_id, count) model, for the positive and negative counters. An example to make this clear:
//if node_a increments a key, say called "likes", corresponding to which the value is a PNCounter, 
//the state of this value becomes {p: {"node_a": 1}, n: 0}, assuming the value initially was {p: 0, n: 0}.
//Now, node_b also did the same increment independetly to get {p: {"node_b": 1}, n:0}, Then if node_a did 
//another increment, it becomes {p: {"node_a": 2}, n: 0}. Now upon merging say node_b with node_a, we get
//{p: {"node_a": 2, "node_b": 1}, n: 0}. This is obtained by taking the max across the nodes for the value 
//of p or n, and the union-ising it. Then the final value reflected will be 2 + 1 = 3. 

#[derive(Debug, Clone, PartialEq)]
pub struct PNCounter {
    pub p: HashMap<NodeId, u64>,
    pub n: HashMap<NodeId, u64>,
}

impl Merge for PNCounter {
    //when merged, both the replicas get to a common state
    fn merge(&mut self, other: &mut Self) {
        //merge positive counts
        for (node, cnt) in other.p.iter() {
            let entry = self.p.entry(node.clone()).or_insert(0);
            *entry = cmp::max(*entry, cnt.clone());
        }
        
        //merge negative counts
        for (node, cnt) in other.n.iter() {
            let entry = self.n.entry(node.clone()).or_insert(0);
            *entry = cmp::max(*entry, cnt.clone());
        }
    }
}

impl PNCounter {
    pub fn new(node_id: String, p: u64, n: u64) -> Self {
        PNCounter { p: HashMap::from([(node_id.clone(), p)]), n: HashMap::from([(node_id.clone(), n)]) }
    }

    pub fn increment(&mut self, node_id: String, amt: u64) {
        *self.p.entry(node_id).or_insert(0) += amt;
    }

    pub fn decrement(&mut self, node_id: String, amt: u64) {
        *self.n.entry(node_id).or_insert(0) += amt;
    }

    //for the user of the node to see the value of the counter
    pub fn value(&self) -> i64 {
        let p_sum: u64 = self.p.values().sum();
        let n_sum: u64 = self.n.values().sum();
        (p_sum as i64) - (n_sum as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_increments_and_decremenets() {
        let node_id = String::from("node_1");
        let mut counter = PNCounter::new(node_id.clone(), 0, 0);
        counter.increment(node_id.clone(), 1);
        counter.increment(node_id.clone(), 1);
        counter.decrement(node_id.clone(), 1);

        assert_eq!(counter.value(), 1);
    }

    #[test]
    fn merge_maintains_total() {
        let node_id_a = String::from("node_1");
        let mut replica_a = PNCounter::new(node_id_a.clone(), 0, 0);
        replica_a.increment(node_id_a.clone(), 1); //becomes 1 now

        let node_id_b = String::from("node_2");
        let mut replica_b = PNCounter::new(node_id_b.clone(), 1, 0);
        replica_b.increment(node_id_b.clone(), 1); //becomes 2 now

        //merge b's state to a
        replica_a.merge(&mut replica_b);

        assert_eq!(replica_a.value(), 3); //as it should get b's value now

        let node_id_c = String::from("node_3");
        let mut replica_c = PNCounter::new(node_id_c.clone(), 0, 0);
        replica_c.increment(node_id_c.clone(), 1);
        replica_c.increment(node_id_c.clone(), 1);
        replica_c.decrement(node_id_c.clone(), 1);

        let node_id_d = String::from("node_4");
        let mut replica_d = PNCounter::new(node_id_d.clone(), 0, 0);
        replica_d.increment(node_id_d.clone(), 1);
        replica_d.increment(node_id_d.clone(), 1);
        replica_d.increment(node_id_d.clone(), 1);

        replica_c.merge(&mut replica_d);
        assert_eq!(replica_c.value(), 4);
    }

    #[test]
    fn test_merge_is_commutative() {
        let node_id_a = String::from("node_1");
        let mut replica_a = PNCounter::new(node_id_a.clone(), 0, 0);
        replica_a.increment(node_id_a.clone(), 1);

        let node_id_b = String::from("node_2");
        let mut replica_b = PNCounter::new(node_id_b.clone(), 1, 0);
        replica_b.decrement(node_id_b.clone(), 1);

        let mut a_then_b = replica_a.clone();
        a_then_b.merge(&mut replica_b);

        let mut b_then_a = replica_b.clone();
        b_then_a.merge(&mut replica_a);

        //the final state must be identical regardless of merge order
        assert_eq!(a_then_b.value(), b_then_a.value());
    }
}
