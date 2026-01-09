use super::Merge;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};
use crate::NodeId;

//Dot here is used to identify from which node the change has occurred and when(when is handled by counter)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Dot {
    pub node_id: String,
    pub counter: u64,
}


//add_tags structure: {"apple": {("node_1", 1), ("node_1", 5), ("node_2", 3)}}
//similar for remove_tags
#[derive(Debug, Clone, PartialEq)]
pub struct AWSet
{
    pub clock: u64,      
    pub add_tags: HashMap<String, HashSet<Dot>>,
    pub remove_tags: HashMap<String, HashSet<Dot>>,
}

impl AWSet
{
    pub fn new() -> Self {
        AWSet {
            clock: 0,
            add_tags: HashMap::new(),
            remove_tags: HashMap::new(),
        }
    }
    
    pub fn next_dot(&mut self, id: NodeId) -> Dot {
        self.clock += 1;
        Dot {
            node_id: id,
            counter: self.clock,
        }
    }

    pub fn add(&mut self, tag: String, id: NodeId) {
        let dot = self.next_dot(id);
        self.add_tags.entry(tag).or_default().insert(dot);
    }
    
    pub fn remove(&mut self, tag: String) {
        //all versions of the tag must be tombstoned, even if those came from additions
        //from different nodes
        if let Some(dots) = self.add_tags.get(&tag) {
            for dot in dots {
                self.remove_tags.entry(tag.clone()).or_default().insert(dot.clone());
            }
        }
    }
    
    pub fn read(&self) -> HashSet<String> {
        let mut visible_elements = HashSet::new();
        
        for (tag, add_dots) in &self.add_tags {
            let dummy_set = HashSet::new();
            let remove_dots = self.remove_tags.get(tag).unwrap_or(&dummy_set);
            
            //if atleast one more instance of this tag is in add_set, its visible
            if add_dots.difference(remove_dots).count() > 0 {
                visible_elements.insert(tag.clone());
            }
        }
        visible_elements
    }
}

impl Merge for AWSet
{
    //merging would just be union-ising the add_tags and remove_tags
    fn merge(&mut self, other: &mut Self) {
        //merge add_tags
        for (tag, other_add_dots) in &other.add_tags {
            let self_dots = self.add_tags.entry(tag.clone()).or_default();
            for dot in other_add_dots {
                self_dots.insert(dot.clone());
            }
        }
        
        //merge remove_tags
        for (tag, other_remove_dots) in &other.remove_tags {
            let self_dots = self.remove_tags.entry(tag.clone()).or_default();
            for dot in other_remove_dots {
                self_dots.insert(dot.clone());
            }
        }
        
        //sync the self clock, lamport clock logic
        self.clock = std::cmp::max(self.clock, other.clock);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_add_remove() {
        let node_id: NodeId = String::from("node_1");
        let mut set = AWSet::new();

        set.add("apple".to_string(), node_id.clone());
        set.add("banana".to_string(), node_id);
        
        let view = set.read();
        assert!(view.contains("apple"));
        assert!(view.contains("banana"));
        assert_eq!(view.len(), 2);

        set.remove("apple".to_string());
        let view_after = set.read();
        assert!(!view_after.contains("apple"));
        assert!(view_after.contains("banana"));
        assert_eq!(view_after.len(), 1);
    }

    #[test]
    fn test_simple_merge() {
        let node_1: NodeId = String::from("node_1");
        let mut replica_1 = AWSet::new();
        replica_1.add("hiking".to_string(), node_1);

        let node_2: NodeId = String::from("node_2");
        let mut replica_2 = AWSet::new();
        replica_2.add("swimming".to_string(), node_2);

        //merge node_2 into node_1
        replica_1.merge(&mut replica_2);

        let view = replica_1.read();
        assert!(view.contains("hiking"));
        assert!(view.contains("swimming"));
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_add_wins_concurrent_conflict() {
        // 1. Both nodes start with "apple".
        // 2. Node 1 removes "apple".
        // 3. Node 2 concurrently adds "apple" (readds).
        // 4. Merge. "apple" still exists because Bs addition is newer (different dot).

        let node_1: NodeId = String::from("node_1");
        let mut replica_1 = AWSet::new();
        replica_1.add("apple".to_string(), node_1); // Creates Dot (A, 1)

        //simulate sync: B starts with the same state as A
        let node_2: NodeId = String::from("node_2");
        let mut replica_2 = replica_1.clone();

        //Node A removes apple (Tombstones Dot (A,1))
        replica_1.remove("apple".to_string());
        assert!(!replica_1.read().contains("apple"));

        //Node B adds apple concurrently (Creates Dot (B, 2))
        //Clock is 2 as B inherited As clock of 1.
        replica_2.add("apple".to_string(), node_2);
        assert!(replica_2.read().contains("apple"));

        //merge B into A
        replica_1.merge(&mut replica_2);

        // The set contains:
        // Add-Set: {(A,1), (B,2)}
        // Remove-Set: {(A,1)}
        // (B,2) is in Add but not Remove, so visible.
        assert!(replica_1.read().contains("apple"), "Add should win over Remove in concurrency");
    }

    #[test]
    fn test_remove_sync() {
        let node_1: NodeId = String::from("node_1");
        let mut replica_1 = AWSet::new();
        replica_1.add("apple".to_string(), node_1);
        
        let mut replica_2 = AWSet::new();
        
        replica_2.merge(&mut replica_1);
        assert!(replica_2.read().contains("apple"));

        replica_1.remove("apple".to_string());

        replica_2.merge(&mut replica_1);
        
        assert!(!replica_2.read().contains("apple"));
    }

    #[test]
    fn test_merge_is_commutative() {
        let node_1: NodeId = String::from("node_1");
        let mut replica_1 = AWSet::new();
        replica_1.add("apple".to_string(), node_1.clone());
        replica_1.remove("apple".to_string()); 
        replica_1.add("banana".to_string(), node_1);

        let node_2: NodeId = String::from("node_2");
        let mut replica_2 = AWSet::new();
        replica_2.add("apple".to_string(), node_2.clone()); 
        replica_2.add("cherry".to_string(), node_2);

        let mut a_then_b = replica_1.clone();
        a_then_b.merge(&mut replica_2);

        let mut b_then_a = replica_2.clone();
        b_then_a.merge(&mut replica_1);

        //check lengths
        assert_eq!(a_then_b.read().len(), b_then_a.read().len());
        
        //check contents
        let view_a = a_then_b.read();
        assert!(view_a.contains("banana"));
        assert!(view_a.contains("cherry"));
        assert!(view_a.contains("apple"));

        let view_b = b_then_a.read();
        assert_eq!(view_a, view_b);
    }
}
