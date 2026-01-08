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
#[derive(Debug, Clone)]
pub struct AWSet
{
    pub id: NodeId,
    pub clock: u64,      
    pub add_tags: HashMap<String, HashSet<Dot>>,
    pub remove_tags: HashMap<String, HashSet<Dot>>,
}

impl AWSet
{
    fn new(node_id: NodeId) -> Self {
        AWSet {
            id: node_id,
            clock: 0,
            add_tags: HashMap::new(),
            remove_tags: HashMap::new(),
        }
    }
    
    fn next_dot(&mut self) -> Dot {
        self.clock += 1;
        Dot {
            node_id: self.id.clone(),
            counter: self.clock,
        }
    }

    fn add(&mut self, tag: String) {
        let dot = self.next_dot();
        self.add_tags.entry(tag).or_default().insert(dot);
    }
    
    fn remove(&mut self, tag: String) {
        //all versions of the tag must be tombstoned, even if those came from additions
        //from different nodes
        if let Some(dots) = self.add_tags.get(&tag) {
            for dot in dots {
                self.remove_tags.entry(tag.clone()).or_default().insert(dot.clone());
            }
        }
    }
    
    fn read(&self) -> HashSet<String> {
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
        let node_id = String::from("node_1");
        let mut set = AWSet::new(node_id);

        set.add("apple".to_string());
        set.add("banana".to_string());
        
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
        let mut replica_a = AWSet::new("node_a".to_string());
        replica_a.add("hiking".to_string());

        let mut replica_b = AWSet::new("node_b".to_string());
        replica_b.add("swimming".to_string());

        //merge B into A
        replica_a.merge(&mut replica_b);

        let view = replica_a.read();
        assert!(view.contains("hiking"));
        assert!(view.contains("swimming"));
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_add_wins_concurrent_conflict() {
        // 1. Both nodes start with "apple".
        // 2. Node A removes "apple".
        // 3. Node B concurrently adds "apple" (readds).
        // 4. Merge. "apple" still exists because Bs addition is newer (different dot).

        let mut replica_a = AWSet::new("node_a".to_string());
        replica_a.add("apple".to_string()); // Creates Dot (A, 1)

        //simulate sync: B starts with the same state as A
        let mut replica_b = replica_a.clone();
        replica_b.id = "node_b".to_string(); 

        //Node A removes apple (Tombstones Dot (A,1))
        replica_a.remove("apple".to_string());
        assert!(!replica_a.read().contains("apple"));

        //Node B adds apple concurrently (Creates Dot (B, 2))
        //Clock is 2 as B inherited As clock of 1.
        replica_b.add("apple".to_string());
        assert!(replica_b.read().contains("apple"));

        //merge B into A
        replica_a.merge(&mut replica_b);

        // The set contains:
        // Add-Set: {(A,1), (B,2)}
        // Remove-Set: {(A,1)}
        // (B,2) is in Add but not Remove, so visible.
        assert!(replica_a.read().contains("apple"), "Add should win over Remove in concurrency");
    }

    #[test]
    fn test_remove_sync() {
        let mut replica_a = AWSet::new("node_a".to_string());
        replica_a.add("apple".to_string());

        let mut replica_b = AWSet::new("node_b".to_string());
        
        replica_b.merge(&mut replica_a);
        assert!(replica_b.read().contains("apple"));

        replica_a.remove("apple".to_string());

        replica_b.merge(&mut replica_a);
        
        assert!(!replica_b.read().contains("apple"));
    }

    #[test]
    fn test_merge_is_commutative() {
        let mut replica_a = AWSet::new("node_a".to_string());
        replica_a.add("apple".to_string());
        replica_a.remove("apple".to_string()); 
        replica_a.add("banana".to_string());

        let mut replica_b = AWSet::new("node_b".to_string());
        replica_b.add("apple".to_string()); 
        replica_b.add("cherry".to_string());

        let mut a_then_b = replica_a.clone();
        a_then_b.merge(&mut replica_b);

        let mut b_then_a = replica_b.clone();
        b_then_a.merge(&mut replica_a);

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
