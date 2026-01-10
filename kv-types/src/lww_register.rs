//used for string support, called register

//methods supported: get, set, append, strlen

use super::Merge;
use crate::NodeId;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Dot {
    pub node_id: NodeId,
    pub counter: u64,
    pub register: String
}

//register_state structure: ("node_1", 1, "name1")
#[derive(Debug, Clone, PartialEq)]
pub struct LwwRegister {
    pub clock: u64,
    pub register_state: Dot,
}

impl LwwRegister {
    pub fn new(id: NodeId) -> Self {
        LwwRegister { clock: 0, register_state: Dot{node_id: id, counter: 0, register: String::new()} }
    }
    
    pub fn next_dot(&mut self, id: NodeId) -> Dot {
        self.clock += 1;
        Dot {
            node_id: id,
            counter: self.clock,
            register: String::new(),
        }
    }
    
    pub fn set(&mut self, register: String, id: NodeId) {
        let mut dot = self.next_dot(id);
        dot.register = register;
        self.register_state = dot;
    }
    
    pub fn get(&self) -> String {
        self.register_state.register.clone()
    }
    
    pub fn append(&mut self, to_append: String, id: NodeId) {
        let mut chosen_value = self.get();
        chosen_value.push_str(&to_append);
        
        //insert new entry to register_state: (node_id, clock, chosen_value)
        self.set(chosen_value, id);
    }
    
    pub fn strlen(&self) -> usize {
        self.get().len()
    }
}

impl Merge for LwwRegister {
    fn merge(&mut self, other: &mut Self) {
        //union-ise the register_states
        if self.register_state.counter < other.register_state.counter {
            self.register_state = other.register_state.clone();
        }
        //if equal clocks, then determine based on node ids
        if self.register_state.counter == other.register_state.counter {
            if other.register_state.node_id > self.register_state.node_id {
                self.register_state = other.register_state.clone();
            }
        }
        
        //sync the clocks
        self.clock = std::cmp::max(self.clock, other.clock);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_set_get() {
        let node_id = String::from("node_1");
        let mut reg = LwwRegister::new(node_id.clone());

        assert_eq!(reg.get(), "");

        reg.set("Hello".to_string(), node_id.clone());
        assert_eq!(reg.get(), "Hello");

        reg.set("World".to_string(), node_id);
        assert_eq!(reg.get(), "World");
    }

    #[test]
    fn test_local_append() {
        let node_id = String::from("node_1");
        let mut reg = LwwRegister::new(node_id.clone());

        reg.set("Hello".to_string(), node_id.clone());
        reg.append(", World".to_string(), node_id);

        assert_eq!(reg.get(), "Hello, World");
        assert_eq!(reg.strlen(), 12);
    }

    #[test]
    fn test_simple_merge() {
        let node_1 = String::from("node_1");
        let mut r1 = LwwRegister::new(node_1.clone());
        r1.set("Value A".to_string(), node_1);

        let node_2 = String::from("node_2");
        let mut r2 = LwwRegister::new(node_2.clone());

        //forcing r2 to have higher clock for test clarity
        r2.clock = 10; 
        r2.set("Value B".to_string(), node_2);

        r1.merge(&mut r2);

        assert_eq!(r1.get(), "Value B");
    }

    #[test]
    fn test_concurrent_conflict_resolution() {
        //two nodes update the register at the exact same logical time, 
        //tie-breaker: node_2 > node_1, so node_2 value should win

        let node_1 = String::from("node_1");
        let mut r1 = LwwRegister::new(node_1.clone());
        
        let node_2 = String::from("node_2");
        let mut r2 = LwwRegister::new(node_2.clone());

        // Both set value at clock 1
        r1.set("Lost Value".to_string(), node_1);
        r2.set("Won Value".to_string(), node_2);  

        assert_eq!(r1.register_state.counter, r2.register_state.counter);

        r1.merge(&mut r2);
        assert_eq!(r1.get(), "Won Value", "node_2 should win because 'node_2' > 'node_1'");

        //verify commutativity
        let mut r1_reset = LwwRegister::new(String::from("node_1"));
        r1_reset.set("Lost Value".to_string(), String::from("node_1"));
        
        r2.merge(&mut r1_reset);
        assert_eq!(r2.get(), "Won Value", "node_2 should stay because it beats node_1");
    }

    #[test]
    fn test_merge_is_commutative() {
        let node_1 = String::from("node_1");
        let mut r1 = LwwRegister::new(node_1.clone());
        r1.set("Apple".to_string(), node_1);

        let node_2 = String::from("node_2");
        let mut r2 = LwwRegister::new(node_2.clone());
        r2.set("Banana".to_string(), node_2); 

        let mut a_then_b = r1.clone();
        a_then_b.merge(&mut r2.clone());

        let mut b_then_a = r2.clone();
        b_then_a.merge(&mut r1.clone());

        assert_eq!(
            a_then_b.get(), 
            b_then_a.get(), 
            "Merge order should not matter"
        );
        
        assert_eq!(a_then_b.clock, b_then_a.clock);
    }
    
    #[test]
    fn test_outdated_update_ignored() {
        let node_1 = String::from("node_1");
        let mut r1 = LwwRegister::new(node_1.clone());
        
        r1.clock = 4;
        r1.set("Future Value".to_string(), node_1.clone()); 
        
        let node_2 = String::from("node_2");
        let mut r2 = LwwRegister::new(node_2.clone());
        
        r2.set("Old Value".to_string(), node_2);

        r1.merge(&mut r2);

        assert_eq!(r1.get(), "Future Value");
    }
}