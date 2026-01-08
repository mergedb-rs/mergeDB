pub mod aw_set;
pub mod lww_set;
pub mod pn_counter;

pub type NodeId = String;

pub trait Merge {
    fn merge(&mut self, other: &mut Self);
}

//this enum is the value, so mergeDB really would be storing key : CrdtValue
pub enum CrdtValue {
    Counter(pn_counter::PNCounter),
    Register(lww_set::LwwSet),
    Set(aw_set::AWSet), //for now its String
}
