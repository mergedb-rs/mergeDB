pub mod config;
pub mod network;

pub mod communication {
    tonic::include_proto!("communication");
}