use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub node_id: String,
    pub listen_address: String,
    pub peers: Vec<String>,
}

impl Config {
    pub fn load_config(config_path: PathBuf) -> Result<Self> {
        let mut file = File::open(&config_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let new_config: Self = toml::from_str(&contents)?;

        Ok(new_config)
    }

    pub fn store_config(node: &Self, config_path: PathBuf) -> Result<()> {
        let mut file = File::create(&config_path)?;

        let contents = toml::to_string(node)?;

        file.write_all(contents.as_bytes())?;

        Ok(())
    }
}
