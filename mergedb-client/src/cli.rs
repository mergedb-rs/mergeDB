use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "mergeDB",
    version,
    about = "A CRDT-powered database!",
    long_about = None
)]
pub struct Cli {
    #[arg(short, long)]
    pub addr: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run in interactive REPL mode
    Interactive,

    /// Set a counter
    Cset {
        key: String,
        value: i64,
    },

    /// Get a counter
    Cget {
        key: String,
    },

    /// Increment a counter
    Cinc {
        key: String,
        amount: i64,
    },

    /// Decrement a counter
    Cdec {
        key: String,
        amount: i64,
    },
    
    /// Add to a set
    Sadd {
        key: String,
        tag: String,
    },
    
    /// Remove from a set
    Srem {
        key: String,
        tag: String,
    },
    
    /// Get the set
    Sget {
        key: String,
    },
    
    /// Set the register
    Rset {
        key: String,
        register: String,
    },
    
    /// Get the register
    Rget {
        key: String,
    },
    
    /// Append to the register
    Rapp {
        key: String,
        reg_append: String,
    },
    
    /// Get register length
    Rlen {
        key: String,
    },
}
