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
    
    /// add to a set
    Sadd {
        key: String,
        tag: String,
    },
    
    /// remove from a set
    Srem {
        key: String,
        tag: String,
    },
    
    /// get the set
    Sget {
        key: String,
    }
    
}
