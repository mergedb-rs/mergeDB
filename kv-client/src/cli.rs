use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "mergeDB",
    version,
    about = "A CRDT-powered database!",
    long_about = None
)]
pub struct Cli {
    /// Address of the node (e.g. 127.0.0.1:8000)
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
}
