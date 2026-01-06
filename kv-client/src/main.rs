mod cli;
mod display;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use colored::*;
use communication::replication_service_client::ReplicationServiceClient;
use communication::PropagateDataRequest;
use std::io::stdin;
use tonic::Request;

pub mod communication {
    tonic::include_proto!("communication");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let addr = cli.addr.unwrap_or_else(|| "127.0.0.1:8000".to_string());

    let endpoint = format!("http://{}", addr);
    let mut client = ReplicationServiceClient::connect(endpoint.clone()).await?;

    match cli.command {
        Some(Commands::Interactive) | None => {
            display::show_welcome_screen_start();
            run_interactive(client).await?;
        }

        Some(Commands::Cset { key, value }) => {
            send_request(&mut client, "CSET", &key, Some(value)).await?;
        }

        Some(Commands::Cget { key }) => {
            send_request(&mut client, "CGET", &key, None).await?;
        }

        Some(Commands::Cinc { key, amount }) => {
            send_request(&mut client, "CINC", &key, Some(amount)).await?;
        }

        Some(Commands::Cdec { key, amount }) => {
            send_request(&mut client, "CDEC", &key, Some(amount)).await?;
        }
    }

    Ok(())
}

async fn send_request(
    client: &mut ReplicationServiceClient<tonic::transport::Channel>,
    cmd: &str,
    key: &str,
    value: Option<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = value.map(|v| v.to_be_bytes().to_vec()).unwrap_or_default();

    let request = Request::new(PropagateDataRequest {
        valuetype: cmd.to_string(),
        key: key.to_string(),
        value: bytes,
    });

    let response = client.propagate_data(request).await?;

    if cmd == "CGET" {
        let raw = response.into_inner().response;
        let val = i64::from_be_bytes(raw.try_into().unwrap_or([0; 8]));
        println!("{}", format!(":: {}", val).cyan());
    } else {
        println!("{}", "✓ OK".green());
    }

    Ok(())
}

async fn run_interactive(mut client: ReplicationServiceClient<tonic::transport::Channel>) -> Result<()>{
    loop {
        crate::display::show_prompt();

        let mut input = String::new();
        stdin().read_line(&mut input)?;
        let parts: Vec<&str> = input.split_whitespace().collect();

        if parts.is_empty() {
            continue;
        }

        match parts[0].to_uppercase().as_str() {
            "HELP" => {
                println!("{}", "Commands:".bold());
                println!("  CSET <key> <value>");
                println!("  CGET <key>");
                println!("  CINC <key> <amount>");
                println!("  CDEC <key> <amount>");
                println!("  EXIT");
            }

            "EXIT" | "QUIT" => {
                println!("{}", "Goodbye!".blue().bold());
                break;
            }

            "CGET" if parts.len() == 2 => {
                let _ = send_request(&mut client, "CGET", parts[1], None).await;
            }

            cmd @ ("CSET" | "CINC" | "CDEC") if parts.len() == 3 => {
                if let Ok(val) = parts[2].parse::<i64>() {
                    let _ = send_request(&mut client, cmd, parts[1], Some(val)).await;
                } else {
                    println!("{}", "Value must be an integer".red());
                }
            }

            _ => {
                println!("{}", "Invalid command. Type HELP.".red());
            }
        }
    }

    Ok(())
}
