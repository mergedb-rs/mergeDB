mod cli;
mod display;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use colored::*;
use communication::replication_service_client::ReplicationServiceClient;
use communication::PropagateDataRequest;
use std::fmt::Debug;
use std::io::stdin;
use tonic::Request;

pub mod communication {
    tonic::include_proto!("communication");
}

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

impl ToBytes for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl ToBytes for usize {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let addr = cli.addr.unwrap_or_else(|| "127.0.0.1:8000".to_string());

    let endpoint = format!("http://{}", addr);
    let mut client = ReplicationServiceClient::connect(endpoint.clone()).await?;

    match cli.command {
        Some(Commands::Interactive) | None => {
            display::show_welcome_screen_start()?;
            run_interactive(client).await?;
        }

        Some(Commands::Cset { key, value }) => {
            send_request(&mut client, "CSET", &key, Some(value)).await?;
        }

        Some(Commands::Cget { key }) => {
            send_request::<i64>(&mut client, "CGET", &key, None).await?;
        }

        Some(Commands::Cinc { key, amount }) => {
            send_request(&mut client, "CINC", &key, Some(amount)).await?;
        }

        Some(Commands::Cdec { key, amount }) => {
            send_request(&mut client, "CDEC", &key, Some(amount)).await?;
        }
        
        Some(Commands::Sadd { key, tag }) => {
            send_request(&mut client, "SADD", &key, Some(tag)).await?;
        }
        
        Some(Commands::Srem { key, tag }) => {
            send_request(&mut client, "SREM", &key, Some(tag)).await?;
        }
        
        Some(Commands::Sget { key }) => {
            send_request::<String>(&mut client, "SGET", &key, None).await?;
        }
        
        Some(Commands::Rset { key, register }) => {
            send_request(&mut client, "RSET", &key, Some(register)).await?;
        }
        
        Some(Commands::Rget { key }) => {
            send_request::<String>(&mut client, "RGET", &key, None).await?;
        }
        
        Some(Commands::Rapp { key, reg_append }) => {
            send_request(&mut client, "RAPP", &key, Some(reg_append)).await?;
        }
        
        Some(Commands::Rlen { key }) => {
            send_request::<usize>(&mut client, "RLEN", &key, None).await?;
        }
    }

    Ok(())
}

async fn send_request<T>(
    client: &mut ReplicationServiceClient<tonic::transport::Channel>,
    cmd: &str,
    key: &str,
    value: Option<T>,
) -> Result<(), Box<dyn std::error::Error>> 
where 
    T: ToBytes + Debug,
{
    let bytes = value.map(|v| v.to_bytes()).unwrap_or_default();

    let request = Request::new(PropagateDataRequest {
        valuetype: cmd.to_string(),
        key: key.to_string(),
        value: bytes,
    }); 

    let response = client.propagate_data(request).await?;
    let inner = response.into_inner();
    
    if cmd == "CGET" {
        let raw = inner.response;
        let val = i64::from_be_bytes(raw.try_into().unwrap_or([0; 8]));
        println!("{}", format!(":: {}", val).cyan());
    } else if cmd == "SGET" {
        //has been serialised by json then converted to string then to be_bytes,
        let raw = inner.response;
        let val: Vec<String> = serde_json::from_slice(&raw).expect("failed to desrialise");
        println!("{}", format!(":: {:?}", val).cyan());
    }else if cmd == "RGET" {
        let raw = inner.response;
        let val = match str::from_utf8(&raw) {
            Ok(v) => v,
            Err(_) => "failed to convert to utf8: {}",
        };
        println!("{}", format!(":: {:?}", val).cyan());
    }else if cmd == "RLEN" {
        let raw = inner.response;
        let val = usize::from_be_bytes(raw.try_into().unwrap_or([0; 8]));
        println!("{}", format!(":: {}", val).cyan());
    }
    else {
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
                println!("  SADD <key> <tag>");
                println!("  SREM <key> <tag>");
                println!("  SGET <key>");
                println!("  RSET <key> <register>");
                println!("  RGET <key>");
                println!("  RAPP <key> <to_append>");
                println!("  RLEN <key>");
                println!("  EXIT");
            }

            "EXIT" | "QUIT" => {
                println!("{}", "Goodbye!".blue().bold());
                break;
            }

            "CGET" if parts.len() == 2 => {
                let _ = send_request::<i64>(&mut client, "CGET", parts[1], None).await;
            }
            
            "SGET" if parts.len() == 2 => {
                let _ = send_request::<String>(&mut client, "SGET", parts[1], None).await;
            }
            
            "RGET" if parts.len() == 2 => {
                let _ = send_request::<String>(&mut client, "RGET", parts[1], None).await;
            }
            
            "RLEN" if parts.len() == 2 => {
                let _ = send_request::<usize>(&mut client, "RLEN", parts[1], None).await;
            }

            cmd @ ("CSET" | "CINC" | "CDEC") if parts.len() == 3 => {
                if let Ok(val) = parts[2].parse::<i64>() {
                    let _ = send_request(&mut client, cmd, parts[1], Some(val)).await;
                } else {
                    println!("{}", "Value must be an integer".red());
                }
            }
            
            cmd @ ("SADD" | "SREM") if parts.len() == 3 => {
                let val = parts[2].to_string();
                let _ = send_request(&mut client, cmd, parts[1], Some(val)).await;
            }
            
            cmd @ ("RSET" | "RAPP") if parts.len() == 3 => {
                let val = parts[2].to_string();
                let _ = send_request(&mut client, cmd, parts[1], Some(val)).await;
            }
            
            _ => {
                println!("{}", "Invalid command. Type HELP.".red());
            }
        }
    }

    Ok(())
}
