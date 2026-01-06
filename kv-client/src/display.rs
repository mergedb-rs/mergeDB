use anyhow::Result;
use colored::*;
use figlet_rs::FIGfont;
use std::io::{stdin, stdout, Write};

pub fn show_welcome_screen_start() -> Result<()> {
    let font = FIGfont::standard().map_err(|e| anyhow::anyhow!(e))?;
    let figure = match font.convert("mergeDB") {
        Some(fig) => fig,
        None => {
            anyhow::bail!("Couldn't convert the font into figure!");
        }
    };

    print!("\x1B[2J\x1B[1;1H");
    println!("{}", figure.to_string().bright_cyan().bold());
    println!("{}", "A CRDT-powered database!".italic().dimmed());
    println!("{}", "\nPress Enter to start...".yellow().bold());

    let mut input = String::new();
    let _ = stdout().flush();
    stdin().read_line(&mut input)?;
    Ok(())
}

pub fn show_prompt() {
    print!("{}", ":: ".bright_green().bold());
    let _ = stdout().flush();
}
