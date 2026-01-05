use figlet_rs::FIGfont;
use colored::*;
use std::io::{stdin, stdout, Write};

pub fn show_welcome_screen_start() {
    let font = FIGfont::standard().unwrap();
    let figure = font.convert("mergeDB").unwrap();

    print!("\x1B[2J\x1B[1;1H");
    println!("{}", figure.to_string().bright_cyan().bold());
    println!("{}", "A CRDT-powered database!".italic().dimmed());
    println!("{}", "\nPress Enter to start...".yellow().bold());

    let mut input = String::new();
    let _ = stdout().flush();
    stdin().read_line(&mut input).unwrap();
}

pub fn show_prompt() {
    print!("{}", ":: ".bright_green().bold());
    let _ = stdout().flush();
}
