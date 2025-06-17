use std::env;

use clap::{Parser, Subcommand};
use kvs::error::Result;
use kvs::kvstore::KvStore;

#[derive(Parser, Debug)]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(name = "get")]
    Get { key: String },

    #[command(name = "set")]
    Set { key: String, value: String },

    #[command(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<()> {
    let path = env::current_dir()?;
    let mut kvstore = KvStore::open(&path)?;
    let cli = Cli::parse();

    match &cli.command {
        Commands::Get { key } => {
            match kvstore.get(key.to_owned())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            };
            Ok(())
        }
        Commands::Set { key, value } => kvstore.set(key.to_owned(), value.to_owned()),
        Commands::Remove { key } => match kvstore.remove(key.to_owned()) {
            Err(err) => {
                println!("Key not found");
                Err(err)
            }
            _ => Ok(()),
        },
    }
}
