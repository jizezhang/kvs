use clap::{Parser, Subcommand};
use kvs::KvStore;

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

fn main() {
    let mut kvstore = KvStore::new();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Get { key } => match kvstore.get(key.to_owned()) {
            Some(value) => println!("{}", value),
            None => println!("{} not found", key),
        },
        Commands::Set { key, value } => {
            kvstore.set(key.to_owned(), value.to_owned());
        }
        Commands::Remove { key } => {
            if !kvstore.remove(key.to_owned()) {
                println!("{} not found", key)
            }
        }
    }
}
