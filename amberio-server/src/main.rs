mod config;
use clap::{Parser, Subcommand};
use config::Config;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod server;
use server::run_server;

#[derive(Parser)]
#[command(name = "amberio")]
#[command(about = "Lightweight object storage for edge cloud nodes")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the server
    Server {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.yaml")]
        config: String,
    },
    /// Initialize a new node
    Init {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.yaml")]
        config: String,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "amberio=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => {
            tracing::info!("Starting Amberio server with config: {}", config);

            let cfg = match Config::from_file(&config) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to load config: {}", e);
                    std::process::exit(1);
                }
            };

            tracing::info!(
                "Node ID: {}, Group ID: {}",
                cfg.node.node_id,
                cfg.node.group_id
            );

            if let Err(e) = run_server(cfg).await {
                tracing::error!("Server error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Init { config } => {
            tracing::info!("Initializing Amberio node with config: {}", config);

            let cfg = match Config::from_file(&config) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to load config: {}", e);
                    std::process::exit(1);
                }
            };

            // Create data directories
            for disk in &cfg.node.disks {
                let amberio_dir = disk.path.join("amberio");
                match std::fs::create_dir_all(&amberio_dir) {
                    Ok(_) => tracing::info!("Created directory: {:?}", amberio_dir),
                    Err(e) => {
                        tracing::error!("Failed to create directory {:?}: {}", amberio_dir, e);
                        std::process::exit(1);
                    }
                }
            }

            tracing::info!(
                "Node {} initialized successfully in group {}",
                cfg.node.node_id,
                cfg.node.group_id
            );
        }
    }
}
