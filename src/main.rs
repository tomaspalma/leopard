mod experiments;

use std::time::Duration;

use clap::{Parser, Subcommand};
use metrics::set_global_recorder;
use runtime::metrics::csv::CsvRecorder;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    CustomNodes {
        #[arg(long, default_value = "merkle")]
        protocol: String,
        #[arg(long)]
        nodes: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let recorder = CsvRecorder::new();
    recorder
        .clone()
        .start_exporter("./metrics_output".to_string(), Duration::from_secs(5));
    set_global_recorder(recorder).expect("Failed to set global metrics recorder");

    match &cli.command {
        Commands::CustomNodes { protocol, nodes } => {
            experiments::custom_nodes(protocol.clone(), nodes.clone()).await;
        }
    }
}
