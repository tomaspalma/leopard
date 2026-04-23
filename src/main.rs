mod experiments;

use log::info;

use clap::{Parser, Subcommand};
use metrics::set_global_recorder;
use runtime::metrics::csv::CsvRecorder;
use runtime::metrics::experiment::{ExperimentContext, set_context};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "default_run")]
    run_id: String,

    #[arg(long, default_value = "1")]
    trial: String,

    #[arg(long, default_value = "unknown")]
    similarity: String,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    exit_on_reconciliation: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    CustomNodes {
        #[arg(long, default_value = "default")]
        node_type: String,
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

    info!("Setting recorder");

    let output_dir = format!("./metrics_output/{}", cli.run_id);
    let _ = set_context(ExperimentContext::new(
        cli.run_id.clone(),
        cli.trial.clone(),
        cli.similarity.clone(),
    ));
    let recorder = CsvRecorder::new();
    recorder.clone().start_exporter(output_dir);
    set_global_recorder(recorder).expect("Failed to set global metrics recorder");

    match &cli.command {
        Commands::CustomNodes {
            node_type,
            protocol,
            nodes,
        } => {
            experiments::custom_nodes(node_type.clone(), protocol.clone(), nodes.clone(), cli.exit_on_reconciliation).await;
        }
    }
}
