use crate::metrics::experiment::ExperimentContext;

pub mod csv;
pub mod experiment;
pub mod resource;

pub struct MetricRegistry {}

impl MetricRegistry {
    pub fn record_counter_metric(
        protocol_id: u64,
        bytes_sent: u64,
        target_str: &str,
        protocol_label: &str,
        context: &ExperimentContext,
    ) {
        let run_id = context.run_id().to_string();
        let trial = context.trial().to_string();
        let similarity = context.similarity().to_string();

        match protocol_id {
            1 => {
                metrics::counter!(
                    "riblt_bytes_sent",
                    "target" => target_str.to_string(),
                    "run_id" => run_id,
                    "trial" => trial,
                    "similarity" => similarity
                )
                .increment(bytes_sent);
            }
            2 => {
                metrics::counter!(
                    "merkle_bytes_sent",
                    "target" => target_str.to_string(),
                    "run_id" => run_id,
                    "trial" => trial,
                    "similarity" => similarity
                )
                .increment(bytes_sent);
            }
            3 => {
                metrics::counter!(
                    "rbf_riblt_bytes_sent",
                    "target" => target_str.to_string(),
                    "protocol" => protocol_label.to_string(),
                    "run_id" => run_id,
                    "trial" => trial,
                    "similarity" => similarity
                )
                .increment(bytes_sent);
            }
            _ => {}
        }
    }
}
