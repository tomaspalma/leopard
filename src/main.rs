use runtime::{TokioRuntime, runner::Runner};

fn main() {
    let _ = Runner::builder()
        .node()
            .protocol();
}
