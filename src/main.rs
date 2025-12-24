use runtime::Runner;
use runtime::TokioRuntime;

fn main() {
    println!("Hello, world!");

    Runner::new(TokioRuntime)
        .node()
            .protocol()
            .protocol()
}
