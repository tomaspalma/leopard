// use crate::{Runtime, TokioRuntime, builder::{Builder, node::NodeBuilder}};
//
// // use node::Node;
//
// pub struct RunnerBuilder {
//     // nodes: Vec<Node>
// }
//
// impl Builder<Box<dyn Runtime>> for RunnerBuilder {
//     fn build(self) -> Box<dyn Runtime>  {
//         let mut runtime = TokioRuntime::new(None);
//
//         for node in self.nodes.iter() {
//             // runtime.add_task(Box::new(async move || {
//             //     Ok(())
//             // }));
//         }
//
//         Box::new(runtime)
//     }
// }
//
// impl RunnerBuilder {
//     pub fn new() -> Self {
//         RunnerBuilder {
//             nodes: vec![],
//         }
//     }
//
//     // pub fn node(&mut self) -> NodeBuilder {
//     //     // NodeBuilder::new()
//     // }
//
//     fn add_built_node(&mut self, node: Node) -> &mut Self {
//         self.nodes.push(node);
//
//         self
//     }
// }
//
