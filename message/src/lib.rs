use std::rc::Rc;

pub trait MessageTypeValues {}

pub trait MessageType {
    fn value(&self) -> Box<dyn MessageTypeValues>;
}

pub enum DefaultMessageTypes {
    Default,
}

pub trait Message {
    fn content(&self) -> Rc<Vec<u8>>;
    fn get_type(&self) -> Rc<dyn MessageType>;
}
