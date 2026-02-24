use std::rc::Rc;

pub trait MessageType {
    type PossibleEnumValues;

    fn value(&self) -> Self::PossibleEnumValues;
}

pub enum DefaultMessageTypes {
    Default,
}

pub struct DefaultMessageType;

impl MessageType for DefaultMessageType {
    type PossibleEnumValues = DefaultMessageTypes;

    fn value(&self) -> DefaultMessageTypes {
        DefaultMessageTypes::Default
    }
}

pub trait Message<MType>
where
    MType: MessageType,
{
    fn content(&self) -> Rc<Vec<u8>>;
    fn get_type(&self) -> Rc<MType>;
}

pub struct DefaultMessage {
    _type: Rc<DefaultMessageType>,
    content: Rc<Vec<u8>>,
}

impl DefaultMessage {
    pub fn new() -> Self {
        Self {
            _type: Rc::new(DefaultMessageType),
            content: Rc::new(Vec::new()),
        }
    }
}

impl Message<DefaultMessageType> for DefaultMessage {
    fn get_type(&self) -> Rc<DefaultMessageType> {
        self._type.clone()
    }

    fn content(&self) -> Rc<Vec<u8>> {
        self.content.clone()
    }
}
