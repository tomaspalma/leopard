use std::rc::Rc;

pub trait MessageType {}

pub struct DefaultMessageType;

impl MessageType for DefaultMessageType {}

pub trait Message<MType>
where
    MType: MessageType,
{
    fn get_type(&self) -> Rc<MType>;
}

pub struct DefaultMessage {
    _type: Rc<DefaultMessageType>,
}

impl Message<DefaultMessageType> for DefaultMessage {
    fn get_type(&self) -> Rc<DefaultMessageType> {
        self._type.clone()
    }
}
