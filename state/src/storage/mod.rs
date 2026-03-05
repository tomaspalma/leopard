pub trait DataState {
    type Item: DataStateItem;

    fn store(&mut self, item: Box<Self::Item>);
    fn get(&self, key: &str) -> Option<Box<Self::Item>>;
}

pub trait DataStateItem {}

pub struct DefaultDataState {}

impl DefaultDataState {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct DefaultDataStateItem {}

impl DataStateItem for DefaultDataStateItem {}

impl DataState for DefaultDataState {
    type Item = DefaultDataStateItem;

    fn store(&mut self, _item: Box<Self::Item>) {}

    fn get(&self, _key: &str) -> Option<Box<Self::Item>> {
        None
    }
}
