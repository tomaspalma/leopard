#[macro_export]
macro_rules! spawn {
    ($block:block) => {
        $crate::spawn(async move {
            $block
        })
    };

    ($func:ident($($arg:expr),* $(,)?)) => {
        $crate::spawn(async move {
            $func($($arg),*).await
        })
    };
}
