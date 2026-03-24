#[macro_export]
macro_rules! spawn {
    ($block:block) => {
        $crate::tokio::spawn(async move {
            $block
        })
    };

    ($func:ident($($arg:expr),* $(,)?)) => {
        $crate::tokio::spawn(async move {
            $func($($arg),*).await
        })
    };
}
