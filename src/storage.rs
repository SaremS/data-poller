use async_trait::async_trait;

use crate::traits::{StorageError, Storer};

pub struct ConsoleStorer;

#[async_trait]
impl<T> Storer<T> for ConsoleStorer
where
    T: Into<String> + Send + 'static,
{
    async fn store(&self, input: T) -> Result<(), StorageError> {
        println!("{}\n", input.into());
        Ok(())
    }
}
