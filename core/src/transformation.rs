use async_trait::async_trait;

use crate::traits::{TransformationError, Transformer};

pub struct IdentityTransformer;

#[async_trait]
impl<T> Transformer<T, T> for IdentityTransformer
where
    T: Send + Sync + Sized + 'static,
{
    async fn transform(&self, input: T) -> Result<T, TransformationError> {
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::*;

    #[tokio::test]
    async fn test_identity_transformer() {
        let transformer = IdentityTransformer;
        let input = "test data".to_string();
        let output = transformer.transform(input.clone()).await.unwrap();
        assert_eq!(input, output);
    }
}
