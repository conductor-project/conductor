use super::*;

/// Authenticates a 256-bit URL-safe bearer token while retaining only its SHA-256 digest.
pub struct BearerTokenAuthenticator {
    pub(super) token_digest: [u8; 32],
}

impl OperatorAuthenticator for BearerTokenAuthenticator {
    fn authenticate(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let Some(value) = values.next() else {
            return false;
        };
        if values.next().is_some() {
            return false;
        }
        let Some(token) = value.as_bytes().strip_prefix(b"Bearer ") else {
            return false;
        };
        if token.len() != 43 {
            return false;
        }
        let provided_digest: [u8; 32] = Sha256::digest(token).into();
        bool::from(self.token_digest.ct_eq(&provided_digest))
    }
}

impl BearerTokenAuthenticator {
    pub fn new(token: &[u8]) -> Result<Self, InvalidOperatorToken> {
        if token.len() != 43
            || !token
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(InvalidOperatorToken);
        }
        Ok(Self {
            token_digest: Sha256::digest(token).into(),
        })
    }
}
