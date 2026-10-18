#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidOperatorToken;

impl std::error::Error for InvalidOperatorToken {}

impl std::fmt::Display for InvalidOperatorToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operator token must be a 32-byte URL-safe token")
    }
}
