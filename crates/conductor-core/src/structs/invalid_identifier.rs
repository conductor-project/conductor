use super::*;

/// An identifier failed the stable ASCII identifier format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidIdentifier;

impl std::error::Error for InvalidIdentifier {}

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("identifier must contain 1-128 ASCII letters, digits, '-', '_' or '.'")
    }
}
