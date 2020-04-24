use std::convert::TryFrom;
use std::iter::FromIterator;

pub(crate) struct BootPage {
    pub(crate) database_name: String,
}

/// Converts the bytes into an `BootPage`.
///
/// ```text
/// Bytes       Content
/// -----		-------
/// ...         ?
/// 148-404     DatabaseName (nchar(128))
/// ...         ?
/// ```
impl TryFrom<[u8; 8192]> for BootPage {
    type Error = &'static str;

    fn try_from(bytes: [u8; 8192]) -> Result<Self, Self::Error> {
        let (s, _, _) = encoding_rs::UTF_16LE.decode(&bytes[148..(404)]);
        let database_name = String::from_iter(s.chars().filter(|c| *c != '†'));

        Ok(Self { database_name })
    }
}
