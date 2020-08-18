use byteorder::{BigEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::iter::FromIterator;

pub(crate) struct PageHeader {
    pub(crate) slot_count: i16,
}
pub(crate) struct BootPage {
    pub(crate) header: PageHeader,
    pub(crate) database_name: String,
    pub(crate) first_sys_indexes: PagePointer,
}

pub(crate) struct PagePointer {
    pub(crate) page_id: u16,
    pub(crate) file_id: u16,
}

/// Converts the bytes into an `BootPage`.
///
/// ```text
/// Bytes       Content
/// -----		-------
/// ...         ?
/// 148-404     DatabaseName (nchar(128))
/// 612-615     FirstSysIndexes PageID (int)
/// 616-617     FirstSysIndexes FileID (smallint)
/// ...         ?
/// ```
impl TryFrom<[u8; 8192]> for BootPage {
    type Error = &'static str;

    fn try_from(bytes: [u8; 8192]) -> Result<Self, Self::Error> {
        let header = PageHeader::try_from(&bytes[0..96])?;

        let (s, _, _) = encoding_rs::UTF_16LE.decode(&bytes[148..(404)]);
        let database_name = String::from_iter(s.chars().filter(|c| *c != '†'));

        let first_sys_indexes = PagePointer {
            page_id: (&bytes[612..616]).read_u16::<BigEndian>().unwrap(),
            file_id: (&bytes[616..618]).read_u16::<BigEndian>().unwrap(),
        };

        Ok(Self {
            header,
            database_name,
            first_sys_indexes,
        })
    }
}

/// Converts the given bytes into a `PageHeader`.
///
/// ```text
/// Bytes       Content
/// -----		-------
/// ...         ?
/// 22-23       SlotCnt (smallint)
/// ...         ?
/// ```
impl TryFrom<&[u8]> for PageHeader {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        assert!(
            bytes.len() == 96,
            "Page header must be 96 bytes but was {}.",
            bytes.len()
        );

        Ok(PageHeader {
            slot_count: (&bytes[22..24]).read_i16::<BigEndian>().unwrap(),
        })
    }
}
