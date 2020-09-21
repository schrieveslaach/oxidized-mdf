use byteorder::{LittleEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::iter::FromIterator;

#[derive(Debug)]
pub(crate) struct PageHeader {
    pub(crate) slot_count: u16,
}

pub struct BootPage {
    pub(crate) header: PageHeader,
    pub(crate) database_name: String,
    pub(crate) first_sys_indexes: PagePointer,

    bytes: [u8; 8192],
}

#[derive(Debug)]
pub(crate) enum Record<'a> {
    Primary(&'a [u8]),
    /* TODO
        Forwarded,
        ForwardingStub,
        Index,
        BlobFragment,
        GhostIndex,
        GhostData,
        GhostVersion,
    */
}

#[derive(Debug)]
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
            page_id: (&bytes[612..616]).read_u16::<LittleEndian>().unwrap(),
            file_id: (&bytes[616..618]).read_u16::<LittleEndian>().unwrap(),
        };

        Ok(Self {
            header,
            database_name,
            first_sys_indexes,
            bytes,
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
        if bytes.len() != 96 {
            return Err("Page header must be 96 bytes.");
        }

        Ok(PageHeader {
            slot_count: (&bytes[22..24]).read_u16::<LittleEndian>().unwrap(),
        })
    }
}

pub(crate) struct Page {
    header: PageHeader,
    bytes: [u8; 8192],
}

impl Page {
    fn slots(&self) -> Vec<usize> {
        let mut slots = Vec::with_capacity(self.header.slot_count as usize);

        for i in 1usize..=self.header.slot_count as usize {
            let index = self.bytes.len() - i * 2;
            let mut slot_bytes = &self.bytes[index..(index + 2)];
            let slot_value = slot_bytes.read_u16::<LittleEndian>().unwrap();
            slots.push(slot_value as usize);
        }

        slots.sort();

        slots
    }

    pub(crate) fn records<'a, 'b: 'a>(&'b self) -> Vec<Record<'a>> {
        let mut records = Vec::with_capacity(self.header.slot_count as usize);

        let slots = self.slots();
        for (index, slot) in slots.iter().enumerate() {
            let record_type = self.bytes[*slot];
            let record_type = (record_type & 0x0E) >> 1;

            if let Some(next_slot) = slots.get(index + 1) {
                let range = *slot..*next_slot;

                let record_type = match record_type {
                    0 => Record::Primary(&self.bytes[range]),
                    record_type => todo!("Unknown record type {}", record_type),
                };

                records.push(record_type);
            } else {
                // TODO...
            }
        }
        records
    }
}

impl TryFrom<[u8; 8192]> for Page {
    type Error = &'static str;

    fn try_from(bytes: [u8; 8192]) -> Result<Self, Self::Error> {
        let header = PageHeader::try_from(&bytes[0..96])?;

        Ok(Self { header, bytes })
    }
}
