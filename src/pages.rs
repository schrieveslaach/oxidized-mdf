use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::iter::FromIterator;

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

impl BootPage {
    // TODO use iterator?
    fn slots(&self) -> Vec<usize> {
        let mut slots = Vec::with_capacity(self.header.slot_count as usize);

        for i in 1..=self.header.slot_count {
            let index = self.bytes.len() - i as usize * 2;
            let mut slot_bytes = &self.bytes[index..(index + 2)];
            let slot_value = slot_bytes.read_u16::<LittleEndian>().unwrap();
            slots.push(slot_value as usize);
        }

        slots
    }

    fn records<'a, 'b: 'a>(&'b self) -> Vec<Record<'a>> {
        let mut types = Vec::with_capacity(self.header.slot_count as usize);

        for i in self.slots() {
            let record_type = self.bytes[i as usize];

            let record_type = (record_type & 0x0E) >> 1;
            let record_type = match record_type {
                0 => Record::Primary(&self.bytes[i..(self.bytes.len() - i)]),
                record_type => todo!("Unknown record type {}", record_type),
            };

            types.push(record_type);
        }
        types
    }

    pub fn sysalloc_units(&self) -> Vec<SysallocUnit> {
        let schema = crate::schema::sysallocunit_schema();

        for record in self.records() {
            match record {
                Record::Primary(bytes) => {
                    let mut bytes = bytes;
                    for column in &schema {
                        println!("Parsing {:?}… bytes", &bytes[0..4]);

                        let (value, rest) = crate::Value::parse(&bytes, &column.column_type);

                        println!("{:?}", value);

                        bytes = rest;
                    }
                }
                _ => todo!(),
            }
        }

        // TODO sysallocunits = scanner.ScanLinkedDataPages<sysallocunit>(bootPage.FirstSysIndexes, CompressionContext.NoCompression).ToList();
        vec![]
    }
}

pub(crate) struct PagePointer {
    pub(crate) page_id: u16,
    pub(crate) file_id: u16,
}

#[derive(Debug)]
pub struct SysallocUnit {}

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
            // TODO Big vs Little
            page_id: (&bytes[612..616]).read_u16::<BigEndian>().unwrap(),
            file_id: (&bytes[616..618]).read_u16::<BigEndian>().unwrap(),
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
        assert!(
            bytes.len() == 96,
            "Page header must be 96 bytes but was {}.",
            bytes.len()
        );

        Ok(PageHeader {
            slot_count: (&bytes[22..24]).read_u16::<LittleEndian>().unwrap(),
        })
    }
}
