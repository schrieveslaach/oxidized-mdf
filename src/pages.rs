use bitvec::{order::Lsb0, slice::BitSlice};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{DateTime, Duration, TimeZone, Utc};
use core::iter::Iterator;
use std::convert::TryFrom;
use std::iter::FromIterator;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct PageHeader {
    pub(crate) slot_count: u16,
    pub(crate) next_page_pointer: Option<PagePointer>,
}

pub struct BootPage {
    pub(crate) header: PageHeader,
    pub(crate) database_name: String,
    pub(crate) first_sys_indexes: PagePointer,
}

#[derive(Debug)]
pub(crate) struct Record<'a> {
    fixed_bytes: &'a [u8],
    r#type: RecordType,
    variable_columns: Option<VariableColumns<'a>>,
}

#[derive(Debug)]
enum RecordType {
    Primary,
    Forwarded,
    ForwardingStub,
    Index,
    BlobFragment,
    GhostIndex,
    GhostData,
    GhostVersion,
}

impl<'a> TryFrom<&'a [u8]> for Record<'a> {
    type Error = &'static str;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        // Bits 1-3 represents record type
        let record_type = (bytes[0] & 0b0000_1110) >> 1;
        let r#type = match record_type {
            0 => RecordType::Primary,
            1 => RecordType::Forwarded,
            2 => RecordType::ForwardingStub,
            3 => RecordType::Index,
            4 => RecordType::BlobFragment,
            5 => RecordType::GhostIndex,
            6 => RecordType::GhostData,
            7 => RecordType::GhostVersion,
            _ => panic!("Unknown record type: {}", record_type),
        };

        // Bit 4 determines whether a null bitmap is present
        let has_null_bitmap = (bytes[0] & 0b0001_0000) > 0;

        // Bit 5 determines whether there are variable length columns
        let has_variable_length_columns = (bytes[0] & 0b0010_0000) > 0;

        let mut bytes = &bytes[2..];

        // Parse fixed length size
        let fixed_length_size = {
            let fixed_length_size = bytes.read_u16::<LittleEndian>().unwrap();
            fixed_length_size - 4
        };

        let (fixed_bytes, mut bytes) = bytes.split_at(fixed_length_size as usize);

        let number_of_columns = bytes.read_u16::<LittleEndian>().unwrap() as usize;

        let (null_bitmap, bytes) = if has_null_bitmap {
            let (null_bitmap, bytes) = bytes.split_at((number_of_columns + 7) / 8);
            (Some(null_bitmap), bytes)
        } else {
            (None, bytes)
        };

        let variable_columns = if has_variable_length_columns {
            Some(VariableColumns::new(bytes, null_bitmap))
        } else {
            None
        };

        Ok(Self {
            fixed_bytes,
            r#type,
            variable_columns,
        })
    }
}

impl<'a> Record<'a> {
    pub(crate) fn has_variable_length_columns(&self) -> bool {
        self.variable_columns.is_some()
    }

    pub(crate) fn parse_i8(self) -> Result<(i8, Record<'a>), &'static str> {
        let (mut bytes, record) = self.parse_bytes(1)?;

        let n = bytes.read_i8().unwrap();

        Ok((n, record))
    }

    pub(crate) fn parse_i16(self) -> Result<(i16, Record<'a>), &'static str> {
        let (mut bytes, record) = self.parse_bytes(2)?;

        let n = bytes.read_i16::<LittleEndian>().unwrap();

        Ok((n, record))
    }

    pub(crate) fn parse_i32(self) -> Result<(i32, Record<'a>), &'static str> {
        let (mut bytes, record) = self.parse_bytes(4)?;

        let n = bytes.read_i32::<LittleEndian>().unwrap();

        Ok((n, record))
    }

    pub(crate) fn parse_i64(self) -> Result<(i64, Record<'a>), &'static str> {
        let (mut bytes, record) = self.parse_bytes(8)?;

        let n = bytes.read_i64::<LittleEndian>().unwrap();

        Ok((n, record))
    }

    fn parse_u128(self) -> Result<(u128, Record<'a>), &'static str> {
        let (mut bytes, record) = self.parse_bytes(16)?;

        let n = bytes.read_u128::<LittleEndian>().unwrap();

        Ok((n, record))
    }

    pub(crate) fn parse_bit(self) -> Result<(bool, Record<'a>), &'static str> {
        let (bytes, record) = self.parse_bytes(1)?;

        Ok((bytes[0] > 0, record))
    }

    const CLOCK_TICK_MS: f64 = 10.0 / 3.0;

    pub(crate) fn parse_datetime(self) -> Result<(DateTime<Utc>, Record<'a>), &'static str> {
        let (time, record) = self.parse_i32()?;
        let (days, record) = record.parse_i32()?;

        let datetime = Utc.ymd(1900, 1, 1).and_hms(0, 0, 0)
            + Duration::milliseconds((time as f64 * Self::CLOCK_TICK_MS) as i64)
            + Duration::days(days as i64);

        Ok((datetime, record))
    }

    pub(crate) fn parse_bytes(self, len: usize) -> Result<(&'a [u8], Record<'a>), &'static str> {
        let (bytes, remaining_bytes) = &self.fixed_bytes.split_at(len);

        let record = Self {
            fixed_bytes: remaining_bytes,
            r#type: self.r#type,
            variable_columns: self.variable_columns,
        };

        Ok((bytes, record))
    }

    pub(crate) fn parse_string_from_fixed_bytes(
        self,
        len: usize,
    ) -> Result<(String, Record<'a>), &'static str> {
        let (bytes, record) = self.parse_bytes(len)?;

        let (s, _, _) = encoding_rs::UTF_8.decode(bytes);
        let s = s.into_owned();

        Ok((s, record))
    }

    pub(crate) fn parse_string(self) -> Result<(Option<String>, Record<'a>), &'static str> {
        let mut variable_columns = match self.variable_columns {
            Some(columns) => columns,
            None => {
                return Err("no variable column data");
            }
        };

        let first = match variable_columns.next() {
            Some(first) => first,
            None => {
                return Err("No more variable data available.");
            }
        };

        let s = first.map(|first| {
            let (s, _, _) = encoding_rs::UTF_16LE.decode(first);
            s.into_owned()
        });

        let record = Self {
            fixed_bytes: self.fixed_bytes,
            r#type: self.r#type,
            variable_columns: Some(variable_columns),
        };

        Ok((s, record))
    }

    pub(crate) fn parse_uuid(self) -> Result<(Uuid, Self), &'static str> {
        let (bytes, record) = self.parse_u128()?;

        let uuid = Uuid::from_u128_le(bytes);

        Ok((uuid, record))
    }
}

#[derive(Debug)]
struct VariableColumns<'a> {
    variable_columns: std::vec::IntoIter<&'a [u8]>,
    null_bitmap: Option<&'a BitSlice<Lsb0, u8>>,
    index: usize,
}

impl<'a> VariableColumns<'a> {
    fn new(bytes: &'a [u8], null_bitmap: Option<&'a [u8]>) -> Self {
        Self {
            variable_columns: Self::parse_variable_columns(&bytes),
            null_bitmap: null_bitmap
                .map(|null_bitmap| BitSlice::from_slice(null_bitmap))
                .flatten(),
            index: 0,
        }
    }

    fn parse_variable_columns<'b>(mut bytes: &'b [u8]) -> std::vec::IntoIter<&'b [u8]> {
        let number_of_variable_length_columns = bytes.read_u16::<LittleEndian>().unwrap();

        /* TODO: from the original coder
        // If there is no fixed length data and no null bitmap, only the number of variable length columns is stored.
        if (FixedLengthData.Length == 0 && !HasNullBitmap)
            NumberOfVariableLengthColumns = NumberOfColumns;
        else
        {
            NumberOfVariableLengthColumns = BitConverter.ToInt16(bytes, offset);
            offset += 2;
        }
        */

        let mut variable_length_column_lengths =
            Vec::with_capacity(number_of_variable_length_columns as usize);
        for _i in 0..number_of_variable_length_columns {
            variable_length_column_lengths.push(bytes.read_i16::<LittleEndian>().unwrap());
        }

        let mut colmuns = Vec::with_capacity(number_of_variable_length_columns as usize);
        for length in variable_length_column_lengths.into_iter() {
            let length = std::cmp::min(length as usize, bytes.len());

            let (column_bytes, remaining_bytes) = bytes.split_at(length);
            colmuns.push(column_bytes);
            bytes = remaining_bytes;
        }

        colmuns.into_iter()
    }
}

impl<'a> Iterator for VariableColumns<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(null_bitmap) = self.null_bitmap {
            if null_bitmap[self.index] {
                self.index += 1;
                return Some(None);
            }
        }

        self.index += 1;
        let item = self.variable_columns.next()?;
        Some(Some(item))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PagePointer {
    pub(crate) page_id: u16,
    pub(crate) file_id: u16,
}

impl PagePointer {
    pub(crate) fn with_page_id(&self, page_id: u16) -> Self {
        Self {
            page_id,
            file_id: self.file_id,
        }
    }
}

impl TryFrom<&[u8]> for PagePointer {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 6 {
            return Err("Page pointer must be 6 bytes.");
        }

        Ok(Self {
            page_id: (&bytes[0..4]).read_u16::<LittleEndian>().unwrap(),
            file_id: (&bytes[4..6]).read_u16::<LittleEndian>().unwrap(),
        })
    }
}

/// Converts the bytes into an `BootPage`.
///
/// ```text
/// Bytes       Content
/// -----       -------
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

        let first_sys_indexes = PagePointer::try_from(&bytes[612..618])?;

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
/// -----       -------
/// ...         ?
//  16-19       NextPageID (int)
/// 20-21       NextPageFileID (smallint)
/// 22-23       SlotCnt (smallint)
/// ...         ?
/// ```
impl TryFrom<&[u8]> for PageHeader {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 96 {
            return Err("Page header must be 96 bytes.");
        }

        let next_page_pointer = PagePointer::try_from(&bytes[16..22])?;
        let next_page_pointer = if next_page_pointer.page_id > 0 {
            Some(next_page_pointer)
        } else {
            None
        };

        Ok(PageHeader {
            slot_count: (&bytes[22..24]).read_u16::<LittleEndian>().unwrap(),
            next_page_pointer,
        })
    }
}

#[derive(Clone, Debug)]
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

        slots.sort_unstable();

        slots
    }

    pub(crate) fn records<'a, 'b: 'a>(&'b self) -> Vec<Record<'a>> {
        let mut records = Vec::with_capacity(self.header.slot_count as usize);

        let slots = self.slots();
        for (index, slot) in slots.iter().enumerate() {
            if let Some(next_slot) = slots.get(index + 1) {
                let range = *slot..*next_slot;
                let record = Record::try_from(&self.bytes[range]).unwrap();
                records.push(record);
            } else {
                // TODO: what about this case???
            }
        }
        records
    }

    pub(crate) fn next_page_pointer(&self) -> Option<&PagePointer> {
        self.header.next_page_pointer.as_ref()
    }
}

impl TryFrom<[u8; 8192]> for Page {
    type Error = &'static str;

    fn try_from(bytes: [u8; 8192]) -> Result<Self, Self::Error> {
        let header = PageHeader::try_from(&bytes[0..96])?;

        Ok(Self { header, bytes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 5u8, 0u8, 1u8, 0u8, 0u8], 1i8),
        case(vec![0u8, 0u8, 5u8, 0u8, 255u8, 0u8, 0u8], -1i8)
    )]
    fn parse_i8(bytes: Vec<u8>, expected_value: i8) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_i8().unwrap();

        assert_eq!(expected_value, parsed_value);
    }

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 6u8, 0u8, 1u8, 0u8, 0u8, 0u8], 1i16),
        case(vec![0u8, 0u8, 6u8, 0u8, 255u8, 255u8, 0u8, 0u8], -1i16)
    )]
    fn parse_i16(bytes: Vec<u8>, expected_value: i16) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_i16().unwrap();

        assert_eq!(expected_value, parsed_value);
    }

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 8u8, 0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8], 1i32),
        case(vec![0u8, 0u8, 8u8, 0u8, 255u8, 255u8, 255u8, 255u8, 0u8, 0u8, 0u8, 0u8], -1i32)
    )]
    fn parse_i32(bytes: Vec<u8>, expected_value: i32) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_i32().unwrap();

        assert_eq!(expected_value, parsed_value);
    }

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 12u8, 0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8], 1i64),
        case(vec![0u8, 0u8, 12u8, 0u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 0u8, 0u8], -1i64)
    )]
    fn parse_i64(bytes: Vec<u8>, expected_value: i64) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_i64().unwrap();

        assert_eq!(expected_value, parsed_value);
    }

    #[rstest(
        bytes,
        expected_value,
        // Bytes copied from data/AWLT2005.mdf
        case(vec![0x30, 0x0, 0x2c, 0x0, 0x4, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0e, 0x0, 0x53, 0x20, 0x0, 0x0, 0x0, 0x0, 0x1, 0x6, 0x0, 0x0, 0x0, 0x15, 0xf6, 0xc2, 0x0, 0x4a, 0x98, 0x0, 0x0, 0x15, 0xf6, 0xc2, 0x0, 0x4a, 0x98, 0x0, 0x0, 0xb, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x54, 0x0, 0x73, 0x0, 0x79, 0x0, 0x73, 0x0, 0x72, 0x0, 0x6f, 0x0, 0x77, 0x0, 0x73, 0x0, 0x65, 0x0, 0x74, 0x0, 0x63, 0x0, 0x6f, 0x0, 0x6c, 0x0, 0x75, 0x0, 0x6d, 0x0, 0x6e, 0x0, 0x73, 0x0], String::from("sysrowsetcolumns"))
    )]
    fn parse_string(bytes: Vec<u8>, expected_value: String) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_string().unwrap();

        assert_eq!(expected_value, parsed_value.unwrap());
    }

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 12u8, 0u8, 0, 0, 0, 0, 249, 148, 0, 0, 0u8, 0u8], Utc.ymd(2004, 6, 1).and_hms(0, 0, 0))
    )]
    fn parse_datetime(bytes: Vec<u8>, expected_value: DateTime<Utc>) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_datetime().unwrap();

        assert_eq!(expected_value, parsed_value);
    }

    #[rstest(
        bytes,
        expected_value,
        case(vec![0u8, 0u8, 20u8, 0u8, 215, 208, 221, 236, 178, 45, 77, 70, 178, 218, 137, 191, 252, 98, 118, 170, 0u8, 0u8], Uuid::from_u128_le(226583458013659211989771997646895829207u128))
    )]
    fn parse_uuid(bytes: Vec<u8>, expected_value: Uuid) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_uuid().unwrap();

        assert_eq!(expected_value, parsed_value);
    }
}
