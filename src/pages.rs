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
        let mut read_bytes = 2usize;

        // Parse fixed length size
        let fixed_length_size = {
            let fixed_length_size = bytes.read_u16::<LittleEndian>().unwrap();
            fixed_length_size - 4
        };
        read_bytes += 2;

        if fixed_length_size == 0 {
            todo!("No fixed length data! Record cannot be handled yet");
        }

        let (fixed_bytes, mut bytes) = bytes.split_at(fixed_length_size as usize);
        read_bytes += fixed_length_size as usize;

        let number_of_columns = bytes.read_u16::<LittleEndian>().unwrap() as usize;
        read_bytes += 2;

        let (null_bitmap, bytes) = if has_null_bitmap {
            let null_bitmap_length = (number_of_columns + 7) / 8;
            let (null_bitmap, bytes) = bytes.split_at(null_bitmap_length);
            read_bytes += null_bitmap_length;
            (Some(null_bitmap), bytes)
        } else {
            (None, bytes)
        };

        let variable_columns = if has_variable_length_columns {
            Some(VariableColumns::new(read_bytes, bytes, null_bitmap))
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
    variable_columns: &'a [u8],
    variable_length_column_lengths: &'a [u8],
    null_bitmap: Option<&'a BitSlice<Lsb0, u8>>,
    index: usize,
    read_bytes_index: Option<usize>,
}

impl<'a> VariableColumns<'a> {
    fn new(mut read_bytes: usize, mut bytes: &'a [u8], null_bitmap: Option<&'a [u8]>) -> Self {
        let number_of_variable_length_columns = bytes.read_u16::<LittleEndian>().unwrap();
        read_bytes += 2;

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

        let (variable_length_column_lengths, variable_columns) =
            bytes.split_at(number_of_variable_length_columns as usize * 2);

        Self {
            variable_columns,
            variable_length_column_lengths,
            null_bitmap: null_bitmap
                .map(|null_bitmap| BitSlice::from_slice(null_bitmap))
                .flatten(),
            index: 0,
            read_bytes_index: Some(read_bytes + variable_length_column_lengths.len()),
        }
    }
}

impl<'a> Iterator for VariableColumns<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let read_bytes_index = self.read_bytes_index.take()?;

        if self.variable_length_column_lengths.len() < 2 {
            return None;
        }

        let (mut length_bytes, variable_length_column_lengths) =
            self.variable_length_column_lengths.split_at(2);
        self.variable_length_column_lengths = variable_length_column_lengths;

        let end_index_of_readable_bytes = length_bytes.read_i16::<LittleEndian>().unwrap() as usize;
        self.read_bytes_index = Some(end_index_of_readable_bytes);

        if let Some(null_bitmap) = self.null_bitmap {
            if null_bitmap[self.index] {
                self.index += 1;
                return Some(None);
            }
        }

        let length = end_index_of_readable_bytes - read_bytes_index;

        let (bytes, remaining_bytes) = self
            .variable_columns
            .split_at(std::cmp::min(length, self.variable_columns.len()));

        self.variable_columns = remaining_bytes;
        self.index += 1;

        Some(Some(bytes))
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

        let slot_range = (self.bytes.len() - self.header.slot_count as usize * 2)..self.bytes.len();
        let mut slot_bytes = &self.bytes[slot_range];

        while !slot_bytes.is_empty() {
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
            let range = match slots.get(index + 1) {
                Some(next_slot) => *slot..*next_slot,
                None => *slot..self.bytes.len(),
            };

            let record = Record::try_from(&self.bytes[range]).unwrap();
            records.push(record);
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
    use pretty_assertions::assert_eq;
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
        case(vec![0x30, 0x0, 0x2c, 0x0, 0x4, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0e, 0x0, 0x53, 0x20, 0x0, 0x0, 0x0, 0x0, 0x1, 0x6, 0x0, 0x0, 0x0, 0x15, 0xf6, 0xc2, 0x0, 0x4a, 0x98, 0x0, 0x0, 0x15, 0xf6, 0xc2, 0x0, 0x4a, 0x98, 0x0, 0x0, 0xb, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x54, 0x0, 0x73, 0x0, 0x79, 0x0, 0x73, 0x0, 0x72, 0x0, 0x6f, 0x0, 0x77, 0x0, 0x73, 0x0, 0x65, 0x0, 0x74, 0x0, 0x63, 0x0, 0x6f, 0x0, 0x6c, 0x0, 0x75, 0x0, 0x6d, 0x0, 0x6e, 0x0, 0x73, 0x0], String::from("sysrowsetcolumns")),
        // Bytes copied from data/spg_verein_TST.mdf
        case(vec![48, 0, 48, 0, 233, 135, 194, 92, 1, 0, 0, 0, 0, 0, 0, 14, 0, 85, 32, 0, 0, 0, 0, 1, 108, 0, 0, 0, 112, 200, 220, 0, 230, 167, 0, 0, 177, 76, 220, 0, 160, 171, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 1, 0, 80, 0, 116, 0, 98, 0, 108, 0, 95, 0, 77, 0, 105, 0, 116, 0, 103, 0, 108, 0, 105, 0, 101, 0, 100, 0, 108, 0, 105, 0, 101, 0, 100, 0], String::from("tbl_Mitglied")),
    )]
    fn parse_string(bytes: Vec<u8>, expected_value: String) {
        let record = Record::try_from(&bytes[..]).unwrap();

        let (parsed_value, _record) = record.parse_string().unwrap();

        assert_eq!(expected_value, parsed_value.unwrap());
    }

    #[test]
    fn parse_string_with_length() {
        // Bytes copied from data/spg_verein_TST.mdf
        let bytes = vec![
            48, 0, 211, 0, 32, 0, 32, 0, 32, 0, 0, 0, 0, 0, 0, 74, 8, 11, 0, 0, 0, 0, 0, 114, 39,
            11, 8, 0, 0, 0, 0, 0, 136, 97, 240, 116, 2, 0, 0, 0, 208, 97, 240, 0, 0, 0, 0, 0, 229,
            28, 11, 116, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 208, 7, 0, 0, 231,
            116, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 220, 3, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 132, 28, 0, 0, 1, 80, 45, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 57, 11, 0, 0, 209,
            177, 172, 13, 0, 0, 0, 116, 215, 136, 178, 53, 58, 11, 1, 0, 0, 0, 0, 116, 215, 136,
            178, 115, 61, 11, 32, 0, 32, 0, 32, 0, 192, 198, 132, 117, 2, 0, 0, 0, 32, 0, 32, 0,
            32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 116, 215, 136, 178, 53, 58, 11, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 0, 10, 16, 12, 8, 0, 0, 0, 0,
            8, 26, 57, 0, 106, 1, 116, 1, 116, 1, 126, 1, 142, 1, 142, 1, 166, 1, 176, 1, 200, 1,
            200, 1, 200, 1, 212, 1, 212, 1, 212, 1, 214, 1, 220, 1, 242, 1, 16, 2, 16, 2, 16, 2,
            50, 2, 52, 2, 54, 2, 54, 2, 54, 2, 54, 2, 54, 2, 54, 2, 86, 2, 110, 2, 132, 2, 154, 2,
            198, 2, 230, 2, 230, 2, 254, 2, 254, 2, 254, 2, 254, 2, 254, 2, 38, 3, 38, 3, 38, 3,
            48, 3, 54, 3, 66, 3, 82, 3, 82, 3, 106, 3, 116, 3, 126, 3, 168, 3, 168, 3, 186, 3, 206,
            3, 210, 3, 220, 3, 48, 0, 48, 0, 48, 0, 48, 0, 48, 0, 48, 0, 49, 0, 48, 0, 48, 0, 48,
            0, 72, 0, 101, 0, 114, 0, 114, 0, 110, 0, 70, 0, 114, 0, 97, 0, 110, 0, 107, 0, 66, 0,
            101, 0, 114, 0, 103, 0, 109, 0, 97, 0, 110, 0, 110, 0, 82, 0, 101, 0, 98, 0, 101, 0,
            110, 0, 114, 0, 105, 0, 110, 0, 103, 0, 32, 0, 53, 0, 54, 0, 51, 0, 56, 0, 49, 0, 48,
            0, 56, 0, 66, 0, 114, 0, 97, 0, 117, 0, 110, 0, 115, 0, 99, 0, 104, 0, 119, 0, 101, 0,
            105, 0, 103, 0, 49, 0, 49, 0, 50, 0, 50, 0, 51, 0, 51, 0, 109, 0, 49, 0, 53, 0, 48, 0,
            48, 0, 53, 0, 51, 0, 49, 0, 47, 0, 52, 0, 50, 0, 51, 0, 51, 0, 52, 0, 52, 0, 48, 0, 53,
            0, 51, 0, 49, 0, 47, 0, 50, 0, 50, 0, 55, 0, 55, 0, 56, 0, 56, 0, 57, 0, 57, 0, 49, 0,
            49, 0, 101, 0, 114, 0, 32, 0, 72, 0, 101, 0, 114, 0, 114, 0, 32, 0, 66, 0, 101, 0, 114,
            0, 103, 0, 109, 0, 97, 0, 110, 0, 110, 0, 44, 0, 48, 0, 114, 0, 48, 0, 48, 0, 49, 0,
            32, 0, 220, 0, 98, 0, 117, 0, 110, 0, 103, 0, 115, 0, 108, 0, 101, 0, 105, 0, 116, 0,
            101, 0, 114, 0, 48, 0, 48, 0, 50, 0, 32, 0, 76, 0, 105, 0, 122, 0, 101, 0, 110, 0, 122,
            0, 32, 0, 65, 0, 48, 0, 49, 0, 55, 0, 50, 0, 47, 0, 49, 0, 49, 0, 50, 0, 50, 0, 51, 0,
            51, 0, 48, 0, 49, 0, 55, 0, 50, 0, 47, 0, 52, 0, 52, 0, 53, 0, 53, 0, 54, 0, 54, 0,
            102, 0, 114, 0, 97, 0, 110, 0, 107, 0, 46, 0, 98, 0, 101, 0, 114, 0, 103, 0, 109, 0,
            97, 0, 110, 0, 110, 0, 64, 0, 116, 0, 101, 0, 115, 0, 116, 0, 46, 0, 100, 0, 101, 0,
            119, 0, 119, 0, 119, 0, 46, 0, 115, 0, 112, 0, 103, 0, 45, 0, 112, 0, 101, 0, 105, 0,
            110, 0, 101, 0, 46, 0, 100, 0, 101, 0, 102, 0, 117, 0, 115, 0, 115, 0, 98, 0, 97, 0,
            108, 0, 108, 0, 46, 0, 106, 0, 112, 0, 103, 0, 66, 0, 69, 0, 82, 0, 71, 0, 77, 0, 65,
            0, 78, 0, 78, 0, 32, 0, 32, 0, 32, 0, 32, 0, 32, 0, 32, 0, 32, 0, 70, 0, 82, 0, 65, 0,
            78, 0, 75, 0, 72, 0, 101, 0, 114, 0, 114, 0, 110, 0, 68, 0, 114, 0, 46, 0, 72, 0, 117,
            0, 98, 0, 101, 0, 114, 0, 116, 0, 66, 0, 101, 0, 114, 0, 103, 0, 109, 0, 97, 0, 110, 0,
            110, 0, 77, 0, 101, 0, 105, 0, 115, 0, 101, 0, 110, 0, 119, 0, 101, 0, 103, 0, 32, 0,
            49, 0, 53, 0, 51, 0, 49, 0, 50, 0, 50, 0, 56, 0, 80, 0, 101, 0, 105, 0, 110, 0, 101, 0,
            101, 0, 114, 0, 32, 0, 72, 0, 101, 0, 114, 0, 114, 0, 32, 0, 68, 0, 114, 0, 46, 0, 32,
            0, 66, 0, 101, 0, 114, 0, 103, 0, 109, 0, 97, 0, 110, 0, 110, 0, 44, 0, 83, 0, 101, 0,
            103, 0, 101, 0, 108, 0, 98, 0, 111, 0, 111, 0, 116, 0, 49, 0, 53, 0, 46, 0, 48, 0, 51,
            0, 46, 0, 50, 0, 48, 0, 48, 0, 53, 0, 49, 0, 48, 0, 50, 0, 56, 0, 53, 0, 48, 0, 48, 0,
        ];
        let record = Record::try_from(&bytes[..]).unwrap();

        let (id, record) = record.parse_string().unwrap();
        assert_eq!(Some(String::from("0000001000")), id);

        let (id, _record) = record.parse_string().unwrap();
        assert_eq!(Some(String::from("Herrn")), id);
    }

    #[rstest(
        bytes,
        case(vec![0b0010_0000, 0u8, 5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]),
    )]
    fn parse_not_string_due_to_missing_data(bytes: Vec<u8>) {
        let record = Record::try_from(&bytes[..]).unwrap();

        assert_eq!(
            "No more variable data available.",
            record.parse_string().unwrap_err()
        );
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
