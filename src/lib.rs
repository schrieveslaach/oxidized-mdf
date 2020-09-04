#![warn(rust_2018_idioms)]

mod pages;

use async_std::fs::File;
use async_std::io::{Read, Result};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use pages::{BootPage, Page, PagePointer, Record};
use std::convert::TryFrom;
use std::pin::Pin;

pub struct MdfDatabase {
    page_reader: PageReader,
    boot_page: BootPage,
}

impl MdfDatabase {
    pub async fn open<P>(p: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut path = PathBuf::new();
        path.push(p);

        let file = File::open(&path).await?;
        let read = Box::pin(file);
        Self::from_read(read).await
    }

    pub async fn from_read(read: Pin<Box<dyn Read>>) -> Result<Self> {
        let mut buffer = [0u8; 8192];
        let mut page_reader = PageReader::new(read);

        // Skipping first headers
        for _i in 0u8..9u8 {
            page_reader.read_next_page(&mut buffer).await?;
        }
        page_reader.read_next_page(&mut buffer).await?;

        Ok(Self {
            page_reader,
            boot_page: BootPage::try_from(buffer).unwrap(),
        })
    }

    pub fn database_name(&self) -> &String {
        &self.boot_page.database_name
    }

    pub async fn sysalloc_unit(&mut self) -> Result<Vec<SysallocUnit>> {
        let mut buffer = [0u8; 8192];
        self.page_reader
            .read_page(&self.boot_page.first_sys_indexes, &mut buffer)
            .await?;

        let page = Page::try_from(buffer).unwrap();

        let mut units = Vec::with_capacity(10);
        for record in page.records() {
            match record {
                Record::Primary(bytes) => {
                    let sysalloc_unit = SysallocUnit::try_from(bytes).unwrap();
                    units.push(sysalloc_unit);
                }
            }
        }

        Ok(units)
    }
}

struct PageReader {
    read: Pin<Box<dyn Read>>,
    page_index: usize,
}

impl PageReader {
    fn new(read: Pin<Box<dyn Read>>) -> Self {
        Self {
            read,
            page_index: 0,
        }
    }

    async fn read_next_page(&mut self, buffer: &mut [u8; 8192]) -> Result<()> {
        self.read.read_exact(&mut buffer[..]).await?;
        self.page_index += 1;
        Ok(())
    }

    async fn read_page(
        &mut self,
        page_pointer: &PagePointer,
        mut buffer: &mut [u8; 8192],
    ) -> Result<()> {
        assert!(self.page_index < page_pointer.page_id as usize, "Currently the database supports only forward reading and the requested page {} has been already read", page_pointer.page_id);

        for _i in self.page_index..=(page_pointer.page_id as usize) {
            self.read_next_page(&mut buffer).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SysallocUnit {
    /*
        new DataColumn("auid", "bigint"),
        new DataColumn("type", "tinyint"),
        new DataColumn("ownerid", "bigint"),
        new DataColumn("status", "int"),
        new DataColumn("fgid", "smallint"),
        new DataColumn("pgfirst", "binary(6)"),
        new DataColumn("pgroot", "binary(6)"),
        new DataColumn("pgfirstiam", "binary(6)"),
        new DataColumn("pcused", "bigint"),
        new DataColumn("pcdata", "bigint"),
        new DataColumn("pcreserved", "bigint"),
        new DataColumn("dbfragid", "int")
    */
    auid: i64,
    r#type: i8,
    ownerid: i64,
    status: i32,
    fgid: i16,
    pgfirst: Vec<u8>,
    pgroot: Vec<u8>,
    pgfirstiam: Vec<u8>,
    pcused: i64,
    pcdata: i64,
    pcreserved: i64,
    dbfragid: i32,
}

impl TryFrom<&[u8]> for SysallocUnit {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut bytes = bytes;
        let auid = bytes.read_i64::<LittleEndian>().unwrap();

        let r#type = bytes.read_i8().unwrap();

        let ownerid = bytes.read_i64::<LittleEndian>().unwrap();

        let status = bytes.read_i32::<LittleEndian>().unwrap();

        let fgid = bytes.read_i16::<LittleEndian>().unwrap();

        let pgfirst = (&bytes[0..6]).to_vec();
        let bytes = &bytes[6..];

        let pgroot = (&bytes[0..6]).to_vec();
        let bytes = &bytes[6..];

        let pgfirstiam = (&bytes[0..6]).to_vec();
        let mut bytes = &bytes[6..];

        let pcused = bytes.read_i64::<LittleEndian>().unwrap();

        let pcdata = bytes.read_i64::<LittleEndian>().unwrap();

        let pcreserved = bytes.read_i64::<LittleEndian>().unwrap();

        let dbfragid = bytes.read_i32::<LittleEndian>().unwrap();

        Ok(Self {
            auid,
            r#type,
            ownerid,
            status,
            fgid,
            pgfirst,
            pgroot,
            pgfirstiam,
            pcused,
            pcdata,
            pcreserved,
            dbfragid,
        })
    }
}
