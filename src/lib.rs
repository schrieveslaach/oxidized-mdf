//! # A Crate for Parsing MDF files
//!
//! `oxidized-mdf` provides utifities to parse MDF files of the [Microsoft SQL Server](https://en.wikipedia.org/wiki/Microsoft_SQL_Server).

#![warn(rust_2018_idioms)]

mod pages;
mod sys;

use crate::pages::{BootPage, Page, PagePointer};
use crate::sys::BaseTableData;
use async_std::fs::File;
use async_std::io::{Read, Result};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::pin::Pin;
use std::rc::Rc;

pub struct MdfDatabase {
    page_reader: PageReader,
    boot_page: BootPage,
    pub(crate) base_table_data: BaseTableData,
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

        let boot_page = BootPage::try_from(buffer).unwrap();
        let base_table_data = BaseTableData::parse(&mut page_reader, &boot_page).await?;

        Ok(Self {
            page_reader,
            boot_page,
            base_table_data,
        })
    }

    pub fn database_name(&self) -> &str {
        &self.boot_page.database_name
    }

    /// Returns the table names of this database file.
    /// 
    /// ```rust
    /// # use oxidized_mdf::MdfDatabase;
    /// # #[async_std::main]
    /// # async fn main() {
    /// let db = MdfDatabase::open("data/AWLT2005.mdf").await.unwrap();
    /// let table_names = db.table_names();
    /// assert!(table_names.contains(&String::from("CK_Product_ListPrice")));
    /// # }
    /// ```
    pub fn table_names(&self) -> Vec<String> {
        self.base_table_data.tables()
    }
}

struct PageReader {
    read: Pin<Box<dyn Read>>,
    page_index: u16,
    page_cache: HashMap<PagePointer, Rc<Page>>,
}

impl PageReader {
    fn new(read: Pin<Box<dyn Read>>) -> Self {
        Self {
            read,
            page_index: 0,
            page_cache: HashMap::new(),
        }
    }

    async fn read_next_page(&mut self, buffer: &mut [u8; 8192]) -> Result<()> {
        self.read.read_exact(&mut buffer[..]).await?;
        self.page_index += 1;
        Ok(())
    }

    async fn read_page(&mut self, page_pointer: &PagePointer) -> Result<Rc<Page>> {
        if let Some(page) = self.page_cache.get(page_pointer) {
            return Ok(page.clone());
        }

        assert!(self.page_index < page_pointer.page_id, "Currently the database supports only forward reading and the requested page {} has been already read", page_pointer.page_id);

        for i in self.page_index..=page_pointer.page_id {
            let mut buffer = [0u8; 8192];
            self.read_next_page(&mut buffer).await?;

            let page = Page::try_from(buffer).unwrap();

            self.page_cache.insert(page_pointer.with_page_id(i), Rc::new(page));
        }

        let page = self.page_cache.get(page_pointer).unwrap();
        Ok(page.clone())
    }
}
