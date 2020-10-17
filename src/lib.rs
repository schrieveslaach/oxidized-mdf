//! # A Crate for Parsing MDF files
//!
//! `oxidized-mdf` provides utifities to parse MDF files of the [Microsoft SQL Server](https://en.wikipedia.org/wiki/Microsoft_SQL_Server).

#![warn(rust_2018_idioms)]

mod pages;
mod sys;

use crate::pages::{BootPage, Page, PagePointer, Record};
use crate::sys::{BaseTableData, Column};
use async_std::fs::File;
use async_std::io::{Read, Result};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use core::task::{Context, Poll};
use futures_lite::stream::StreamExt;
use std::collections::{BTreeMap, HashMap};
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
        Self::from_read(Box::new(file)).await
    }

    pub async fn from_read(read: Box<dyn Read + Unpin>) -> Result<Self> {
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
    /// assert!(table_names.contains(&String::from("Customer")));
    /// # }
    /// ```
    pub fn table_names(&self) -> Vec<String> {
        self.base_table_data.tables()
    }

    /// Returns the column names of the given table name.
    ///
    /// ```rust
    /// # use oxidized_mdf::MdfDatabase;
    /// # #[async_std::main]
    /// # async fn main() {
    /// let db = MdfDatabase::open("data/AWLT2005.mdf").await.unwrap();
    ///
    /// let column_names = db.column_names("Address").unwrap();
    /// assert!(column_names.contains(&String::from("City")));
    /// # }
    /// ```
    pub fn column_names(&self, table_name: &str) -> Option<Vec<String>> {
        Some(
            self.base_table_data
                .table(table_name)?
                .columns
                .into_iter()
                .map(|c| c.name.to_string())
                .collect(),
        )
    }

    /// Returns a stream of the rows in the given table.
    ///
    /// ```rust
    /// use oxidized_mdf::{MdfDatabase, Value};
    /// use async_std::stream::StreamExt;
    ///
    /// # #[async_std::main]
    /// # async fn main() {
    /// let mut db = MdfDatabase::open("data/AWLT2005.mdf").await.unwrap();
    /// let mut rows = db.rows("Address").unwrap();
    /// let first_row = rows.next().await.unwrap().unwrap();
    ///
    /// assert_eq!(
    ///     first_row.value("AddressLine1").cloned(),
    ///     Some(Value::String(String::from("8713 Yosemite Ct.BothellWashingtonUnited S")))
    /// );
    /// # }
    /// ```
    pub fn rows<'a, 'b: 'a>(
        &'b mut self,
        table_name: &str,
    ) -> Option<impl Stream<Item = std::result::Result<Row, &'static str>> + 'a> {
        let table = self.base_table_data.table(table_name)?;

        let page_pointers = table.page_pointers();
        Some(
            self.page_reader
                .read_pages(page_pointers)
                .flat_map(move |page| {
                    let mut rows = Vec::new();

                    for record in page.unwrap().records().into_iter() {
                        let mut columns = BTreeMap::new();

                        let mut record = Some(record);
                        for column in &table.columns {
                            let (value, r) = match Value::parse(column, record.take().unwrap()) {
                                Ok((value, r)) => (value, r),
                                Err(_e) => {
                                    break;
                                }
                            };

                            columns.insert(column.name.to_string(), value);

                            record = Some(r);
                        }

                        rows.push(Ok(Row { columns }));
                    }

                    futures_lite::stream::iter(rows.into_iter())
                }),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Value {
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    String(String),
}

impl Value {
    fn parse<'a>(
        column: &Column<'_>,
        record: Record<'a>,
    ) -> std::result::Result<(Self, Record<'a>), &'static str> {
        match column.r#type {
            "tinyint" => {
                let (int, r) = record.parse_i8()?;
                Ok((Value::TinyInt(int), r))
            }
            "smallint" => {
                let (int, r) = record.parse_i16()?;
                Ok((Value::SmallInt(int), r))
            }
            "int" => {
                let (int, r) = record.parse_i32()?;
                Ok((Value::Int(int), r))
            }
            "nvarchar" => {
                let (string, r) = record.parse_string()?;
                Ok((Value::String(string), r))
            }
            _ => {
                eprintln!(
                    "---> {} not yet supported for column {} (record: {:?})",
                    &column.r#type, &column.name, &record
                );
                Err("Unknown column type")
            }
        }
    }
}

#[derive(Debug)]
pub struct Row {
    columns: BTreeMap<String, Value>,
}

impl Row {
    pub fn value(&self, column_name: &str) -> Option<&Value> {
        self.columns.get(column_name)
    }

    pub fn values(self) -> Vec<(String, Value)> {
        self.columns.into_iter().collect()
    }
}

struct PageReader {
    read: Box<dyn Read + Unpin>,
    page_index: u16,
    page_cache: HashMap<PagePointer, Rc<Page>>,
}

impl PageReader {
    fn new(read: Box<dyn Read + Unpin>) -> Self {
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

        assert!(self.page_index <= page_pointer.page_id, "Currently the database supports only forward reading and the requested page {} has been already read", page_pointer.page_id);

        for i in self.page_index..=page_pointer.page_id {
            let mut buffer = [0u8; 8192];
            self.read_next_page(&mut buffer).await?;

            let page = Page::try_from(buffer).unwrap();

            self.page_cache
                .insert(page_pointer.with_page_id(i), Rc::new(page));
        }

        let page = self.page_cache.get(page_pointer).unwrap();
        Ok(page.clone())
    }

    fn read_pages<'a, 'b: 'a>(&'b mut self, page_pointers: Vec<PagePointer>) -> PageStream<'a> {
        PageStream {
            page_pointers: page_pointers.into_iter(),
            page_reader: self,
        }
    }
}

struct PageStream<'a> {
    page_pointers: std::vec::IntoIter<PagePointer>,
    page_reader: &'a mut PageReader,
}

impl<'a> Stream for PageStream<'a> {
    type Item = Result<Rc<Page>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        mut _ctx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        match self.page_pointers.next() {
            Some(page_pointer) => {
                let f = self.page_reader.read_page(&page_pointer);
                futures_lite::pin!(f);
                Poll::Ready(Some(futures_lite::future::block_on(f)))
            }
            None => Poll::Ready(None),
        }
    }
}
