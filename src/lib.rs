#![warn(rust_2018_idioms)]

mod pages;
mod schema;

use crate::schema::ColumnType;
use async_std::fs::File;
use async_std::io::{Read, Result};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use pages::BootPage;
use std::convert::TryFrom;
use std::pin::Pin;

pub struct MdfDatabase {
    read: Pin<Box<dyn Read>>,
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
        let mut read = read;

        // Skipping first headers
        for _i in 0..9 {
            read.read_exact(&mut buffer).await?;
        }
        read.read_exact(&mut buffer).await?;

        Ok(Self {
            read,
            boot_page: BootPage::try_from(buffer).unwrap(),
        })
    }

    pub fn database_name(&self) -> &String {
        &self.boot_page.database_name
    }

    pub fn boot_page(&self) -> &BootPage {
        &self.boot_page
    }
}

#[derive(Debug)]
pub enum Value {
    BigInt(num_bigint::BigInt),
    Binary(Vec<u8>),
    Int(i32),
    SmallInt(i16),
    TinyInt(i8),
}

impl Value {
    pub(crate) fn parse<'a, 'b>(bytes: &'a [u8], column_type: &'b ColumnType) -> (Self, &'a [u8]) {
        match column_type.fixed_length() {
            Some(length) => {
                let mut bytes_to_parse = &bytes[0..(length as usize)];

                println!("parsing column type {:?}", column_type);
                let v = match column_type {
                    ColumnType::BigInt => {
                        Value::BigInt(num_bigint::BigInt::from_signed_bytes_be(bytes_to_parse))
                    }
                    ColumnType::Binary(_) => Value::Binary(bytes_to_parse.to_vec()),
                    ColumnType::Int => {
                        Value::Int(bytes_to_parse.read_i32::<LittleEndian>().unwrap())
                    }
                    ColumnType::SmallInt => {
                        Value::SmallInt(bytes_to_parse.read_i16::<LittleEndian>().unwrap())
                    }
                    ColumnType::TinyInt => Value::TinyInt(bytes_to_parse.read_i8().unwrap()),
                    _ => todo!("parsing with variable length not yet implemented"),
                };

                let rest = &bytes[(length as usize)..];

                (v, rest)
            }
            None => todo!(),
        }
    }
}
