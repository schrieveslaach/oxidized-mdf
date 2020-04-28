#![warn(rust_2018_idioms)]

mod pages;

use async_std::fs::File;
use async_std::io::{Read, Result};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
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
}
