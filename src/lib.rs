#![warn(rust_2018_idioms)]

mod pages;

use async_std::fs::File;
use async_std::io::{Result, SeekFrom};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use pages::BootPage;
use std::convert::TryFrom;

pub struct MdfDatabase {
    path: PathBuf,
    boot_page: BootPage,
}

impl MdfDatabase {
    pub async fn open<P>(p: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut path = PathBuf::new();
        path.push(p);

        let mut file = File::open(&path).await?;
        file.seek(SeekFrom::Start(9 * 8192)).await?;
        let mut buffer = [0u8; 8192];
        file.read_exact(&mut buffer).await?;

        Ok(Self {
            path,
            boot_page: BootPage::try_from(buffer).unwrap(),
        })
    }

    pub fn database_name(&self) -> &String {
        &self.boot_page.database_name
    }
}
