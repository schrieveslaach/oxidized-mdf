#[macro_use]
extern crate prettytable;

use futures_lite::stream::StreamExt;
use oxidized_mdf::MdfDatabase;
use prettytable::{Cell, Row, Table};

async fn print_rows(db: &mut MdfDatabase, table: &str) {
    println!("--------------------");
    println!("Data of table: {}", table);
    println!("--------------------");

    let mut pretty_table = Table::new();

    let mut rows = db.rows(&table).unwrap();
    while let Some(row) = rows.next().await {
        let row = row.unwrap();
        let values = row.values();

        if pretty_table.is_empty() {
            let cells = values.iter().map(|(k, _)| Cell::new(k)).collect::<Vec<_>>();
            pretty_table.add_row(Row::new(cells));
        }

        let cells = values
            .into_iter()
            .map(|(_, v)| Cell::new(&format!("{:?}", v)))
            .collect::<Vec<_>>();
        pretty_table.add_row(Row::new(cells));
    }

    pretty_table.printstd();
}

#[async_std::main]
async fn main() {
    let mut db = MdfDatabase::open("data/AWLT2005.mdf").await.unwrap();

    for table in db.table_names() {
        print_rows(&mut db, &table).await;
    }
}
