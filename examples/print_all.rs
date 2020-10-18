use futures_lite::stream::StreamExt;
use oxidized_mdf::MdfDatabase;
use prettytable::{Cell, Row, Table};
use std::path::PathBuf;
use structopt::StructOpt;

async fn print_rows(db: &mut MdfDatabase, table: &str) {
    let mut rows = match db.rows(&table) {
        Some(rows) => rows,
        None => {
            eprintln!("No table {}", table);
            return;
        }
    };

    let mut pretty_table = Table::new();

    while let Some(row) = rows.next().await {
        let row = row.unwrap();
        let values = row.values();

        if pretty_table.is_empty() {
            let cells = values.iter().map(|(k, _)| Cell::new(k)).collect::<Vec<_>>();
            pretty_table.add_row(Row::new(cells));
        }

        let cells = values
            .into_iter()
            .map(|(_, v)| Cell::new(&format!("{}", v)))
            .collect::<Vec<_>>();
        pretty_table.add_row(Row::new(cells));
    }

    println!("--------------------");
    println!("Data of table: {}", table);
    println!("--------------------");
    pretty_table.printstd();
}

#[async_std::main]
async fn main() {
    let opt = Opts::from_args();

    let mut db = MdfDatabase::open(opt.path).await.unwrap();

    match opt.table {
        None => {
            for table in db.table_names() {
                print_rows(&mut db, &table).await;
            }
        }
        Some(table) => {
            print_rows(&mut db, &table).await;
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "print_all",
    about = "An example of how to read data from an MDF file."
)]
struct Opts {
    /// The path to the MDF file.
    #[structopt(parse(from_os_str))]
    path: PathBuf,

    /// Prints only the content of the given table
    #[structopt(long)]
    table: Option<String>,
}
