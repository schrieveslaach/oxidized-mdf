use async_std::io::Result;
use oxidized_mdf::MdfDatabase;
use rstest::rstest;

#[rstest(
    file,
    db_name,
    case("spg_verein_TST.mdf", "spg_verein_TST"),
    case("AWLT2005.mdf", "AdventureWorksLT")
)]
#[async_std::test]
async fn test_read_database_name(file: &str, db_name: &str) -> Result<()> {
    let db = MdfDatabase::open(format!("tests/{}", file)).await?;
    assert_eq!(db.database_name(), db_name);
    Ok(())
}

#[async_std::test]
async fn test_read_boot_page_records() -> Result<()> {
    let db = MdfDatabase::open("tests/AWLT2005.mdf").await?;
    let sysalloc_units = db.boot_page().sysalloc_units();
    println!("{:?}", sysalloc_units);
    Ok(())
}
