use async_std::io::Result;
use oxidized_mdf::MdfDatabase;
use pretty_assertions::assert_eq;
use rstest::rstest;

#[rstest(
    file,
    db_name,
    case("spg_verein_TST.mdf", "spg_verein_TST"),
    case("AWLT2005.mdf", "AdventureWorksLT")
)]
#[async_std::test]
async fn test_read_database_name(file: &str, db_name: &str) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;
    assert_eq!(db.database_name(), db_name);
    Ok(())
}
