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
async fn test_read_spg_verein_sample_database_name(file: &str, db_name: &str) -> Result<()> {
    let db = MdfDatabase::open(format!("tests/{}", file)).await?;
    assert_eq!(db.database_name(), db_name);
    Ok(())
}
