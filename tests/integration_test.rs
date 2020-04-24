use async_std::io::Result;
use oxidized_mdf::MdfDatabase;

#[async_std::test]
async fn test_read_spg_verein_sample_database_name() -> Result<()> {
    let db = MdfDatabase::open("tests/spg_verein_TST.mdf").await?;
    assert_eq!(db.database_name(), "spg_verein_TST");
    Ok(())
}
