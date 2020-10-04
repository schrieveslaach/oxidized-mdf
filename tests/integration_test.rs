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
async fn database_name(file: &str, db_name: &str) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;
    assert_eq!(db.database_name(), db_name);
    Ok(())
}

#[rstest(
    file,
    table_names,
    case("spg_verein_TST.mdf", vec!["___Tool_Text", "tblDOSBAbteilungen", "tblDOSBExportBE", "tblDOSBExportJAFachverbandsweise", "tblDOSBExportJAGesamt", "tblDOSBExportJASportartenweise", "tblDOSBExportJB", "tblDOSBSportarten", "tblDOSBVerein", "tblDOSBVorstand", "tblIMPORTAbrechnung", "tblIMPORTAbrechnungArtgArt", "tblIMPORTAbrechnungDetails", "tblIMPORTAbrechnungDetailsZeilen", "tblImportEmailParameter", "tblImportMTAbteilungBeitraege", "tblImportMTAenderungen", "tblImportMTEhrungen", "tblImportMTFunktionen", "tblImportMitglied", "tblImportSTAbteilungBeitraegeaege", "tblImportSTAbteilungenngen<0ᡚ淍\u{1}\u{0}\u{0}\u{0}倀⅋�Ŭ\u{0}\u{0}\u{f4b5}¥Ꞽ\u{0}ꋷ\u{86}Ꞔ\u{0}\u{0}\u{0}", "tblImportSTBerufsgruppen", "tblImportSTBezirke", "tblImportSTEhrungen", "tblImportSTFunktionen", "tblImportSTVerein", "tblImportUmsaetzeetze", "tblImportZahlungseingaenge", "tblImportZahlungseingaengeFibu", "tbl_Abrechnung", "tbl_AbrechnungAbgerechnet", "tbl_AbrechnungArtgArt", "tbl_AbrechnungDatum", "tbl_AbrechnungDetails", "tbl_AbrechnungDetailsZeilen<0乩\u{1dfb}\u{1}\u{0}\u{0}Ȁ䐀䌠틪ŗ\u{1b}\u{0}ᒄĵꝻ\u{0}驄ÂꞺ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}t", "tbl_AbrechnungDetailsZeilenARCHIV", "tbl_AbrechnungDetails_ARCHIV", "tbl_AbrechnungSicherung", "tbl_Abrechnung_ARCHIV", "tbl_Abrechnungen_SEPA_Dateien", "tbl_Abteilung", "tbl_Abteilung_Beitragtrag", "tbl_Aenderungsprotokoll", "tbl_Aenderungsprotokoll_Gruende", "tbl_Ansichten", "tbl_BE_Mitglied_AKTIV_JN00该槜\u{1}\u{0}\u{0}\u{c00}嘀 \u{0}Ā\u{0}\u{0}�\u{9f}ꞌ\u{0}鑼±ꞌ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}°", "tbl_Bankleitzahlen", "tbl_Beitragsart", "tbl_Beitragserhebung_Sortierung", "tbl_Beitragserhebungen", "tbl_Beitragserhebungen_ARCHIV", "tbl_Beitragserhebungen_Details", "tbl_Beitragserhebungen_Details_ARCHIV", "tbl_Beitragserhebungen_Nacharbeiten", "tbl_Beitragserhebungen_SEPA_Dateien", "tbl_Benutzerfelder", "tbl_Berufsgruppe", "tbl_Bezirk", "tbl_Buchungsschluessel", "tbl_DOSBCodierung", "tbl_DOSBFachverbaende", "tbl_DOSBFunktionen", "tbl_DOSBMitglied_Funktion", "tbl_DOSBSportarten", "tbl_DOSBStatistiktypen", "tbl_DOSB_VE", "tbl_DOSB_VO", "tbl_DateiImport", "tbl_DateiImportRohdaten", "tbl_Dateiimport_Fehler", "tbl_Ehrung", "tbl_EmailParameter", "tbl_Exportvorlagen", "tbl_ExportvorlagenFeldnamen", "tbl_Feldnamen<0ܩ泎\u{1}\u{0}\u{0}\u{0}倀\u{f04b}�ū\u{0}\u{0}ᆴĵꝻ\u{0}鬡ÁꞺ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}h", "tbl_Formulare", "tbl_Formularnamen", "tbl_Funktiontion", "tbl_H_Abteilung_Beitrag", "tbl_H_Abteilunglung", "tbl_H_Beitragsart", "tbl_H_Berufsgruppe", "tbl_H_Bezirk", "tbl_H_Ehrung", "tbl_H_Funktion", "tbl_H_Mitglied_Abteilung_Beitrag", "tbl_H_Mitglied_Ehrung", "tbl_H_Mitglied_Funktiontion", "tbl_H_Mitgliedlied", "tbl_H_Umsaetze", "tbl_H_Verein", "tbl_H_Zahlungseingaenge", "tbl_ID", "tbl_ImportProtokoll", "tbl_Importzuordnungen", "tbl_Laendernder", "tbl_Mahnungen", "tbl_Mahnungen_ARCHIV<0\u{ad9}\u{eb9}\u{1}\u{0}\u{0}Ȁ䐀ﰠ등Ş\u{4}\u{0}ᑼĵꝻ\u{0}針ÂꞺ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}r", "tbl_Mahnungen_Details", "tbl_Mahnungen_Details_ARCHIV<0卋Ⴁ\u{1}\u{0}\u{0}Ȁ䐀ᠠ\u{e461}Ś\u{4}\u{0}ᑽĵꝻ\u{0}䯽ÃꞺ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}v", "tbl_MassenaenderungFeldname", "tbl_Mitglied_Abteilung_Beitrag", "tbl_Mitglied_Ehrung", "tbl_Mitglied_Funktion", "tbl_Mitglieder_GES_SEL", "tbl_Mitgliedlied", "tbl_PLZ", "tbl_SEPA_Ausfuehrungen", "tbl_Selektionen", "tbl_SelektionenDetails", "tbl_SelektionenDetailsFilterungSortierung", "tbl_Statistik_Altersgruppen", "tbl_Statistik_Mitgliedschaft", "tbl_Strassenverzeichnis", "tbl_Umsaetze", "tbl_Verein", "tbl_Voreinstellungen", "tbl_Zahlarten", "tbl_Zahlungseingaenge", "tbl_Zahlweise", "tbl__datenversion", "tbl_tempStatistikDaten", "tbl_tempStatistikDatenAusgetreten", "tbl_tempStatistikZahlungsdaten", "tbl_temp_EXPORT", "tbl_temp_LL_Daten", "tbl_temp_LL_Daten_Mitglieder", "tbl_temp_LL_Daten_Rechnung<0蟜ӥ\u{1}\u{0}\u{0}\u{c00}吀ㅒछĂ\u{0}\u{0}鬼ÁꞺ\u{0}鈳\u{86}Ꞽ\u{0}\u{0}\u{0}\u{c}\u{0}\u{1}~", "trace_xe_action_map", "trace_xe_event_map"]),
    case("AWLT2005.mdf", vec!["Address", "BuildVersion", "Customer", "CustomerAddress", "ErrorLog", "Product", "ProductCategory", "ProductDescription", "ProductModel", "ProductModelProductDescription", "SalesOrderDetail", "SalesOrderHeader"])
)]
#[async_std::test]
async fn tables(file: &str, table_names: Vec<&str>) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;

    let mut tables = db.table_names();
    tables.sort();

    assert_eq!(tables, table_names);

    Ok(())
}

#[rstest(
    file,
    table_name,
    column_names,
    // TODO: strange column name for address table. Check encoding
    case("AWLT2005.mdf", "Address", vec!["AddressIDⱶ\u{0}\u{1}\u{0}\u{1}\u{0}�", "AddressLine1", "AddressLine2", "City", "CountryRegion", "ModifiedDate", "PostalCode", "StateProvince", "rowguid"])
)]
#[async_std::test]
async fn columns(file: &str, table_name: &str, column_names: Vec<&str>) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;

    let mut columns = db.column_names(table_name).unwrap();
    columns.sort();

    assert_eq!(columns, column_names);

    Ok(())
}
