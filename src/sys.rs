use crate::pages::{BootPage, PagePointer, Record};
use crate::PageReader;
use std::convert::TryFrom;

pub(crate) struct BaseTableData {
    sysalloc_units: Vec<SysallocUnit>,
    sysrow_sets: Vec<SysrowSet>,
    sysschobjs: Vec<Sysschobj>,
}

const SYSROWEST_AUID: i64 = 327680;
const SYSSCHOBJS_IDMAJOR: i32 = 34;

impl BaseTableData {
    pub(crate) async fn parse(
        page_reader: &mut PageReader,
        boot_page: &BootPage,
    ) -> async_std::io::Result<Self> {
        let page = page_reader.read_page(&boot_page.first_sys_indexes).await?;

        let sysalloc_units = page
            .records()
            .into_iter()
            .map(SysallocUnit::try_from)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        let sysrowset_page_pointer = sysalloc_units
            .iter()
            .find(|unit| unit.auid == SYSROWEST_AUID)
            .and_then(|unit| PagePointer::try_from(&unit.pgfirst[..]).ok())
            .unwrap();

        let page = page_reader.read_page(&sysrowset_page_pointer).await?;
        let sysrow_sets = page
            .records()
            .into_iter()
            .map(SysrowSet::try_from)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        let rowset_id = sysrow_sets
            .iter()
            .find(|row| row.idmajor == SYSSCHOBJS_IDMAJOR && row.idminor == 1)
            .map(|row| row.rowsetid)
            .unwrap();

        let sysschobj_page_pointer = sysalloc_units
            .iter()
            .find(|unit| unit.auid == rowset_id && unit.r#type == 1)
            .and_then(|unit| PagePointer::try_from(&unit.pgfirst[..]).ok())
            .unwrap();

        let page = page_reader.read_page(&sysschobj_page_pointer).await?;
        let sysschobjs = page
            .records()
            .into_iter()
            .map(Sysschobj::try_from)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        Ok(Self {
            sysalloc_units,
            sysrow_sets,
            sysschobjs,
        })
    }

    fn objects_dollar(&self) -> impl Iterator<Item = &Sysschobj> {
        self.sysschobjs
            .iter()
            .filter(|o| o.nsclass == 0 && o.pclass == 1)
    }

    pub(crate) fn tables(&self) -> Vec<String> {
        self.objects_dollar().map(|o| o.name.clone()).collect()
    }
}

#[derive(Debug)]
pub(crate) struct SysallocUnit {
    auid: i64,
    r#type: i8,
    ownerid: i64,
    status: i32,
    fgid: i16,
    pgfirst: Vec<u8>,
    pgroot: Vec<u8>,
    pgfirstiam: Vec<u8>,
    pcused: i64,
    pcdata: i64,
    pcreserved: i64,
    // TODO dbfragid: i32,
}

impl<'a> TryFrom<Record<'a>> for SysallocUnit {
    type Error = &'static str;

    fn try_from(record: Record<'a>) -> Result<Self, Self::Error> {
        let (auid, record) = record.parse_i64()?;
        let (r#type, record) = record.parse_i8()?;
        let (ownerid, record) = record.parse_i64()?;
        let (status, record) = record.parse_i32()?;
        let (fgid, record) = record.parse_i16()?;

        let (pgfirst, record) = record.parse_bytes(6)?;
        let (pgroot, record) = record.parse_bytes(6)?;
        let (pgfirstiam, record) = record.parse_bytes(6)?;

        let (pcused, record) = record.parse_i64()?;
        let (pcdata, record) = record.parse_i64()?;
        let (pcreserved, _record) = record.parse_i64()?;
        // TODO let (dbfragid, _record) = record.parse_i32()?;

        Ok(Self {
            auid,
            r#type,
            ownerid,
            status,
            fgid,
            pgfirst: pgfirst.to_vec(),
            pgroot: pgroot.to_vec(),
            pgfirstiam: pgfirstiam.to_vec(),
            pcused,
            pcdata,
            pcreserved,
            // TODO dbfragid,
        })
    }
}

#[derive(Debug)]
pub(crate) struct SysrowSet {
    rowsetid: i64,
    ownertype: i8,
    idmajor: i32,
    idminor: i32,
    numpart: i32,
    status: i32,
    fgidfs: i16,
    rcrows: i64,
    // TODO cmprlevel: i8,
    // TODO fillfact: i8,
    // TODO maxnullbit: i16,
    // TODO maxleaf: i32,
    // TODO maxint: i16,
    // TODO minleaf: i16,
    // TODO minint: i16,
    // TODO rsguid: varbinary,
    // TODO lockres: varbinary,
    // TODO dbfragid: i32
}

impl<'a> TryFrom<Record<'a>> for SysrowSet {
    type Error = &'static str;

    fn try_from(record: Record<'a>) -> Result<Self, Self::Error> {
        let (rowsetid, record) = record.parse_i64()?;
        let (ownertype, record) = record.parse_i8()?;
        let (idmajor, record) = record.parse_i32()?;
        let (idminor, record) = record.parse_i32()?;
        let (numpart, record) = record.parse_i32()?;
        let (status, record) = record.parse_i32()?;
        let (fgidfs, record) = record.parse_i16()?;
        let (rcrows, _record) = record.parse_i64()?;
        // TODO let (cmprlevel, record) = record.parse_i8()?;
        // TODO let (fillfact, _record) = record.parse_i8()?;

        // TODO let maxnullbit = bytes.read_i16::<LittleEndian>().unwrap();
        // TODO let maxleaf = bytes.read_i32::<LittleEndian>().unwrap();
        // TODO let maxint = bytes.read_i16::<LittleEndian>().unwrap();
        // TODO let minleaf = bytes.read_i16::<LittleEndian>().unwrap();
        // TODO let minint = bytes.read_i16::<LittleEndian>().unwrap();

        Ok(Self {
            rowsetid,
            ownertype,
            idmajor,
            idminor,
            numpart,
            status,
            fgidfs,
            rcrows,
            // TODO cmprlevel,
            // TODO fillfact,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Sysschobj {
    id: i32,
    name: String,
    nsid: i32,
    nsclass: i8,
    status: i32,
    r#type: String,
    pid: i32,
    pclass: i8,
    // intprop: i32,
    // TODO created: datetime,
    // TODO modified: datetime,
}

impl<'a> TryFrom<Record<'a>> for Sysschobj {
    type Error = &'static str;

    fn try_from(record: Record<'a>) -> Result<Self, Self::Error> {
        let (id, record) = record.parse_i32()?;
        let (name, record) = record.parse_string()?;
        let (nsid, record) = record.parse_i32()?;
        let (nsclass, record) = record.parse_i8()?;
        let (status, record) = record.parse_i32()?;

        let (r#type, record) = record.parse_bytes(2)?;
        let r#type = String::from_utf8(r#type.to_vec()).unwrap();

        let (pid, record) = record.parse_i32()?;
        let (pclass, _record) = record.parse_i8()?;

        Ok(Self {
            id,
            name,
            nsid,
            nsclass,
            status,
            r#type,
            pid,
            pclass,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::MdfDatabase;
    use async_std::io::Result;
    use pretty_assertions::assert_eq;

    #[async_std::test]
    async fn test_read_boot_page_records() -> Result<()> {
        let db = MdfDatabase::open("data/AWLT2005.mdf").await?;
        let auids = db
            .base_table_data
            .sysalloc_units
            .iter()
            .map(|su| su.auid)
            .collect::<Vec<_>>();

        assert_eq!(
            auids,
            vec![
                262144,
                327680,
                458752,
                524288,
                851968,
                983040, // TODO check if the record is also shown by an MDF viewer
                281474978349056,
                281474978414592,
                281474978480128,
                281474978611200,
                281474978938880,
                281474979397632,
                281474979594240,
                281474979987456,
                281474980052992,
                281474980249600,
                281474980315136,
                281474980642816,
                281474980904960,
                281474980970496,
                281474981560320,
                281474981625856,
                281474981691392,
                562949956763648,
                562949958270976,
                562949958336512,
                844424931901440,
                1125899909070848,
                71776119065149440,
                72057594037993472,
                72057594038059008,
                72057594038190080,
                72057594038255616,
                72057594038386688,
                72057594038452224,
                72057594038583296,
                72057594038648832,
                72057594038779904,
                72057594038845440,
                72057594038976512,
                72057594039107584,
                72057594039173120,
                72057594039304192,
                72057594039435264,
                72057594039566336,
                72057594039697408,
                72057594039762944,
                72057594039959552,
                72057594040025088,
                72057594040156160,
                72057594040287232,
                72057594040418304,
                72057594040483840,
                72057594040614912,
                72057594040680448,
                72057594040811520,
                72057594040877056,
                72057594041008128,
                72057594041073664,
                72057594041204736,
                72057594041335808,
                72057594041466880,
                72057594041532416,
                72057594041597952,
                72057594041663488,
                72057594041729024,
                72057594041794560,
                72057594041860096,
                72057594041925632,
                72057594041991168,
                72057594042056704,
                72057594042122240,
                72057594042187776,
                72057594042253312,
                72057594042318848,
                72057594042384384,
                72057594042449920,
                72057594042515456,
                72057594042580992,
                72057594042646528,
                72057594042712064,
                72057594042777600,
                72057594042843136,
                72057594042908672,
                72057594042974208,
                72057594043039744,
                72057594043105280,
                72057594043170816,
                72057594043236352,
                72057594043432960,
                72057594043498496,
                72057594043564032,
                72057594043957248,
                /* TODO: there are more values in the sysalloc page…
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                720575940,
                */
            ]
        );
        Ok(())
    }
}