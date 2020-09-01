#[derive(Debug)]
pub(crate) enum ColumnType {
    BigInt,
    Binary(u16),
    Bit,
    Char,
    DateTime,
    Decimal,
    Image,
    Int,
    Money,
    NChar,
    NText,
    NVarchar,
    RID,
    SmallDatetime,
    SmallInt,
    SmallMoney,
    Text,
    TinyInt,
    UniqueIdentifier,
    Uniquifier,
    VarBinary,
    Varchar,
    Variant,
}

impl ColumnType {
    pub(crate) fn fixed_length(&self) -> Option<u16> {
        match &self {
            ColumnType::BigInt => Some(8u16),
            ColumnType::Binary(len) => Some(*len),
            ColumnType::Int => Some(4u16),
            ColumnType::SmallInt => Some(2u16),
            ColumnType::TinyInt => Some(1u16),
            _ => todo!("unknown column type"),
        }
    }
}

pub(crate) struct DataColumn {
    pub(crate) column_type: ColumnType,
    pub(crate) name: String,
}

pub(crate) fn sysallocunit_schema() -> Vec<DataColumn> {
    vec![
        DataColumn {
            name: String::from("auid"),
            column_type: ColumnType::BigInt,
        },
        DataColumn {
            name: String::from("type"),
            column_type: ColumnType::TinyInt,
        },
        DataColumn {
            name: String::from("ownerid"),
            column_type: ColumnType::BigInt,
        },
        DataColumn {
            name: String::from("status"),
            column_type: ColumnType::Int,
        },
        DataColumn {
            name: String::from("fgid"),
            column_type: ColumnType::SmallInt,
        },
        DataColumn {
            name: String::from("pgfirst"),
            column_type: ColumnType::Binary(6),
        },
        DataColumn {
            name: String::from("pgroot"),
            column_type: ColumnType::Binary(6),
        },
        DataColumn {
            name: String::from("pgfirstiam"),
            column_type: ColumnType::Binary(6),
        },
        DataColumn {
            name: String::from("pcused"),
            column_type: ColumnType::BigInt,
        },
        DataColumn {
            name: String::from("pcdata"),
            column_type: ColumnType::BigInt,
        },
        DataColumn {
            name: String::from("pcreserved"),
            column_type: ColumnType::BigInt,
        },
        DataColumn {
            name: String::from("dbfragid"),
            column_type: ColumnType::Int,
        },
    ]
}
