use crate::read::load_file;
use polars::prelude::{DataFrame, DataFrameJoinOps, JoinArgs, JoinType, NamedFrom, Series};
use std::path::PathBuf;

static SITES: &[&str] = &[
    "SW3", "CT1,2,3", "BL 2,3", "BL2,3", "GS 4+8", "LE9", "LE3+4", "GS 4+8", "RV1", "LE 13+17",
    "OV2", "LG3", "RT9", "LE24", "LE4", "RT9", "EL1",
];

pub(super) fn add_location(mut df: DataFrame, location_code: Option<usize>) -> DataFrame {
    location_code
        .map(|lc| {
            df.with_column(Series::new("Site Number", vec![lc as u32; df.height()]))
                .unwrap();
            df.with_column(Series::new("Site Code", vec![SITES[lc - 1]; df.height()]))
                .unwrap();
            let mut columns = df.get_column_names();
            let num_cols = columns.len();
            columns = [&columns[num_cols-2..], &columns[..num_cols-2]].concat();
            match df.select(columns) {
                Ok(raw) => raw,
                Err(e) => panic!("{:?}", e),
            }
        })
        .unwrap_or(df)
}

pub(super) fn join_location(raw: DataFrame, location_df: &Option<DataFrame>) -> DataFrame {
    location_df
        .as_ref()
        .map(|ldf| {
            match raw.join(
                ldf,
                ["Date", "Time of Day"],
                ["Date", "Time of Day"],
                JoinArgs::new(JoinType::Left),
            ) {
                Ok(df) => df,
                Err(e) => panic!("{:?}", e),
            }
        })
        .map(|mut df| {
            df.rename("Site", "Site Code").unwrap();
            df.rename("Number", "Site Number").unwrap();
            let mut columns = df.get_column_names();
            let num_cols = columns.len();
            columns = [&columns[num_cols-2..], &columns[..num_cols-2]].concat();
            match df.select( columns ) {
                Ok(raw) => raw,
                Err(e) => panic!("{:?}", e),
            }
        })
        .unwrap_or(raw)
}

pub(super) fn load_location(opb: &Option<PathBuf>) -> Option<DataFrame> {
    opb.as_ref().map(|pb| load_file(pb, ','))
}
