use crate::read::load_file;
use polars::prelude::{DataFrame, DataFrameJoinOps, JoinArgs, JoinType};
use std::path::PathBuf;

pub(super) fn load_location(opb: &Option<PathBuf>) -> Option<DataFrame> {
    opb.as_ref().map(|pb| load_file(pb, ','))
}

pub(super) fn join_location(raw: DataFrame, location_df: &Option<DataFrame>) -> DataFrame {
    match location_df {
        Some(ldf) => {
            match raw.join(
                ldf,
                ["Date", "Time of Day"],
                ["Date", "Time of Day"],
                JoinArgs::new(JoinType::Left),
            ) {
                Ok(df) => df,
                Err(e) => panic!("{:?}", e),
            }
        }
        None => raw,
    }
}
