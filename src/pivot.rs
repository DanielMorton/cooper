use polars::prelude::pivot::pivot;
use polars::prelude::{ChunkCompare, DataFrame};

pub(super) fn species_pivot(df: &DataFrame, min_count: Option<u8>) -> DataFrame {
    let filtered_df = match min_count {
        Some(m) => {
            let col = match df.column("ID Count") {
                Ok(c) => c,
                Err(e) => panic!("{}", e),
            };
            let mask = match col.gt_eq(m) {
                Ok(ma) => ma,
                Err(e) => panic!("{}", e),
            };
            match df.filter(&mask) {
                Ok(fdf) => fdf,
                Err(e) => panic!("{}", e),
            }
        }
        None => df.to_owned(),
    };
    match pivot(
        &filtered_df,
        ["ID Count"],
        ["Date", "Time", "Channel"],
        ["Common Name"],
        true,
        None,
        None,
    ) {
        Ok(p) => p,
        Err(e) => panic!("{}", e),
    }
}
