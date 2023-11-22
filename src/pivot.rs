use polars::prelude::pivot::pivot_stable;
use polars::prelude::{ChunkCompare, DataFrame, DataFrameJoinOps};

pub(super) fn species_pivot(
    agg: &DataFrame,
    date_range: &DataFrame,
    min_count: Option<u8>,
) -> DataFrame {
    let filtered_df = match min_count {
        Some(m) => {
            let col = match agg.column("ID Count") {
                Ok(c) => c,
                Err(e) => panic!("{}", e),
            };
            let mask = match col.gt_eq(m) {
                Ok(ma) => ma,
                Err(e) => panic!("{}", e),
            };
            match agg.filter(&mask) {
                Ok(fdf) => fdf,
                Err(e) => panic!("{}", e),
            }
        }
        None => agg.to_owned(),
    };
    match pivot_stable(
        &filtered_df,
        ["ID Count"],
        ["Date", "Time", "Channel"],
        ["Common Name"],
        true,
        None,
        None,
    )
    .and_then(|pivot| {
        date_range.left_join(
            &pivot,
            ["Date", "Time", "Channel"],
            ["Date", "Time", "Channel"],
        )
    })
    .and_then(|df| df.sort(["Date", "Time", "Channel"], vec![false; 3], true))
    {
        Ok(p) => p,
        Err(e) => panic!("{}", e),
    }
}
