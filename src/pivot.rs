use polars::prelude::pivot::pivot_stable;
use polars::prelude::{ChunkCompare, DataFrame, DataFrameJoinOps};

static COLUMNS: &[&str] = &["Common Name"];
static INDEX: &[&str] = &["Date", "Time of Day", "Channel"];
static VALUES: &[&str] = &["ID Count"];
pub(super) fn species_pivot(
    agg: &DataFrame,
    date_range: &DataFrame,
    min_count: Option<u8>,
) -> DataFrame {
    let filtered_df = match min_count {
        Some(m) => {
            let col = match agg.column(VALUES[0]) {
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
    match pivot_stable(&filtered_df, VALUES, INDEX, COLUMNS, true, None, None)
        .and_then(|pivot| date_range.left_join(&pivot, INDEX, INDEX))
        .and_then(|df| df.sort(INDEX, vec![false; 3], true))
    {
        Ok(p) => p,
        Err(e) => panic!("{}", e),
    }
}
