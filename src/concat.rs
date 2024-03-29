use polars::frame::DataFrame;
use polars::functions::concat_df_diagonal;
use polars::prelude::UniqueKeepStrategy;

pub(super) fn concat(df_list: &[DataFrame]) -> DataFrame {
    match concat_df_diagonal(df_list)
        .and_then(|df| df.unique(None, UniqueKeepStrategy::First, None))
        .and_then(|df| {
            df.sort(
                ["Date", "Time", "Channel", "Common Name"],
                vec![false; 4],
                false,
            )
        }) {
        Ok(d) => d,
        Err(e) => panic!("{:?}", e),
    }
}
