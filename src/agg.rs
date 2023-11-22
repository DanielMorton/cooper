use polars::prelude::DataFrame;

pub fn agg_df(df: &DataFrame) -> DataFrame {
    match df.group_by(["Date", "Time", "Channel", "Species Code", "Common Name"]) {
        Ok(g) => match g.select(["Confidence"]).count().and_then(|mut df| {
            df.rename("Confidence_count", "ID Count").unwrap();
            df.sort(
                ["Date", "Time", "Channel", "Common Name"],
                vec![false; 4],
                true,
            )
        }) {
            Ok(agg) => agg,
            Err(e) => panic!("{}", e),
        },
        Err(e) => panic!("{}", e),
    }
}
