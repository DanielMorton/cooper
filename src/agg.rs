use polars::prelude::DataFrame;

pub fn agg_df(df: &DataFrame) -> DataFrame {
    match df.group_by([
        "Season",
        "Date",
        "Time of Day",
        "Channel",
        "Species Code",
        "Common Name",
        "Order",
        "Family",
        "Scientific Name"
    ]) {
        Ok(g) => match g.select(["Confidence"]).count().and_then(|mut df| {
            df.rename("Confidence_count", "ID Count").unwrap();
            df.sort(
                ["Date", "Time of Day", "Channel", "Common Name"],
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
