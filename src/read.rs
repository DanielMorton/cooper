use crate::file_meta::FileMeta;
use polars::frame::DataFrame;
use polars::prelude::{LazyCsvReader, LazyFileListReader, NamedFrom};
use polars::series::Series;
use std::path::PathBuf;

pub fn load_file(pb: &PathBuf) -> DataFrame {
    match LazyCsvReader::new(pb)
        .has_header(true)
        .with_separator(u8::try_from('\t').unwrap())
        .finish()
        .map(|f| f.collect())
    {
        Ok(r) => match r {
            Ok(df) => df,
            Err(e) => panic!("Failed to load {:?}:\n {}", pb, e),
        },
        Err(e) => panic!("Failed to load {:?}:\n {}", pb, e),
    }
}

pub(super) fn read_df(pb: &PathBuf) -> DataFrame {
    let mut df = load_file(pb);
    let file_meta = FileMeta::new(pb);
    let size = df.height();
    df.with_column(Series::new("Date", vec![file_meta.get_date(); size]))
        .unwrap();
    df.with_column(Series::new("Time", vec![file_meta.get_time(); size]))
        .unwrap();
    df.with_column(Series::new(
        "Time of Day",
        vec![file_meta.get_time_of_day(); size],
    ))
    .unwrap();
    df.with_column(Series::new("Channel", vec![file_meta.channel; size]))
        .unwrap();
    match df.select([
        "Date",
        "Time",
        "Channel",
        "Begin Time (s)",
        "End Time (s)",
        "Low Freq (Hz)",
        "High Freq (Hz)",
        "Species Code",
        "Common Name",
        "Confidence",
    ]) {
        Ok(df) => df,
        Err(e) => panic!("{}", e),
    }
}
