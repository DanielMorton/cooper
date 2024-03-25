use crate::file_meta::FileMeta;
use polars::export::ahash::HashSet;
use polars::prelude::{ChunkCompare, DataFrame, LazyCsvReader, LazyFileListReader, NamedFrom};
use polars::series::Series;
use std::path::PathBuf;

pub(super) fn load_file(pb: &PathBuf, sep: char) -> DataFrame {
    match LazyCsvReader::new(pb)
        .has_header(true)
        .with_separator(u8::try_from(sep).unwrap())
        .finish()
        .map(|f| f.collect())
    {
        Ok(r) => match r {
            Ok(df) => df,
            Err(e) => panic!("Failed to load {:?}:\n {:?}", pb, e),
        },
        Err(e) => panic!("Failed to load {:?}:\n {:?}", pb, e),
    }
}

fn load_data(pb: &PathBuf) -> DataFrame {
    load_file(pb, '\t')
}

pub(super) fn filter_df(df: DataFrame, raw_filter: Option<f32>) -> DataFrame {
    match raw_filter {
        Some(f) => {
            match df.column("Confidence")
                .and_then(|col| col.gt_eq(f))
                .and_then(|mask| df.filter(&mask)) {
                Ok(c) => c,
                Err(e) => panic!("{:?}", e),
            }
        }
        None => df,
    }
}

pub(super) fn read_df(pb: &PathBuf, years: &mut HashSet<i32>) -> DataFrame {
    let mut df = load_data(pb);
    let file_meta = FileMeta::new(pb);
    let size = df.height();
    let date = file_meta.get_date();
    let year = match date[..4].parse::<i32>() {
        Ok(y) => y,
        Err(e) => panic!("{:?}", e),
    };
    years.insert(year);
    df.with_column(Series::new("Season", vec![file_meta.get_season(); size]))
        .unwrap();
    df.with_column(Series::new("Year", vec![year; size]))
        .unwrap();
    df.with_column(Series::new("Date", vec![date; size]))
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
        "Season",
        "Year",
        "Date",
        "Time",
        "Time of Day",
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
        Err(e) => panic!("{:?}", e),
    }
}
