use polars::frame::DataFrame;
use polars::prelude::{LazyCsvReader, LazyFileListReader, NamedFrom};
use polars::series::Series;
use std::path::PathBuf;

pub fn agg_df(df: DataFrame) -> DataFrame {
    match df.group_by(["Species Code", "Common Name"]) {
        Ok(g) => match g.select(["Confidence"]).count() {
            Ok(agg) => agg,
            Err(e) => panic!("{}", e),
        },
        Err(e) => panic!("{}", e),
    }
}
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

pub fn parse_file_name(pb: &PathBuf) -> (String, &str, u32) {
    let file_name = match pb.file_name() {
        Some(f) => match f.to_str() {
            Some(fs) => fs,
            None => panic!("Unable to convert {:?} to string", f),
        },
        None => panic!("Missing file name for {:?}", pb),
    };
    let mut file_split = file_name.split('.').next().unwrap().split('_');
    let date = match file_split.next() {
        Some(d) => {
            match d.parse::<u32>() {
                Ok(_) => format!("{}-{}-{}", &d[0..4], &d[4..6], &d[6..8]),
                Err(_) => {
                    match file_split.next() {
                        Some(d) => format!("{}-{}-{}", &d[0..4], &d[4..6], &d[6..8]),
                        None => panic!("No date in file name {}.", &file_name)
                    }
                }
            }
        }
        None => panic!("No date in file name."),
    };
    let time = match file_split.next() {
        Some(t) => match t[..2].parse::<u8>() {
            Ok(tint) => {
                if tint < 12 {
                    "AM"
                } else {
                    "PM"
                }
            }
            Err(e) => panic!("{}", e),
        },
        None => panic!("No time in file name {}.", &file_name),
    };
    let channel = match file_split.next_back() {
        Some(c) => match c.parse::<u32>() {
            Ok(cint) => cint,
            Err(e) => panic!("{}", e),
        },
        None => 1
    };
    (date, time, channel)
}

pub(super) fn make_output(pb: &PathBuf) -> DataFrame {
    let mut df = load_file(pb);
    let mut agg = agg_df(df);
    agg.rename("Confidence_count", "ID Count").unwrap();
    let size = agg.height();
    let (date, time, channel) = parse_file_name(pb);
    df.with_column(Series::new("Date", vec![date; size]))
        .unwrap();
    agg.with_column(Series::new("Time", vec![time; size]))
        .unwrap();
    agg.with_column(Series::new("Channel", vec![channel; size]))
        .unwrap();
    match agg.select([
        "Date",
        "Time",
        "Channel",
        "Species Code",
        "Common Name",
        "ID Count",
    ]) {
        Ok(df) => df,
        Err(e) => panic!("{}", e),
    }
}
