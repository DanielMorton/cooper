use polars::prelude::{DataFrame, NamedFrom, Series};
use std::path::PathBuf;
use time::{format_description, Date};

fn make_date_vec(min_date: Date, max_date: &Date, vec: &mut Vec<String>) {
    if min_date.le(max_date) {
        let next_date = match min_date.next_day() {
            Some(d) => d,
            None => panic!("Next date out of range."),
        };
        for _ in 0..4 {
            vec.push(min_date.to_string());
        }
        if next_date.le(max_date) {
            make_date_vec(next_date, max_date, vec)
        }
    }
}
pub(super) fn make_date_range(input_files: &[PathBuf]) -> DataFrame {
    let input_dates = input_files
        .iter()
        .filter_map(|f| {
            f.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| {
                    let mut split = n.split('_');
                    split.next().map(|s| {
                        match s.parse::<u32>() {
                            Ok(_) => s,
                            Err(_) => split.next().unwrap()
                        }
                    })
                })
        })
        .collect::<Vec<_>>();
    let format = match format_description::parse("[year][month][day]") {
        Ok(f) => f,
        Err(e) => panic!("{}", e),
    };
    let min_date = match input_dates.iter().min() {
        Some(&m) => match Date::parse(m, &format) {
            Ok(d) => d,
            Err(e) => panic!("{}", e),
        },
        None => panic!("Empty Date List"),
    };
    let max_date = match input_dates.iter().max() {
        Some(&m) => match Date::parse(m, &format) {
            Ok(d) => d,
            Err(e) => panic!("{}", e),
        },
        None => panic!("Empty Date List"),
    };
    let mut date_vec = Vec::new();
    make_date_vec(min_date, &max_date, &mut date_vec);
    let date = Series::new("Date", date_vec);
    let mut time_vec = Vec::with_capacity(date.len());
    let mut channel_vec = Vec::with_capacity(date.len());
    for _ in 0..date.len() / 4 {
        time_vec.push("AM");
        channel_vec.push(1u32);
        time_vec.push("AM");
        channel_vec.push(2u32);
        time_vec.push("PM");
        channel_vec.push(1u32);
        time_vec.push("PM");
        channel_vec.push(2u32);
    }
    let time = Series::new("Time", time_vec);
    let channel = Series::new("Channel", channel_vec);
    match DataFrame::new(vec![date, time, channel]) {
        Ok(d) => d,
        Err(e) => panic!("{}", e),
    }
}
