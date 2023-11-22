use itertools::Itertools;
use polars::prelude::{DataFrame, NamedFrom, Series};
use std::path::PathBuf;
use time::{format_description, Date};

static CHANNELS: &[u8] = &[1, 2];
static DATE_FORMAT: &str = "[year][month][day]";
static TIMES: &[&str] = &["AM", "PM"];

fn make_date_vec(min_date: Date, max_date: &Date, vec: &mut Vec<String>) {
    if min_date.le(max_date) {
        let next_date = match min_date.next_day() {
            Some(d) => d,
            None => panic!("Next date out of range."),
        };
        vec.push(min_date.to_string());
        if next_date.le(max_date) {
            make_date_vec(next_date, max_date, vec)
        }
    }
}
pub(super) fn make_date_range(input_files: &[PathBuf]) -> DataFrame {
    let input_dates = input_files
        .iter()
        .filter_map(|f| {
            f.file_name().and_then(|n| n.to_str()).and_then(|n| {
                let mut split = n.split('_');
                split.next().map(|s| match s.parse::<u32>() {
                    Ok(_) => s,
                    Err(_) => split.next().unwrap(),
                })
            })
        })
        .collect::<Vec<_>>();
    let format = match format_description::parse(DATE_FORMAT) {
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
    let mut dates = Vec::new();
    let date_len = 4 * dates.len();
    make_date_vec(min_date, &max_date, &mut dates);
    let mut date_vec = Vec::with_capacity(date_len);
    let mut time_vec = Vec::with_capacity(date_len);
    let mut channel_vec = Vec::with_capacity(date_len);
    dates
        .iter()
        .cartesian_product(TIMES)
        .cartesian_product(CHANNELS)
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|((d, &t), &c)| {
            date_vec.push(d.to_owned());
            time_vec.push(t);
            channel_vec.push(c);
        });
    let date = Series::new("Date", date_vec);
    let time = Series::new("Time", time_vec);
    let channel = Series::new("Channel", channel_vec);
    match DataFrame::new(vec![date, time, channel]) {
        Ok(d) => d,
        Err(e) => panic!("{}", e),
    }
}
