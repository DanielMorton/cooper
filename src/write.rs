use polars::prelude::{CsvWriter, DataFrame, SerWriter};
use std::fs::File;

pub(super) fn write_csv(df: &mut DataFrame, file_name: &str) {
    let file = match File::create(file_name) {
        Ok(f) => f,
        Err(e) => panic!("{}", e),
    };
    match CsvWriter::new(&file).has_header(true).finish(df) {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }
}
