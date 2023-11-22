use crate::date::make_date_range;
use crate::parse::CooperParse;
use crate::pivot::species_pivot;
use crate::read::make_output;
use polars::functions::concat_df_diagonal;
use polars::prelude::{CsvWriter, DataFrameJoinOps, SerWriter, UniqueKeepStrategy};
use std::fs::File;

mod date;
mod parse;
mod pivot;
mod read;

fn main() {
    let matches = parse::parse();
    let input_dir = matches.get_input_dir();
    let input_files = matches.get_input_files(input_dir);
    let output = matches.get_output_file(input_dir);
    let pivot_output = matches.get_output_pivot_file(input_dir);
    let min_count = matches.get_min_count();
    let date_range = make_date_range(&input_files);

    let df_list = input_files
        .iter()
        .map(make_output)
        .collect::<Vec<_>>();

    let mut df = match concat_df_diagonal(&df_list) {
        Ok(d) => d,
        Err(e) => panic!("{}", e),
    }
    .sort(
        ["Date", "Time", "Channel", "Common Name"],
        vec![false; 4],
        true,
    )
    .unwrap().unique_stable(None, UniqueKeepStrategy::First, None).unwrap();

    let file = match File::create(output) {
        Ok(f) => f,
        Err(e) => panic!("{}", e),
    };
    match CsvWriter::new(&file).has_header(true).finish(&mut df) {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }

    let pivot_df = species_pivot(&df, min_count);
    let mut full_pivot_df = match date_range.left_join(
        &pivot_df,
        ["Date", "Time", "Channel"],
        ["Date", "Time", "Channel"],
    ) {
        Ok(fpdf) => fpdf,
        Err(e) => panic!("{}", e),
    };

    let pivot_file = match File::create(pivot_output) {
        Ok(f) => f,
        Err(e) => panic!("{}", e),
    };
    match CsvWriter::new(&pivot_file)
        .has_header(true)
        .finish(&mut full_pivot_df)
    {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }
}
