use polars::export::ahash::HashSet;
use polars::prelude::{ChunkCompare, CsvWriter, DataFrame, SerWriter};
use std::fs::File;

pub(super) fn write_csv(df: &mut DataFrame, file_name: &str) {
    let file = match File::create(file_name) {
        Ok(f) => f,
        Err(e) => panic!("{:?}", e)
    };
    match CsvWriter::new(&file).has_header(true).finish(df) {
        Ok(_) => (),
        Err(e) => panic!("{:?}", e)
    }
}

pub(super) fn write_by_year(df: &DataFrame, file_name: &str, years: &HashSet<i32>) {
    years.iter().for_each(|&y| {
        let file_split = file_name.split('/').map(String::from).collect::<Vec<_>>();
        let year_file = format!("{}/{}_{}", file_split[0], y, file_split[1]);
        let mut write_df = match df.column("Year")
            .and_then(|s| s.equal(y))
            .and_then(|mask| df.filter(&mask)) {
            Ok(fdf) => fdf,
            Err(e) => panic!("{:?}", e)
        };
        write_csv(&mut write_df, &year_file)
    })
}
