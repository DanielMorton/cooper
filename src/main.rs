use crate::agg::agg_df;
use crate::concat::concat;
use crate::date::make_date_range;
use crate::location::{add_location, join_location, load_location};
use crate::parse::CooperParse;
use crate::pivot::species_pivot;
use crate::read::{filter_df, read_df};
use crate::species::load_species;
use crate::write::{write_by_year, write_csv};
use polars::export::ahash::{HashSet, HashSetExt};
use polars::prelude::{DataFrameJoinOps, JoinArgs, JoinType};
use std::time::Instant;

mod agg;
mod concat;
mod date;
mod file_meta;
mod location;
mod parse;
mod pivot;
mod read;
mod species;
mod write;

fn print_hms(start: &Instant) {
    let millis = start.elapsed().as_millis();
    let seconds = millis / 1000;
    let (hour, minute, second) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    println!("{:02}:{:02}:{:02}.{}", hour, minute, second, millis % 1000)
}

fn main() {
    let matches = parse::parse();
    let by_year = matches.get_by_year();
    let input_dir = matches.get_input_dir();
    let agg_output = matches.get_output_agg_file(input_dir);
    let raw_output = matches.get_output_raw_file(input_dir);
    let pivot_output = matches.get_output_pivot_file(input_dir);
    let input_files = matches.get_input_files(input_dir);
    let min_count = matches.get_min_count();
    let raw_filter = matches.get_raw_filter();
    let location_file = matches.get_location();
    let location_code = matches.get_fixed_location();
    let date_range = make_date_range(&input_files);

    let s = Instant::now();
    let mut years = HashSet::new();
    let raw_list = input_files
        .iter()
        .map(|pb| read_df(pb, &mut years))
        .filter(|df| df.height() > 0)
        .map(|df| filter_df(df, raw_filter))
        .collect::<Vec<_>>();

    let species = load_species();
    let location = load_location(&location_file);

    let mut raw = concat(&raw_list);
    raw = join_location(raw, &location);
    raw = add_location(raw, location_code);
    raw = match raw.join(
        &species,
        ["Common Name"],
        ["Common Name"],
        JoinArgs::new(JoinType::Left),
    ) {
        Ok(df) => df,
        Err(e) => panic!("{:?}", e),
    };
    if by_year {
        write_by_year(&raw, &raw_output, &years)
    } else {
        write_csv(&mut raw, &raw_output)
    };

    let mut agg = agg_df(&raw);
    agg = join_location(agg, &location);
    agg = add_location(agg, location_code);
    write_csv(&mut agg, &agg_output);

    let mut pivot_df = species_pivot(&agg, &date_range, min_count);
    write_csv(&mut pivot_df, &pivot_output);
    println!("Run time:");
    print_hms(&s);
}
