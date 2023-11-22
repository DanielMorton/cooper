use crate::agg::agg_df;
use crate::date::make_date_range;
use crate::parse::CooperParse;
use crate::pivot::species_pivot;
use crate::read::read_df;
use crate::write::write_csv;
use crate::concat::concat;

mod agg;
mod date;
mod file_meta;
mod parse;
mod pivot;
mod read;
mod write;
mod concat;

fn main() {
    let matches = parse::parse();
    let input_dir = matches.get_input_dir();
    let input_files = matches.get_input_files(input_dir);
    let raw_output = matches.get_output_raw_file(input_dir);
    let agg_output = matches.get_output_agg_file(input_dir);
    let pivot_output = matches.get_output_pivot_file(input_dir);
    let min_count = matches.get_min_count();
    let date_range = make_date_range(&input_files);

    let raw_list = input_files
        .iter()
        .map(read_df)
        .filter(|df| df.height() > 0)
        .collect::<Vec<_>>();

    let mut raw = concat(&raw_list);
    write_csv(&mut raw, &raw_output);

    let mut agg = agg_df(&raw);
    write_csv(&mut agg, &agg_output);

    let mut pivot_df = species_pivot(&agg, &date_range, min_count);
    write_csv(&mut pivot_df, &pivot_output)
}
