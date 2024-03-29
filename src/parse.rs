use clap::{arg, value_parser, Arg, ArgGroup, ArgMatches, Command};
use std::fs;
use std::path::PathBuf;

pub(crate) fn parse() -> ArgMatches {
    Command::new("cooper")
        .arg(arg!(--dir[DIR]))
        .arg(arg!(--output <OUTPUT>))
        .arg(arg!(--year))
        .arg(
            Arg::new("min-count")
                .long("min-count")
                .required(false)
                .value_parser(value_parser!(u8)),
        )
        .arg(
            Arg::new("raw-filter")
                .long("raw-filter")
                .required(false)
                .value_parser(value_parser!(f32)),
        )
        .arg(
            Arg::new("location")
                .long("location")
                .required(false)
                .value_parser(value_parser!(String)),
        )
        .arg(
            Arg::new("fixed-location")
                .long("fixed-location")
                .required(false)
                .value_parser(value_parser!(usize)),
        )
        .group(
            ArgGroup::new("location-type")
                .args(["location", "fixed-location"])
                .required(false),
        )
        .get_matches()
}

pub(super) trait CooperParse<'a> {
    fn get_by_year(&self) -> bool;
    fn get_fixed_location(&self) -> Option<usize>;

    fn get_input_dir(&self) -> &str;

    fn get_input_files(&self, dir: &str) -> Vec<PathBuf>;

    fn get_location(&self) -> Option<PathBuf>;

    fn get_min_count(&self) -> Option<u8>;

    fn get_output_agg_file(&self, dir: &str) -> String;

    fn get_output_base(&self, dir: &str) -> String;

    fn get_output_pivot_file(&self, dir: &str) -> String;

    fn get_output_raw_file(&self, dir: &str) -> String;

    fn get_raw_filter(&self) -> Option<f32>;
}

impl<'a> CooperParse<'a> for ArgMatches {
    fn get_by_year(&self) -> bool {
        self.get_flag("year")
    }

    fn get_fixed_location(&self) -> Option<usize> {
        self.get_one::<usize>("fixed-location").copied()
    }

    fn get_input_dir(&self) -> &str {
        match self.get_one::<String>("dir") {
            Some(dir) => dir,
            None => panic!("No recording directory provided."),
        }
    }

    fn get_input_files(&self, dir: &str) -> Vec<PathBuf> {
        match fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) => panic!("{}", e),
        }
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| f.is_file())
        .filter(|f| f.extension().is_some())
        .filter(|f| f.extension().unwrap().to_str().unwrap() == "txt")
        .collect::<Vec<_>>()
    }

    fn get_location(&self) -> Option<PathBuf> {
        self.get_one::<String>("location").map(|f| {
            let mut pb = PathBuf::new();
            pb.push(f);
            pb
        })
    }

    fn get_min_count(&self) -> Option<u8> {
        self.get_one::<u8>("min-count").copied()
    }

    fn get_output_agg_file(&self, dir: &str) -> String {
        let base_file = self.get_output_base(dir);
        let filter = self.get_raw_filter().map(|f| f.to_string());
        format!("{}_agg{}.csv", base_file, filter.unwrap_or("".to_owned()))
    }

    fn get_output_base(&self, dir: &str) -> String {
        match self.get_one::<String>("output") {
            Some(out) => match out.split('.').next() {
                Some(b) => b,
                None => dir,
            },
            None => dir,
        }
        .to_owned()
    }

    fn get_output_pivot_file(&self, dir: &str) -> String {
        let base_file = self.get_output_base(dir);
        base_file + "_pivot.csv"
    }

    fn get_output_raw_file(&self, dir: &str) -> String {
        let base_file = self.get_output_base(dir);
        let filter = self.get_raw_filter().map(|f| f.to_string());
        format!("{}_raw{}.csv", base_file, filter.unwrap_or("".to_owned()))
    }

    fn get_raw_filter(&self) -> Option<f32> {
        self.get_one::<f32>("raw-filter").copied()
    }
}
