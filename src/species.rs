use crate::read::load_file;
use polars::prelude::DataFrame;
use std::path::PathBuf;

static BIRD_SPECIES: &str = "bird_species.csv";
pub(super) fn load_species() -> DataFrame {
    let mut pb = PathBuf::new();
    pb.push(BIRD_SPECIES);
    load_file(&pb, ',')
}
