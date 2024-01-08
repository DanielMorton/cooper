use polars::prelude::{DataFrame, LazyFileListReader};

static BIRD_SPECIES: &str = "bird_species.csv";
pub(super) fn load_species() -> DataFrame {
    match polars::prelude::LazyCsvReader::new(BIRD_SPECIES)
        .has_header(true)
        .with_separator(u8::try_from(',').unwrap())
        .finish()
        .map(|f| f.collect())
    {
        Ok(r) => match r {
            Ok(df) => df,
            Err(e) => panic!("Failed to load {}:\n {:?}", BIRD_SPECIES, e),
        },
        Err(e) => panic!("Failed to load {}:\n {:?}", BIRD_SPECIES, e),
    }
}