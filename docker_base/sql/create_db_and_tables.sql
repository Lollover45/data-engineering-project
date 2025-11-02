DROP DATABASE IF EXISTS messud;

CREATE DATABASE messud;

-- base tables

CREATE table messud.gbif(
    eventDate String,
    year UInt16,
    month UInt8,
    day UInt8,
    individualCount UInt64,
    continent String,
    countryCode String,
    stateProvince String,
    county String,
    decimalLatitude Float64,
    decimalLongitude Float64,
    scientificName String,
    species String
)
ENGINE = MergeTree
ORDER BY (year, month, day, county);

CREATE TABLE messud.aphis(
    sample_year UInt16,
    sample_month_number UInt8,
    sample_month String,
    state_code String,
    sampling_county String,

    varroa_per_100_bees Float32,
    million_spores_per_bee Float32,

    abpv Nullable(String),                  -- 0 = false, 1 = true
    abpv_percentile Nullable(Float32),

    amsv1 Nullable(String),
    amsv1_percentile Nullable(Float32),

    cbpv Nullable(String),
    cbpv_percentile Nullable(Float32),

    dwv Nullable(String),
    dwv_percentile Nullable(Float32),

    dwv_b Nullable(String),
    dwv_b_percentile Nullable(Float32),

    iapv Nullable(String),
    iapv_percentile Nullable(Float32),

    kbv Nullable(String),
    kbv_percentile Nullable(Float32),

    lsv2 Nullable(String),
    lsv2_percentile Nullable(Float32),

    sbpv Nullable(String),
    sbpv_percentile Nullable(Float32),

    mkv Nullable(String),
    mkv_percentile Nullable(Float32),

    pesticides String
)
ENGINE = MergeTree
ORDER BY (sample_year, state_code, sampling_county);
