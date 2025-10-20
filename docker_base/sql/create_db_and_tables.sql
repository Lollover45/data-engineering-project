DROP DATABASE IF EXISTS messud;

CREATE DATABASE messud;

-- base tables

CREATE table messud.gbif(
    eventDate String,
    year UInt16,
    month UInt8,
    day UInt8,
    individualCount UInt64,
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
    state_code FixedString(2),
    sampling_county String,

    varroa_per_100_bees Float32,
    million_spores_per_bee Float32,

    abpv UInt8,                  -- 0 = false, 1 = true
    abpv_percentile UInt8,

    amsv1 UInt8,
    amsv1_percentile UInt8,

    cbpv UInt8,
    cbpv_percentile UInt8,

    dwv UInt8,
    dwv_percentile UInt8,

    dwv_b UInt8,
    dwv_b_percentile UInt8,

    iapv UInt8,
    iapv_percentile UInt8,

    kbv UInt8,
    kbv_percentile UInt8,

    lsv2 UInt8,
    lsv2_percentile UInt8,

    sbpv UInt8,
    sbpv_percentile UInt8,

    mkv UInt8,
    mkv_percentile UInt8,

    pesticides String
)
ENGINE = MergeTree
ORDER BY (sample_year, state_code, sampling_county);
