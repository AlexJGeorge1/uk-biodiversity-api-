-- SQL schema defining biodiversity_indicators table and related constraints.

CREATE TABLE IF NOT EXISTS biodiversity_indicators (
    -- Auto-incrementing primary key, uniquely identifies each row
    id             SERIAL PRIMARY KEY,

    -- Name of the biodiversity indicator; one of: farmland_birds,
    -- specialist_butterflies, generalist_butterflies, priority_species
    indicator_name VARCHAR(64)    NOT NULL,

    -- The calendar year this index value was recorded (e.g. 1970, 2022)
    year           INTEGER        NOT NULL,

    -- The index value for this indicator in this year; baseline = 100.0
    -- Values below 100 indicate decline relative to baseline year
    index_value    NUMERIC(10, 4) NOT NULL,

    -- Timestamp of when this row was inserted into the database
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    -- Prevents duplicate entries for the same indicator and year
    UNIQUE (indicator_name, year)
);

-- Metadata table powering the GET /indicators endpoint.
-- One row per indicator; stores human-readable labels and context.
CREATE TABLE IF NOT EXISTS indicator_metadata (
    -- Matches indicator_name values in biodiversity_indicators; acts as the
    -- logical foreign key linking metadata to time-series data
    indicator_name VARCHAR(64)  PRIMARY KEY,

    -- Human-readable label shown in API responses (e.g. "Farmland Bird Index")
    display_name   VARCHAR(128) NOT NULL,

    -- The year the index is normalised to (index_value = 100.0 in this year)
    baseline_year  INTEGER      NOT NULL,

    -- Short ecological description of what this indicator measures and why
    -- it is significant; used in GET /indicators and GET /indicator/{name}
    description    TEXT         NOT NULL
);

