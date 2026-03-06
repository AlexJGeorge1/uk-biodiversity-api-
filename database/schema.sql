-- SQL schema defining biodiversity_indicators table and related constraints.

CREATE TABLE IF NOT EXISTS biodiversity_indicators (
    id             SERIAL PRIMARY KEY,
    indicator_name VARCHAR(64)    NOT NULL,
    year           INTEGER        NOT NULL,
    index_value    NUMERIC(10, 4) NOT NULL,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (indicator_name, year)
);
