CREATE TABLE asset_schema (
primaryid serial PRIMARY KEY,
asset_name varchar(255) NOT NULL,
schema_json varchar(2000) NOT NULL,
UNIQUE(asset_name)
);
