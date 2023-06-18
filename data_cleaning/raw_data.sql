-- create database
create database data_cleaning;
use database data_cleaning;

-- create a schema for the raw data
create schema raw;
use schema raw;
drop schema public;

-- create stage to put raw data downloaded from gthub
create stage raw_data;
list @raw_data;

-- create file format for data
create or replace file format csv_ff
type = 'csv'
FIELD_DELIMITER = ','
record_delimiter = '\n'
encoding = 'UTF-8'
FIELD_OPTIONALLY_ENCLOSED_BY='"'    --needed as some columns have data with comma
PARSE_HEADER = true    --needed for infer_schema
--skip_header = 0        --needed for selecting from stage
;


--query directly from stage to check
select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
$11, $12, $13, $14, $15, $16, $17, $18, $19
from @raw_data/NashvilleHousingData.csv (file_format => 'csv_ff') limit 3;

--use SF function to query metadata
select * from table(
infer_schema(
location => '@raw_data/NashvilleHousingData.csv'
, file_format => 'csv_ff'
));

--directly use metadata to create table
create or replace table raw_tbl
 USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          location => '@raw_data/NashvilleHousingData.csv',
          file_format => 'csv_ff'
        )
      )
);

-- load data into table
COPY into raw_tbl from @raw_data/NashvilleHousingData.csv FILE_FORMAT = (FORMAT_NAME= 'csv_ff') MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- check results
select * from raw_tbl limit 3;
select count(*) from raw_tbl;

--create backup of raw table, and clean data in orginal table
--clone from before starting today's work query
create table raw_tbl_backup 
clone raw_tbl BEFORE (STATEMENT => '01ad069a-3200-c336-0004-101600052136');
