CREATE OR REPLACE WAREHOUSE MALLARD_QUERY_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

create database user_db_mallard;

create schema raw;

create schema curation;

create schema analytics;