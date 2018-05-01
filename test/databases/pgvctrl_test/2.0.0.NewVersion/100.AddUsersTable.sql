CREATE SCHEMA IF NOT EXISTS membership;

CREATE TABLE IF NOT EXISTS membership.users (
    username varchar,
    first_name varchar,
    last_name varchar,
    dob date,
    load_ts TIMESTAMP
);
