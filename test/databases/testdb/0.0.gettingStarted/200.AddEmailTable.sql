CREATE TABLE IF NOT EXISTS membership.user_emails (
    email_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    email_addy VARCHAR ,
    load_ts TIMESTAMP
);
