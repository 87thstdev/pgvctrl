CREATE TABLE IF NOT EXISTS error_set (
    error_id SERIAL PRIMARY KEY,
    error_code VARCHAR NOT NULL UNIQUE,
    error_name VARCHAR NOT NULL UNIQUE
);


INSERT INTO error_set (error_code, error_name)
VALUES ('1000', 'General Error')
ON CONFLICT DO NOTHING;

INSERT INTO error_set (error_code, error_name)
VALUES ('2000', 'Type Error')
ON CONFLICT DO NOTHING;

INSERT INTO error_set (error_code, error_name)
VALUES ('3000', 'Conversion Error')
ON CONFLICT DO NOTHING;

INSERT INTO error_set (error_code, error_name)
VALUES ('4000', 'WHY WOULD YOU DO THAT!')
ON CONFLICT DO NOTHING;
