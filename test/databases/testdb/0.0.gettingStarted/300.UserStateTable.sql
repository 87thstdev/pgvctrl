CREATE TABLE IF NOT EXISTS membership.user_state (
    user_state_id SERIAL PRIMARY KEY,
    user_state VARCHAR NOT NULL UNIQUE
);


INSERT INTO membership.user_state (user_state)
VALUES ('PROSPECT')
ON CONFLICT DO NOTHING;

INSERT INTO membership.user_state (user_state)
VALUES ('INCOMING')
ON CONFLICT DO NOTHING;

INSERT INTO membership.user_state (user_state)
VALUES ('PHASE I')
ON CONFLICT DO NOTHING;

INSERT INTO membership.user_state (user_state)
VALUES ('PHASE II')
ON CONFLICT DO NOTHING;

INSERT INTO membership.user_state (user_state)
VALUES ('PHASE III')
ON CONFLICT DO NOTHING;

INSERT INTO membership.user_state (user_state)
VALUES ('OUT GOING')
ON CONFLICT DO NOTHING;
