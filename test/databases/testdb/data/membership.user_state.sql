--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.2
-- Dumped by pg_dump version 9.6.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = membership, pg_catalog;

ALTER TABLE IF EXISTS ONLY membership.user_state DROP CONSTRAINT IF EXISTS user_state_user_state_key;
ALTER TABLE IF EXISTS ONLY membership.user_state DROP CONSTRAINT IF EXISTS user_state_pkey;
ALTER TABLE IF EXISTS membership.user_state ALTER COLUMN user_state_id DROP DEFAULT;
DROP SEQUENCE IF EXISTS membership.user_state_user_state_id_seq;
DROP TABLE IF EXISTS membership.user_state;
SET search_path = membership, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: user_state; Type: TABLE; Schema: membership; Owner: 87admin
--

CREATE TABLE user_state (
    user_state_id integer NOT NULL,
    user_state character varying NOT NULL
);


ALTER TABLE user_state OWNER TO "87admin";

--
-- Name: user_state_user_state_id_seq; Type: SEQUENCE; Schema: membership; Owner: 87admin
--

CREATE SEQUENCE user_state_user_state_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE user_state_user_state_id_seq OWNER TO "87admin";

--
-- Name: user_state_user_state_id_seq; Type: SEQUENCE OWNED BY; Schema: membership; Owner: 87admin
--

ALTER SEQUENCE user_state_user_state_id_seq OWNED BY user_state.user_state_id;


--
-- Name: user_state user_state_id; Type: DEFAULT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_state ALTER COLUMN user_state_id SET DEFAULT nextval('user_state_user_state_id_seq'::regclass);


--
-- Data for Name: user_state; Type: TABLE DATA; Schema: membership; Owner: 87admin
--

INSERT INTO user_state (user_state_id, user_state) VALUES (1, 'PROSPECT');
INSERT INTO user_state (user_state_id, user_state) VALUES (2, 'INCOMING');
INSERT INTO user_state (user_state_id, user_state) VALUES (3, 'PHASE I');
INSERT INTO user_state (user_state_id, user_state) VALUES (4, 'PHASE II');
INSERT INTO user_state (user_state_id, user_state) VALUES (5, 'PHASE III');
INSERT INTO user_state (user_state_id, user_state) VALUES (6, 'OUT GOING');


--
-- Name: user_state_user_state_id_seq; Type: SEQUENCE SET; Schema: membership; Owner: 87admin
--

SELECT pg_catalog.setval('user_state_user_state_id_seq', 12, true);


--
-- Name: user_state user_state_pkey; Type: CONSTRAINT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_state
    ADD CONSTRAINT user_state_pkey PRIMARY KEY (user_state_id);


--
-- Name: user_state user_state_user_state_key; Type: CONSTRAINT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_state
    ADD CONSTRAINT user_state_user_state_key UNIQUE (user_state);


--
-- PostgreSQL database dump complete
--

