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

SET search_path = public, pg_catalog;

ALTER TABLE IF EXISTS ONLY public.error_set DROP CONSTRAINT IF EXISTS error_set_pkey;
ALTER TABLE IF EXISTS ONLY public.error_set DROP CONSTRAINT IF EXISTS error_set_error_name_key;
ALTER TABLE IF EXISTS ONLY public.error_set DROP CONSTRAINT IF EXISTS error_set_error_code_key;
ALTER TABLE IF EXISTS public.error_set ALTER COLUMN error_id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.error_set_error_id_seq;
DROP TABLE IF EXISTS public.error_set;
SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: error_set; Type: TABLE; Schema: public; Owner: 87admin
--

CREATE TABLE error_set (
    error_id integer NOT NULL,
    error_code character varying NOT NULL,
    error_name character varying NOT NULL
);


ALTER TABLE error_set OWNER TO "87admin";

--
-- Name: error_set_error_id_seq; Type: SEQUENCE; Schema: public; Owner: 87admin
--

CREATE SEQUENCE error_set_error_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE error_set_error_id_seq OWNER TO "87admin";

--
-- Name: error_set_error_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: 87admin
--

ALTER SEQUENCE error_set_error_id_seq OWNED BY error_set.error_id;


--
-- Name: error_set error_id; Type: DEFAULT; Schema: public; Owner: 87admin
--

ALTER TABLE ONLY error_set ALTER COLUMN error_id SET DEFAULT nextval('error_set_error_id_seq'::regclass);


--
-- Data for Name: error_set; Type: TABLE DATA; Schema: public; Owner: 87admin
--

INSERT INTO error_set (error_id, error_code, error_name) VALUES (1, '1000', 'General Error');
INSERT INTO error_set (error_id, error_code, error_name) VALUES (2, '2000', 'Type Error');
INSERT INTO error_set (error_id, error_code, error_name) VALUES (3, '3000', 'Conversion Error');
INSERT INTO error_set (error_id, error_code, error_name) VALUES (4, '4000', 'WHY WOULD YOU DO THAT!');


--
-- Name: error_set_error_id_seq; Type: SEQUENCE SET; Schema: public; Owner: 87admin
--

SELECT pg_catalog.setval('error_set_error_id_seq', 4, true);


--
-- Name: error_set error_set_error_code_key; Type: CONSTRAINT; Schema: public; Owner: 87admin
--

ALTER TABLE ONLY error_set
    ADD CONSTRAINT error_set_error_code_key UNIQUE (error_code);


--
-- Name: error_set error_set_error_name_key; Type: CONSTRAINT; Schema: public; Owner: 87admin
--

ALTER TABLE ONLY error_set
    ADD CONSTRAINT error_set_error_name_key UNIQUE (error_name);


--
-- Name: error_set error_set_pkey; Type: CONSTRAINT; Schema: public; Owner: 87admin
--

ALTER TABLE ONLY error_set
    ADD CONSTRAINT error_set_pkey PRIMARY KEY (error_id);


--
-- PostgreSQL database dump complete
--

