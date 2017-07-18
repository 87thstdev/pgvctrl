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

--
-- Name: membership; Type: SCHEMA; Schema: -; Owner: 87admin
--

CREATE SCHEMA membership;


ALTER SCHEMA membership OWNER TO "87admin";

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = membership, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: user_emails; Type: TABLE; Schema: membership; Owner: 87admin
--

CREATE TABLE user_emails (
    email_id integer NOT NULL,
    user_id integer,
    email_addy character varying,
    load_ts timestamp without time zone
);


ALTER TABLE user_emails OWNER TO "87admin";

--
-- Name: user_emails_email_id_seq; Type: SEQUENCE; Schema: membership; Owner: 87admin
--

CREATE SEQUENCE user_emails_email_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE user_emails_email_id_seq OWNER TO "87admin";

--
-- Name: user_emails_email_id_seq; Type: SEQUENCE OWNED BY; Schema: membership; Owner: 87admin
--

ALTER SEQUENCE user_emails_email_id_seq OWNED BY user_emails.email_id;


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
-- Name: users; Type: TABLE; Schema: membership; Owner: 87admin
--

CREATE TABLE users (
    username character varying,
    first_name character varying,
    last_name character varying,
    dob date,
    load_ts timestamp without time zone
);


ALTER TABLE users OWNER TO "87admin";

SET search_path = public, pg_catalog;

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
-- Name: repository_version; Type: TABLE; Schema: public; Owner: 87admin
--

CREATE TABLE repository_version (
    version character varying NOT NULL,
    repository_name character varying NOT NULL,
    version_hash jsonb
);


ALTER TABLE repository_version OWNER TO "87admin";

SET search_path = membership, pg_catalog;

--
-- Name: user_emails email_id; Type: DEFAULT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_emails ALTER COLUMN email_id SET DEFAULT nextval('user_emails_email_id_seq'::regclass);


--
-- Name: user_state user_state_id; Type: DEFAULT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_state ALTER COLUMN user_state_id SET DEFAULT nextval('user_state_user_state_id_seq'::regclass);


SET search_path = public, pg_catalog;

--
-- Name: error_set error_id; Type: DEFAULT; Schema: public; Owner: 87admin
--

ALTER TABLE ONLY error_set ALTER COLUMN error_id SET DEFAULT nextval('error_set_error_id_seq'::regclass);


SET search_path = membership, pg_catalog;

--
-- Name: user_emails user_emails_pkey; Type: CONSTRAINT; Schema: membership; Owner: 87admin
--

ALTER TABLE ONLY user_emails
    ADD CONSTRAINT user_emails_pkey PRIMARY KEY (email_id);


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


SET search_path = public, pg_catalog;

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

