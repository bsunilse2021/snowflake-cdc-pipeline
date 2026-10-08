-- RAW  : landing/source tables the stream watches
-- CORE : curated, query-ready tables maintained incrementally
-- AUDIT: run logs and schema version history
CREATE SCHEMA IF NOT EXISTS {{ DATABASE }}.RAW;
CREATE SCHEMA IF NOT EXISTS {{ DATABASE }}.CORE;
CREATE SCHEMA IF NOT EXISTS {{ DATABASE }}.AUDIT;
