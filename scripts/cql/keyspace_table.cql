CREATE KEYSPACE IF NOT EXISTS txtly WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS txtly.url_mappings (
    slug text,
    long_url text,
    company_id text,
    channel smallint,
    advanced smallint, 
    PRIMARY KEY (slug)) WITH default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS txtly.company_urls_expiry (
    company_id text,
    expiry date,
    slug text,
    PRIMARY KEY (company_id, expiry, slug)) 
    WITH CLUSTERING ORDER BY (expiry DESC);

CREATE TABLE IF NOT EXISTS txtly.company_urls_created (
    company_id text,
    bucket text,
    created date,
    slug text,
    PRIMARY KEY ((company_id,bucket) , created, slug)) 
    WITH CLUSTERING ORDER BY (created DESC);

