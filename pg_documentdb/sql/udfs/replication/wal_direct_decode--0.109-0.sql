CREATE OR REPLACE FUNCTION documentdb_api.decode_wal_direct(
    start_lsn pg_lsn,
    end_lsn pg_lsn DEFAULT NULL,
    max_records integer DEFAULT 100
)
RETURNS TABLE(event_json text, event_data text)
AS 'MODULE_PATHNAME', 'documentdb_decode_wal_direct'
LANGUAGE C STRICT VOLATILE;