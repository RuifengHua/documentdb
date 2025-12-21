CREATE OR REPLACE FUNCTION documentdb_api.decode_wal_direct(
    start_lsn pg_lsn DEFAULT NULL,
    max_records integer DEFAULT 100,
    namespace_filter text DEFAULT NULL,
    start_at_operation_time bigint DEFAULT NULL
)
RETURNS TABLE(event_json text, event_data text, last_scanned_lsn text)
AS 'MODULE_PATHNAME', 'documentdb_decode_wal_direct'
LANGUAGE C VOLATILE;