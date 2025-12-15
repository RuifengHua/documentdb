use crate::{
    changestream::{ChangeEvent, ChangeStreamManager},
    context::{ConnectionContext, RequestContext},
    error::{DocumentDBError, ErrorCode, Result},
    postgres::Connection,
    requests::request_tracker::RequestTracker,
    responses::{RawResponse, Response},
    util::get_current_time_millis,
};
use bson::{rawdoc, RawArrayBuf};

pub async fn process_changestream(
    _request_context: &RequestContext<'_>,
    connection_context: &ConnectionContext,
    db: &str,
    collection: &str,
) -> Result<Response> {
    let manager = connection_context.service_context.changestream_manager();
    let namespace = format!("{}.{}", db, collection);
    let cursor_id = manager.create_cursor(namespace).await;

    let ns = format!("{}.{}", db, collection);
    // return an empty first batch for initial watch() call
    let first_batch = RawArrayBuf::new();

    let local_time = get_current_time_millis()?;

    Ok(Response::Raw(RawResponse(rawdoc! {
        "cursor": {
            "id": cursor_id,
            "ns": ns,
            "firstBatch": first_batch
        },
        "operationTime": bson::Timestamp {
            time: (local_time / 1000) as u32,
            increment: (local_time % 1000) as u32
        },
        "ok": 1.0
    })))
}

/// Process getMore request for change stream cursor.
///
/// Polls WAL for new changes, retrieves events for the cursor, converts them to MongoDB
/// change event format, and returns them in nextBatch. Implements server-side blocking:
/// loops until events are found or timeout occurs. Uses maxTimeMS to distinguish between
/// next() (blocking) and try_next() (immediate return with maxTimeMS=0).
pub async fn process_changestream_getmore(
    cursor_id: i64,
    connection_context: &ConnectionContext,
    batch_size: usize,
    max_time_ms: Option<i64>,
) -> Result<Response> {
    let manager = connection_context.service_context.changestream_manager();

    if !manager.has_cursor(cursor_id).await {
        return Err(DocumentDBError::documentdb_error(
            ErrorCode::CursorNotFound,
            format!("Cursor {} not found", cursor_id),
        ));
    }

    let start_time = std::time::Instant::now();
    // When maxTimeMS is not provided (try_next), return immediately (timeout=0)
    // When maxTimeMS is provided (next), use that value for blocking
    let timeout_ms = max_time_ms.unwrap_or(0).max(0) as u128;

    // Loop until we have events or timeout
    loop {
        poll_wal_changes(connection_context, manager).await?;

        let events = manager
            .get_events(cursor_id, batch_size)
            .await
            .unwrap_or_default();

        if !events.is_empty() {
            let mut next_batch = RawArrayBuf::new();
            for e in events {
                if let Some(event_doc) = build_change_event(e) {
                    next_batch.push(event_doc);
                }
            }

            let local_time = get_current_time_millis()?;
            return Ok(Response::Raw(RawResponse(rawdoc! {
                "cursor": {
                    "id": cursor_id,
                    "nextBatch": next_batch
                },
                "ok": 1.0,
                "operationTime": bson::Timestamp {
                    time: (local_time / 1000) as u32,
                    increment: (local_time % 1000) as u32
                }
            })));
        }

        // Check timeout
        if start_time.elapsed().as_millis() >= timeout_ms {
            let local_time = get_current_time_millis()?;
            return Ok(Response::Raw(RawResponse(rawdoc! {
                "cursor": {
                    "id": cursor_id,
                    "nextBatch": RawArrayBuf::new()
                },
                "ok": 1.0,
                "operationTime": bson::Timestamp {
                    time: (local_time / 1000) as u32,
                    increment: (local_time % 1000) as u32
                }
            })));
        }

        // Sleep briefly before polling again to avoid hammering PostgreSQL
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Poll PostgreSQL WAL for new changes and deliver them to change stream cursors.
///
/// Queries the logical replication slot to get JSON-formatted change events from the decoder plugin.
/// Each event includes operation type, namespace, LSN, timestamp, document key, and document content.
/// Events are parsed, namespace-mapped from internal table names to db.collection format, and
/// broadcast to all active change stream cursors.
async fn poll_wal_changes(
    connection_context: &ConnectionContext,
    manager: &ChangeStreamManager,
) -> Result<()> {
    use tokio_postgres::types::Type;

    // Query WAL changes from replication slot (use get_changes to consume them)
    let query = "SELECT data FROM pg_logical_slot_get_changes('documentdb_changestream', NULL, 1000, 'include-xids', '0')";

    let service_context = &connection_context.service_context;
    let pool = service_context
        .connection_pool_manager()
        .system_requests_connection()
        .await?;

    let empty_types: &[Type] = &[];
    let empty_params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[];
    let rows = pool
        .query(
            query,
            empty_types,
            empty_params,
            None,
            &mut RequestTracker::new(),
        )
        .await?;

    for row in rows {
        if let Ok(data) = row.try_get::<_, String>("data") {
            parse_and_deliver_event(&data, &pool, manager).await;
        }
    }

    Ok(())
}

/// Parse JSON event from decoder and deliver to change stream manager
async fn parse_and_deliver_event(data: &str, pool: &Connection, manager: &ChangeStreamManager) {
    let event_json = match serde_json::from_str::<serde_json::Value>(data) {
        Ok(json) => json,
        Err(_) => return,
    };

    let (op, ns, lsn, timestamp) = match (
        event_json.get("op").and_then(|v| v.as_str()),
        event_json.get("ns").and_then(|v| v.as_str()),
        event_json.get("lsn").and_then(|v| v.as_str()),
        event_json.get("timestamp").and_then(|v| v.as_i64()),
    ) {
        (Some(o), Some(n), Some(l), Some(t)) => (o, n, l, t),
        _ => return,
    };

    let actual_ns = map_table_to_namespace(pool, ns)
        .await
        .unwrap_or_else(|| ns.to_string());

    let document_key = event_json
        .get("documentKey")
        .and_then(|dk| dk.get("_id"))
        .and_then(|id| id.as_str())
        .map(|s| s.to_string());

    let full_document = event_json
        .get("fullDocument")
        .and_then(|fd| fd.as_str())
        .map(|s| s.to_string());

    let old_document = event_json
        .get("oldDocument")
        .and_then(|od| od.as_str())
        .map(|s| s.to_string());

    let wall_time = timestamp / 1000;
    let cluster_time = timestamp / 1000000;
    let (_, increment) = manager.get_cluster_time_with_increment(cluster_time).await;

    let event = ChangeEvent {
        op: op.to_string(),
        ns: actual_ns,
        lsn: lsn.to_string(),
        document_key,
        full_document,
        old_document,
        wall_time,
        cluster_time,
        cluster_time_increment: increment,
    };

    manager.deliver_event(event).await;
}

fn hex_to_bytes(hex: &str) -> std::result::Result<Vec<u8>, std::num::ParseIntError> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect()
}

/// Decode hex-encoded BSON and return RawDocumentBuf
fn decode_hex_bson(hex: &str) -> Option<bson::RawDocumentBuf> {
    hex_to_bytes(hex)
        .ok()
        .and_then(|bytes| bson::RawDocumentBuf::from_bytes(bytes).ok())
}

/// Extract ObjectId from document key hex (field name is empty string)
fn extract_object_id(doc_key_hex: &str) -> Option<bson::oid::ObjectId> {
    decode_hex_bson(doc_key_hex)
        .and_then(|doc| doc.get("").ok().flatten().and_then(|v| v.as_object_id()))
}

/// Encode LSN as base64 resume token (opaque like MongoDB)
fn encode_resume_token(lsn: &str) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(lsn.as_bytes())
}

/// Build MongoDB change event document from ChangeEvent
fn build_change_event(e: ChangeEvent) -> Option<bson::RawDocumentBuf> {
    let op_type = match e.op.as_str() {
        "i" => "insert",
        "u" => "update",
        "d" => "delete",
        _ => return None,
    };

    let db_name = e.ns.split('.').next().unwrap_or("");
    let coll_name = e.ns.split('.').nth(1).unwrap_or("");
    let wall_time_date = bson::DateTime::from_millis(e.wall_time);
    let cluster_time_timestamp = bson::Timestamp {
        time: e.cluster_time as u32,
        increment: e.cluster_time_increment,
    };
    let oid = e
        .document_key
        .as_ref()
        .and_then(|hex| extract_object_id(hex))?;

    let resume_token = encode_resume_token(&e.lsn);

    let base_doc = rawdoc! {
        "operationType": op_type,
        "ns": { "db": db_name, "coll": coll_name },
        "_id": { "_data": resume_token },
        "clusterTime": cluster_time_timestamp,
        "wallTime": wall_time_date,
        "documentKey": { "_id": oid }
    };

    match op_type {
        "insert" => append_to_base_insert(base_doc, &e),
        "update" => append_to_base_update(base_doc, &e),
        "delete" => append_to_base_delete(base_doc),
        _ => None,
    }
}

fn append_to_base_insert(
    base_doc: bson::RawDocumentBuf,
    e: &ChangeEvent,
) -> Option<bson::RawDocumentBuf> {
    let full_doc = decode_hex_bson(e.full_document.as_ref()?)?;
    let mut doc = base_doc;
    doc.append("fullDocument", full_doc);
    Some(doc)
}

fn append_to_base_update(
    base_doc: bson::RawDocumentBuf,
    e: &ChangeEvent,
) -> Option<bson::RawDocumentBuf> {
    let old_hex = e.old_document.as_ref()?;
    let new_hex = e.full_document.as_ref()?;
    let update_desc = compute_update_description(old_hex, new_hex)?;
    let full_doc = decode_hex_bson(new_hex)?;

    let mut doc = base_doc;
    doc.append("fullDocument", full_doc);
    doc.append("updateDescription", update_desc);
    Some(doc)
}

fn append_to_base_delete(base_doc: bson::RawDocumentBuf) -> Option<bson::RawDocumentBuf> {
    Some(base_doc)
}

fn is_array_truncation(old_value: bson::RawBsonRef, new_value: bson::RawBsonRef) -> bool {
    let (Some(old_array), Some(new_array)) = (old_value.as_array(), new_value.as_array()) else {
        return false;
    };

    let mut old_iter = old_array.into_iter();
    let mut new_iter = new_array.into_iter();

    // Check prefix equality
    // Single traversal to make the new_iter exhaust
    for new_elem in &mut new_iter {
        let (Ok(new_e), Some(Ok(old_e))) = (new_elem, old_iter.next()) else {
            return false;
        };

        if new_e.to_raw_bson() != old_e.to_raw_bson() {
            return false;
        }
    }

    // Truncation requires that old has more elements
    old_iter.next().is_some()
}

/// Find updated and new fields by comparing old and new documents
fn find_updated_and_new_fields(
    old_doc: &bson::RawDocumentBuf,
    new_doc: &bson::RawDocumentBuf,
    updated_fields: &mut bson::RawDocumentBuf,
    truncated_arrays: &mut bson::RawArrayBuf,
) {
    for result in new_doc.iter() {
        if let Ok((key, new_value)) = result {
            if key == "_id" {
                continue;
            }
            match old_doc.get(key) {
                // if the key exist in both
                Ok(Some(old_value)) => {
                    if is_array_truncation(old_value, new_value) {
                        truncated_arrays.push(key);
                        continue;
                    }

                    // Field exists in both - check if changed
                    if old_value.to_raw_bson() != new_value.to_raw_bson() {
                        updated_fields.append(key, new_value.to_raw_bson());
                    }
                }
                // if the key exist only in new
                _ => {
                    // New field
                    updated_fields.append(key, new_value.to_raw_bson());
                }
            }
        }
    }
}

/// Find removed fields by checking which fields exist in old but not in new
fn find_removed_fields(
    old_doc: &bson::RawDocumentBuf,
    new_doc: &bson::RawDocumentBuf,
    removed_fields: &mut bson::RawArrayBuf,
) {
    for result in old_doc.iter() {
        if let Ok((key, _)) = result {
            if key == "_id" {
                continue;
            }
            if new_doc.get(key).ok().flatten().is_none() {
                removed_fields.push(key);
            }
        }
    }
}

/// Compute MongoDB updateDescription by comparing old and new documents.
///
/// Returns a document with updatedFields, removedFields, and truncatedArrays.
/// Detects field changes, deletions, and array truncations (prefix-preserving size reductions).
fn compute_update_description(
    old_doc_hex: &str,
    new_doc_hex: &str,
) -> Option<bson::RawDocumentBuf> {
    let old_doc = decode_hex_bson(old_doc_hex)?;
    let new_doc = decode_hex_bson(new_doc_hex)?;

    let mut updated_fields = bson::RawDocumentBuf::new();
    let mut removed_fields = bson::RawArrayBuf::new();
    let mut truncated_arrays = bson::RawArrayBuf::new();

    find_updated_and_new_fields(
        &old_doc,
        &new_doc,
        &mut updated_fields,
        &mut truncated_arrays,
    );
    find_removed_fields(&old_doc, &new_doc, &mut removed_fields);

    Some(rawdoc! {
        "updatedFields": updated_fields,
        "removedFields": removed_fields,
        "truncatedArrays": truncated_arrays
    })
}

async fn map_table_to_namespace(pool: &Connection, table_ns: &str) -> Option<String> {
    // Extract collection_id from documentdb_data.documents_X
    if let Some(table_name) = table_ns.strip_prefix("documentdb_data.documents_") {
        if let Ok(collection_id) = table_name.parse::<i32>() {
            let query = "SELECT database_name, collection_name FROM documentdb_api_catalog.collections WHERE collection_id = $1";
            let types = &[tokio_postgres::types::Type::INT4];
            let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&collection_id];

            if let Ok(rows) = pool
                .query(query, types, params, None, &mut RequestTracker::new())
                .await
            {
                if let Some(row) = rows.first() {
                    if let (Ok(db), Ok(coll)) =
                        (row.try_get::<_, String>(0), row.try_get::<_, String>(1))
                    {
                        return Some(format!("{}.{}", db, coll));
                    }
                }
            }
        }
    }
    None
}
