use crate::{
    changestream::ChangeEvent,
    context::{ConnectionContext, RequestContext},
    error::{DocumentDBError, ErrorCode, Result},
    postgres::Connection,
    requests::request_tracker::RequestTracker,
    responses::{RawResponse, Response},
    util::get_current_time_millis,
};
use bson::{rawdoc, RawArrayBuf};

fn build_cursor_response(
    cursor_id: i64,
    db: &str,
    ns_suffix: &str,
    batch: RawArrayBuf,
    resume_token: &str,
) -> Result<Response> {
    let local_time = get_current_time_millis()?;
    Ok(Response::Raw(RawResponse(rawdoc! {
        "cursor": {
            "id": cursor_id,
            "ns": format!("{}.{}", db, ns_suffix),
            "firstBatch": batch,
            "postBatchResumeToken": { "_data": encode_resume_token(resume_token) }
        },
        "operationTime": bson::Timestamp {
            time: (local_time / 1000) as u32,
            increment: (local_time % 1000) as u32
        },
        "ok": 1.0
    })))
}

fn build_getmore_response(
    cursor_id: i64,
    batch: RawArrayBuf,
    resume_token: &str,
) -> Result<Response> {
    let local_time = get_current_time_millis()?;
    Ok(Response::Raw(RawResponse(rawdoc! {
        "cursor": {
            "id": cursor_id,
            "nextBatch": batch,
            "postBatchResumeToken": { "_data": encode_resume_token(resume_token) }
        },
        "ok": 1.0,
        "operationTime": bson::Timestamp {
            time: (local_time / 1000) as u32,
            increment: (local_time % 1000) as u32
        }
    })))
}

pub async fn process_changestream(
    _request_context: &RequestContext<'_>,
    connection_context: &ConnectionContext,
    db: &str,
    collection: &str,
    resume_token: Option<String>,
    start_at_operation_time: Option<i64>,
) -> Result<Response> {
    let manager = connection_context.service_context.changestream_manager();
    let is_db_level = collection.is_empty();
    let namespace = if is_db_level {
        db.to_string()
    } else {
        format!("{}.{}", db, collection)
    };

    let start_lsn = match resume_token {
        // If user provided resumeAfter, we should use that lsn to start
        Some(token) => Some(decode_resume_token(&token)?),
        // If resume_token is None and start_at_operation_time is None, then we use the current LSN
        None if start_at_operation_time.is_none() => {
            Some(get_current_lsn(connection_context).await?)
        }
        // If start_at_operation_time is provided, we set resume_token to None and use start_at_operation_time later
        None => None,
    };

    let cursor_id = manager
        .create_cursor(namespace, start_lsn.clone(), start_at_operation_time)
        .await;
    let ns_suffix = if is_db_level {
        "$cmd.aggregate"
    } else {
        collection
    };
    let resume_token = start_lsn.as_deref().unwrap_or("");

    build_cursor_response(cursor_id, db, ns_suffix, RawArrayBuf::new(), resume_token)
}

async fn get_current_lsn(connection_context: &ConnectionContext) -> Result<String> {
    let pool = connection_context
        .service_context
        .connection_pool_manager()
        .system_requests_connection()
        .await?;
    let rows = pool
        .query(
            "SELECT pg_current_wal_lsn()::text",
            &[],
            &[],
            None,
            &mut RequestTracker::new(),
        )
        .await?;
    rows.first()
        .and_then(|row| row.try_get::<_, String>(0).ok())
        .ok_or_else(|| {
            DocumentDBError::documentdb_error(
                ErrorCode::InternalError,
                "Failed to get current LSN".to_string(),
            )
        })
}

pub async fn process_changestream_getmore(
    cursor_id: i64,
    connection_context: &ConnectionContext,
    batch_size: usize,
    max_time_ms: Option<i64>,
) -> Result<Response> {
    let manager = connection_context.service_context.changestream_manager();
    let start_time = std::time::Instant::now();
    let timeout_ms = max_time_ms.unwrap_or(0).max(0) as u128;

    loop {
        let (namespace, current_lsn, start_at_operation_time) =
            manager.get_cursor_info(cursor_id).await.ok_or_else(|| {
                DocumentDBError::documentdb_error(
                    ErrorCode::CursorNotFound,
                    format!("Cursor {} not found", cursor_id),
                )
            })?;

        let (events, last_scanned_lsn) = fetch_wal_events(
            connection_context,
            &namespace,
            current_lsn.as_deref(),
            start_at_operation_time,
            batch_size,
        )
        .await?;

        let post_batch_resume_lsn =
            last_scanned_lsn.unwrap_or_else(|| current_lsn.clone().unwrap_or_default());

        if !events.is_empty() {
            let resume_lsn = events.last().unwrap().lsn.clone();
            manager
                .update_cursor_lsn(cursor_id, resume_lsn.clone())
                .await;

            let mut next_batch = RawArrayBuf::new();
            for mut e in events {
                e.cluster_time_increment = manager
                    .get_next_cluster_time_increment(e.cluster_time)
                    .await;
                if let Some(doc) = build_change_event(e) {
                    next_batch.push(doc);
                }
            }
            return Ok(build_getmore_response(
                cursor_id,
                next_batch,
                &post_batch_resume_lsn,
            )?);
        }

        if start_time.elapsed().as_millis() >= timeout_ms {
            return Ok(build_getmore_response(
                cursor_id,
                RawArrayBuf::new(),
                &post_batch_resume_lsn,
            )?);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

async fn fetch_wal_events(
    connection_context: &ConnectionContext,
    namespace: &str,
    start_lsn: Option<&str>,
    start_at_operation_time: Option<i64>,
    batch_size: usize,
) -> Result<(Vec<ChangeEvent>, Option<String>)> {
    let pool = connection_context
        .service_context
        .connection_pool_manager()
        .system_requests_connection()
        .await?;
    let internal_ns = map_namespace_to_table(&pool, namespace).await?;

    let query = match (start_lsn, start_at_operation_time) {
        (Some(lsn), _) => format!(
            "SELECT event_json, last_scanned_lsn FROM documentdb_api.decode_wal_direct('{}', {}, '{}', NULL)",
            lsn, batch_size, internal_ns
        ),
        (None, Some(ts)) => format!(
            "SELECT event_json, last_scanned_lsn FROM documentdb_api.decode_wal_direct(NULL, {}, '{}', {})",
            batch_size, internal_ns, ts
        ),
        _ => return Ok((Vec::new(), None)),
    };

    let rows = pool
        .query(&query, &[], &[], None, &mut RequestTracker::new())
        .await?;
    let mut events = Vec::new();
    let mut last_scanned_lsn = None;
    for row in rows {
        if let Ok(data) = row.try_get::<_, String>(0) {
            if let Some(event) = parse_wal_event(&data, &pool).await {
                events.push(event);
            }
        }
        if let Ok(lsn) = row.try_get::<_, String>(1) {
            last_scanned_lsn = Some(lsn);
        }
    }
    Ok((events, last_scanned_lsn))
}

async fn parse_wal_event(data: &str, pool: &Connection) -> Option<ChangeEvent> {
    let event_json = serde_json::from_str::<serde_json::Value>(data).ok()?;
    let op = event_json.get("op")?.as_str()?;
    let ns = event_json.get("ns")?.as_str()?;
    let lsn = event_json.get("lsn")?.as_str()?;
    let timestamp = event_json.get("timestamp")?.as_i64()?;
    let actual_ns = map_table_to_namespace(pool, ns)
        .await
        .unwrap_or_else(|| ns.to_string());
    let document_key = event_json
        .get("tuple")
        .and_then(|t| t.as_str())
        .map(|s| s.to_string());

    Some(ChangeEvent {
        op: op.to_string(),
        ns: actual_ns,
        lsn: lsn.to_string(),
        document_key: document_key.clone(),
        full_document: document_key.clone(),
        old_document: if op == "d" { document_key } else { None },
        wall_time: timestamp / 1000,
        cluster_time: timestamp / 1000000,
        cluster_time_increment: 0,
    })
}

async fn map_namespace_to_table(pool: &Connection, namespace: &str) -> Result<String> {
    let parts: Vec<&str> = namespace.split('.').collect();
    if parts.len() != 2 {
        return Ok("documentdb_data.*".to_string());
    }

    let (db, coll) = (parts[0], parts[1]);
    let rows = pool.query(
        "SELECT collection_id FROM documentdb_api_catalog.collections WHERE database_name = $1 AND collection_name = $2",
        &[tokio_postgres::types::Type::TEXT, tokio_postgres::types::Type::TEXT],
        &[&db, &coll],
        None,
        &mut RequestTracker::new()
    ).await?;

    if let Some(row) = rows.first() {
        if let Ok(collection_id) = row.try_get::<_, i64>(0) {
            return Ok(format!("documentdb_data.documents_{}", collection_id));
        }
    }
    Ok(format!("documentdb_data.nonexistent_{}_{}", db, coll))
}

async fn map_table_to_namespace(pool: &Connection, table_ns: &str) -> Option<String> {
    let collection_id = table_ns
        .strip_prefix("documentdb_data.documents_")?
        .parse::<i32>()
        .ok()?;
    let rows = pool.query(
        "SELECT database_name, collection_name FROM documentdb_api_catalog.collections WHERE collection_id = $1",
        &[tokio_postgres::types::Type::INT4],
        &[&collection_id],
        None,
        &mut RequestTracker::new()
    ).await.ok()?;

    rows.first().and_then(|row| {
        let db = row.try_get::<_, String>(0).ok()?;
        let coll = row.try_get::<_, String>(1).ok()?;
        Some(format!("{}.{}", db, coll))
    })
}

fn hex_to_bytes(hex: &str) -> std::result::Result<Vec<u8>, std::num::ParseIntError> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect()
}

fn decode_hex_bson(hex: &str) -> Option<bson::RawDocumentBuf> {
    let bytes = hex_to_bytes(hex).ok()?;
    for offset in 24..bytes.len().saturating_sub(4) {
        let bson_len_bytes = bytes.get(offset..offset + 4)?;
        let bson_len = u32::from_le_bytes([
            bson_len_bytes[0],
            bson_len_bytes[1],
            bson_len_bytes[2],
            bson_len_bytes[3],
        ]) as usize;
        if bson_len >= 10 && bson_len <= bytes.len() - offset {
            if let Ok(doc) =
                bson::RawDocumentBuf::from_bytes(bytes[offset..offset + bson_len].to_vec())
            {
                return Some(doc);
            }
        }
    }
    None
}

fn extract_object_id(doc_key_hex: &str) -> Option<bson::oid::ObjectId> {
    decode_hex_bson(doc_key_hex)
        .and_then(|doc| doc.get("_id").ok().flatten().and_then(|v| v.as_object_id()))
}

fn encode_resume_token(lsn: &str) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(lsn.as_bytes())
}

fn decode_resume_token(token: &str) -> Result<String> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let bytes = STANDARD.decode(token).map_err(|_| {
        DocumentDBError::documentdb_error(ErrorCode::BadValue, "Invalid resume token".to_string())
    })?;
    String::from_utf8(bytes).map_err(|_| {
        DocumentDBError::documentdb_error(ErrorCode::BadValue, "Invalid resume token".to_string())
    })
}

fn build_change_event(e: ChangeEvent) -> Option<bson::RawDocumentBuf> {
    let op_type = match e.op.as_str() {
        "i" => "insert",
        "u" => "update",
        "d" => "delete",
        _ => return None,
    };

    let mut parts = e.ns.split('.');
    let (db_name, coll_name) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let oid = extract_object_id(e.document_key.as_ref()?)?;

    let mut doc = rawdoc! {
        "operationType": op_type,
        "ns": { "db": db_name, "coll": coll_name },
        "_id": { "_data": encode_resume_token(&e.lsn) },
        "clusterTime": bson::Timestamp { time: e.cluster_time as u32, increment: e.cluster_time_increment },
        "wallTime": bson::DateTime::from_millis(e.wall_time),
        "documentKey": { "_id": oid }
    };

    if op_type != "delete" {
        let full_doc = decode_hex_bson(e.full_document.as_ref()?)?;
        doc.append("fullDocument", full_doc);
    }
    Some(doc)
}