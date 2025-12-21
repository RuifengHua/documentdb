use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type CursorId = i64;

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub op: String,
    pub ns: String,
    pub lsn: String,
    pub document_key: Option<String>,
    pub full_document: Option<String>,
    pub old_document: Option<String>,
    pub wall_time: i64,              // Unix timestamp in milliseconds
    pub cluster_time: i64,           // Unix timestamp in seconds
    pub cluster_time_increment: u32, // Increment within the second
}

pub struct ChangeStreamManager {
    cursors: Arc<RwLock<HashMap<CursorId, Cursor>>>,
    next_cursor_id: Arc<RwLock<i64>>,
    // Track increment counter per second for clusterTime
    last_cluster_time: Arc<RwLock<(i64, u32)>>, // (seconds, increment)
}

impl ChangeStreamManager {
    pub fn new() -> Self {
        Self {
            cursors: Arc::new(RwLock::new(HashMap::new())),
            next_cursor_id: Arc::new(RwLock::new(1)),
            last_cluster_time: Arc::new(RwLock::new((0, 0))),
        }
    }

    pub async fn create_cursor(&self, namespace: String, start_lsn: Option<String>, start_at_operation_time: Option<i64>) -> CursorId {
        let mut next_id = self.next_cursor_id.write().await;
        let cursor_id = *next_id;
        *next_id += 1;

        let cursor = Cursor {
            namespace,
            current_lsn: start_lsn,
            start_at_operation_time,
        };

        self.cursors.write().await.insert(cursor_id, cursor);
        cursor_id
    }

    pub async fn has_cursor(&self, cursor_id: CursorId) -> bool {
        self.cursors.read().await.contains_key(&cursor_id)
    }

    pub async fn get_cursor_info(&self, cursor_id: CursorId) -> Option<(String, Option<String>, Option<i64>)> {
        let cursors = self.cursors.read().await;
        cursors
            .get(&cursor_id)
            .map(|c| (c.namespace.clone(), c.current_lsn.clone(), c.start_at_operation_time))
    }

    pub async fn update_cursor_lsn(&self, cursor_id: CursorId, new_lsn: String) {
        let mut cursors = self.cursors.write().await;
        if let Some(cursor) = cursors.get_mut(&cursor_id) {
            cursor.current_lsn = Some(new_lsn);
        }
    }

    pub async fn delete_cursor(&self, cursor_id: CursorId) {
        self.cursors.write().await.remove(&cursor_id);
    }

    pub async fn get_next_cluster_time_increment(&self, cluster_time_seconds: i64) -> u32 {
        let mut last = self.last_cluster_time.write().await;
        if last.0 == cluster_time_seconds {
            last.1 += 1;
            last.1
        } else {
            *last = (cluster_time_seconds, 1);
            1
        }
    }
}

struct Cursor {
    namespace: String,
    current_lsn: Option<String>,
    start_at_operation_time: Option<i64>,
}
