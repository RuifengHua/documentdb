use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
// Multiple readers can hold lock simultaneously while only one writer can hold lock (blocks all readers/writers)
use tokio::sync::RwLock;

pub type CursorId = i64;

// Note: Add Serialize/Deserialize derives when integrating with Kafka or other stuff in the future
///
///    An example of ChangeEvent
///    {
///        operationType: 'insert',
///        ns: { db: 'mydb', coll: 'test' },
///        _id: { _data: '0/253F420' },
///        clusterTime: Timestamp({ t: 1765753609, i: 1 }),
///        wallTime: ISODate('2025-12-14T23:06:49.582Z'),
///        documentKey: { _id: ObjectId('693f4309f13e24d00289b03e') },
///        fullDocument: { _id: ObjectId('693f4309f13e24d00289b03e'), x: 1 }
///    }
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

    /// Creates a new change stream cursor for the given namespace.
    ///
    /// Returns the cursor ID that can be used to retrieve events.
    pub async fn create_cursor(&self, namespace: String) -> CursorId {
        let mut next_id = self.next_cursor_id.write().await;
        let cursor_id = *next_id;
        *next_id += 1;

        let cursor = Cursor {
            namespace,
            buffer: VecDeque::new(),
            max_buffer_size: 10000, // Limit to 10K events (~30MB per cursor)
        };

        self.cursors.write().await.insert(cursor_id, cursor);
        cursor_id
    }

    /// Checks if a cursor with the given ID exists.
    pub async fn has_cursor(&self, cursor_id: CursorId) -> bool {
        self.cursors.read().await.contains_key(&cursor_id)
    }

    /// Retrieves up to batch_size events from the cursor's buffer.
    ///
    /// Returns None if the cursor doesn't exist, or Some(Vec) with available events.
    pub async fn get_events(
        &self,
        cursor_id: CursorId,
        batch_size: usize,
    ) -> Option<Vec<ChangeEvent>> {
        let mut cursors = self.cursors.write().await;
        let cursor = cursors.get_mut(&cursor_id)?;

        let mut events = Vec::new();
        for _ in 0..batch_size {
            if let Some(event) = cursor.buffer.pop_front() {
                events.push(event);
            } else {
                break;
            }
        }
        Some(events)
    }

    /// Delivers a change event to all cursors watching the event's namespace.
    /// If a cursor's buffer is full, the oldest event is dropped (FIFO eviction).
    pub async fn deliver_event(&self, event: ChangeEvent) {
        let mut cursors = self.cursors.write().await;
        for cursor in cursors.values_mut() {
            if cursor.namespace == event.ns {
                // If buffer is full, drop oldest event
                if cursor.buffer.len() >= cursor.max_buffer_size {
                    cursor.buffer.pop_front();
                }
                cursor.buffer.push_back(event.clone());
            }
        }
    }

    // Get next cluster time with proper increment (MongoDB-style)
    pub async fn get_cluster_time_with_increment(&self, timestamp_seconds: i64) -> (u32, u32) {
        let mut last_time = self.last_cluster_time.write().await;
        let (last_seconds, last_increment) = *last_time;

        if timestamp_seconds == last_seconds {
            // Same second, increment the counter
            let new_increment = last_increment + 1;
            *last_time = (timestamp_seconds, new_increment);
            (timestamp_seconds as u32, new_increment)
        } else {
            // New second, reset increment to 1
            *last_time = (timestamp_seconds, 1);
            (timestamp_seconds as u32, 1)
        }
    }
}

struct Cursor {
    namespace: String,
    buffer: VecDeque<ChangeEvent>,
    max_buffer_size: usize, // Maximum events to buffer
}
