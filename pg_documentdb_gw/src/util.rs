/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/util.rs
 *
 *-------------------------------------------------------------------------
 */

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{DocumentDBError, Result};

/// Gets the current time in milliseconds since Unix epoch.
pub fn get_current_time_millis() -> Result<i64> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                DocumentDBError::internal_error("Failed to get the current time".to_string())
            })?
            .as_millis(),
    )
    .map_err(|_| DocumentDBError::internal_error("Current time exceeded an i64".to_string()))
}
