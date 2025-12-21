/*-------------------------------------------------------------------------
 * wal_direct_decode.c
 *
 * Direct WAL decoding without replication slot
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/heapam_xlog.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/commit_ts.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relfilenumbermap.h"
#include "utils/timestamp.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"

PG_FUNCTION_INFO_V1(documentdb_decode_wal_direct);

#define POSTGRES_EPOCH_OFFSET_US 946684800000000LL

typedef struct WalDecodeState
{
    XLogRecPtr start_lsn;
    XLogRecPtr end_lsn;
    XLogRecPtr last_scanned_lsn;  /* Last LSN scanned, regardless of filter match */
    int max_records;
    int records_read;
    char **results;
    int result_count;
    char *namespace_filter;
} WalDecodeState;

static void decode_wal_range(WalDecodeState *state);
static int64 get_commit_timestamp_micros(TransactionId xid);
static bool namespace_matches(const char *current_ns, const char *filter);
static XLogRecPtr find_lsn_by_timestamp(int64 target_time_seconds);

static int
wal_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
              XLogRecPtr targetPtr, char *readBuf)
{
    XLogSegNo segno = targetPagePtr / wal_segment_size;
    uint32 offset = targetPagePtr % wal_segment_size;
    char path[MAXPGPATH];
    int fd, bytes;
    
    snprintf(path, MAXPGPATH, "pg_wal/%08X%08X%08X", 1,
             (uint32)(segno / 0x100000000ULL), (uint32)(segno % 0x100000000ULL));
    
    fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0) return -1;
    
    if (lseek(fd, (off_t)offset, SEEK_SET) < 0) {
        close(fd);
        return -1;
    }
    
    bytes = read(fd, readBuf, XLOG_BLCKSZ);
    close(fd);
    
    return (bytes == XLOG_BLCKSZ) ? bytes : -1;
}

static int64
get_commit_timestamp_micros(TransactionId xid)
{
    TimestampTz ts;
    
    if (TransactionIdIsValid(xid) && TransactionIdGetCommitTsData(xid, &ts, NULL))
        return (int64)ts + POSTGRES_EPOCH_OFFSET_US;
    
    return (int64)GetCurrentTimestamp() + POSTGRES_EPOCH_OFFSET_US;
}

static bool
namespace_matches(const char *current_ns, const char *filter)
{
    if (!filter || strcmp(filter, "*") == 0)
        return true;
    
    if (strcmp(current_ns, filter) == 0)
        return true;
    
    size_t len = strlen(filter);
    if (len > 2 && filter[len-1] == '*' && filter[len-2] == '.')
        return strncmp(current_ns, filter, len-1) == 0;
    
    return false;
}

static char *
data_to_hex(char *data, Size len)
{
    if (!data || len == 0) return NULL;
    
    char *hex = palloc(len * 2 + 1);
    for (Size i = 0; i < len; i++)
        sprintf(hex + i * 2, "%02x", (unsigned char)data[i]);
    hex[len * 2] = '\0';
    return hex;
}

static void
decode_heap_operation(StringInfo buf, XLogReaderState *record, Relation rel, const char *filter)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    const char *schema = get_namespace_name(RelationGetNamespace(rel));
    const char *table = RelationGetRelationName(rel);
    
    if (!schema || strcmp(schema, "documentdb_data") != 0 ||
        !table || strncmp(table, "documents_", 10) != 0)
        return;
    
    if (filter) {
        char ns[256];
        snprintf(ns, sizeof(ns), "%s.%s", schema, table);
        if (!namespace_matches(ns, filter))
            return;
    }
    
    int64 timestamp = get_commit_timestamp_micros(XLogRecGetXid(record));
    char *tuple_data = NULL;
    
    if (XLogRecHasBlockData(record, 0)) {
        Size tuple_len;
        char *data = XLogRecGetBlockData(record, 0, &tuple_len);
        tuple_data = data_to_hex(data, tuple_len);
    }
    
    const char *op_type = NULL;
    if (info == XLOG_HEAP_INSERT || info == XLOG_HEAP_INIT_PAGE)
        op_type = "i";
    else if (info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE)
        op_type = "u";
    else if (info == XLOG_HEAP_DELETE) {
        xl_heap_delete *xlrec = (xl_heap_delete *)XLogRecGetData(record);
        char *old_data = NULL;
        
        if (xlrec->flags & XLH_DELETE_CONTAINS_OLD_TUPLE) {
            char *rec_data = XLogRecGetData(record);
            Size rec_len = XLogRecGetDataLen(record);
            if (rec_len > SizeOfHeapDelete)
                old_data = data_to_hex(rec_data + SizeOfHeapDelete, rec_len - SizeOfHeapDelete);
        }
        
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blkno;
        XLogRecGetBlockTag(record, 0, &rlocator, &forknum, &blkno);
        
        appendStringInfo(buf, "{\"op\":\"d\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%ld,\"tid\":\"(%u,%u)\",\"tuple\":\"%s\"}",
            schema, table, LSN_FORMAT_ARGS(record->EndRecPtr), timestamp, blkno, xlrec->offnum, old_data ? old_data : "null");
        
        if (old_data) pfree(old_data);
        if (tuple_data) pfree(tuple_data);
        return;
    }

    if (op_type) {
        appendStringInfo(buf, "{\"op\":\"%s\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%ld,\"tuple\":\"%s\"}",
            op_type, schema, table, LSN_FORMAT_ARGS(record->EndRecPtr), timestamp, tuple_data ? tuple_data : "null");
    }
    
    if (tuple_data) pfree(tuple_data);
}

static void
decode_wal_range(WalDecodeState *state)
{
    XLogReaderState *reader;
    char *errormsg = NULL;
    int count = 0;
    
    reader = XLogReaderAllocate(wal_segment_size, NULL,
                                XL_ROUTINE(.page_read = wal_page_read,
                                          .segment_open = NULL,
                                          .segment_close = NULL), NULL);
    
    if (!reader)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    
    XLogBeginRead(reader, state->start_lsn);
    state->last_scanned_lsn = state->start_lsn;
    
    /* Find next valid record after start_lsn to resume from */
    if (!XLogRecPtrIsInvalid(state->start_lsn)) {
        XLogRecPtr next_lsn = XLogFindNextRecord(reader, state->start_lsn);
        if (XLogRecPtrIsInvalid(next_lsn)) {
            /* No valid record found - return empty results */
            XLogReaderFree(reader);
            state->result_count = 0;
            return;
        }
        XLogBeginRead(reader, next_lsn);
        state->last_scanned_lsn = next_lsn;
    }
    
    while (count < state->max_records) {
        XLogRecord *record = XLogReadRecord(reader, &errormsg);
        if (!record) break;
        
        /* Update last scanned LSN for every record we read */
        state->last_scanned_lsn = reader->EndRecPtr;
        
        uint8 rmid = XLogRecGetRmid(reader);
        // Only process heap operations (table data changes). Skip other WAL records like index updates, checkpoints, etc.
        if (rmid != RM_HEAP_ID && rmid != RM_HEAP2_ID) continue;
        // Skip records without block references (no actual data).
        if (!XLogRecHasBlockRef(reader, 0)) continue;
        
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blkno;
        XLogRecGetBlockTag(reader, 0, &rlocator, &forknum, &blkno);
        
        Oid relid = RelidByRelfilenumber(rlocator.spcOid, rlocator.relNumber);
        if (!OidIsValid(relid)) continue;
        
        Relation rel = RelationIdGetRelation(relid);
        if (!RelationIsValid(rel)) continue;
        
        StringInfoData buf;
        initStringInfo(&buf);
        decode_heap_operation(&buf, reader, rel, state->namespace_filter);
        
        if (buf.len > 0) {
            state->results[count] = pstrdup(buf.data);
            count++;
        }
        
        pfree(buf.data);
        relation_close(rel, NoLock);
    }
    
    state->result_count = count;
    XLogReaderFree(reader);
}

static XLogRecPtr
find_lsn_by_timestamp(int64 target_time_seconds)
{
    XLogReaderState *reader;
    char *errormsg = NULL;
    int64 target_time_us = target_time_seconds * 1000000LL;
    
    reader = XLogReaderAllocate(wal_segment_size, NULL,
                                XL_ROUTINE(.page_read = wal_page_read,
                                          .segment_open = NULL,
                                          .segment_close = NULL), NULL);
    
    if (!reader)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    
    XLogRecPtr earliest_lsn = GetRedoRecPtr();
    XLogBeginRead(reader, earliest_lsn);
    
    XLogRecord *first_record = XLogReadRecord(reader, &errormsg);
    if (!first_record) {
        XLogReaderFree(reader);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg("cannot read WAL record from LSN %X/%X", LSN_FORMAT_ARGS(earliest_lsn))));
    }
    
    TransactionId xid = XLogRecGetXid(reader);
    if (TransactionIdIsValid(xid)) {
        int64 first_time = get_commit_timestamp_micros(xid);
        if (first_time >= target_time_us) {
            XLogReaderFree(reader);
            return earliest_lsn;
        }
    }
    
    XLogRecPtr prev_lsn = reader->ReadRecPtr;
    
    while (true) {
        XLogRecord *record = XLogReadRecord(reader, &errormsg);
        if (!record) break;
        
        xid = XLogRecGetXid(reader);
        if (TransactionIdIsValid(xid)) {
            int64 record_time = get_commit_timestamp_micros(xid);
            
            /* Return prev_lsn when we've moved past the target second */
            if (record_time >= target_time_us + 1000000LL) {
                XLogReaderFree(reader);
                return prev_lsn;
            }
        }
        
        prev_lsn = reader->ReadRecPtr;
    }
    
    /* Reached end of WAL - return last LSN read (no more records) */
    XLogReaderFree(reader);
    return prev_lsn;
}

Datum
documentdb_decode_wal_direct(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    WalDecodeState *state;
    
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        XLogRecPtr start_lsn = PG_ARGISNULL(0) ? InvalidXLogRecPtr : PG_GETARG_LSN(0);
        int max_records = PG_ARGISNULL(1) ? 100 : PG_GETARG_INT32(1);
        char *namespace_filter = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));
        int64 start_at_operation_time = PG_ARGISNULL(3) ? 0 : PG_GETARG_INT64(3);
        
        if (!XLogRecPtrIsInvalid(start_lsn) && start_at_operation_time != 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("cannot specify both start_lsn and start_at_operation_time")));
        
        if (XLogRecPtrIsInvalid(start_lsn) && start_at_operation_time == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("must specify either start_lsn or start_at_operation_time")));
        
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("function returning record called in context that cannot accept type record")));
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        
        state = (WalDecodeState *)palloc0(sizeof(WalDecodeState));
        state->start_lsn = start_lsn;
        state->end_lsn = InvalidXLogRecPtr;
        state->last_scanned_lsn = InvalidXLogRecPtr;
        state->max_records = max_records;
        state->records_read = 0;
        state->result_count = 0;
        state->namespace_filter = namespace_filter ? pstrdup(namespace_filter) : NULL;
        state->results = (char **)palloc0(sizeof(char *) * max_records);
        
        if (start_at_operation_time != 0)
            state->start_lsn = find_lsn_by_timestamp(start_at_operation_time);
        
        decode_wal_range(state);
        
        funcctx->user_fctx = state;
        MemoryContextSwitchTo(oldcontext);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    state = (WalDecodeState *)funcctx->user_fctx;
    
    if (state->records_read >= state->result_count)
        SRF_RETURN_DONE(funcctx);
    
    Datum values[3];
    bool nulls[3] = {false, false, false};
    
    values[0] = CStringGetTextDatum(state->results[state->records_read]);
    values[1] = CStringGetTextDatum("decoded_event");
    
    /* Return last_scanned_lsn as third column */
    char lsn_str[32];
    snprintf(lsn_str, sizeof(lsn_str), "%X/%X", LSN_FORMAT_ARGS(state->last_scanned_lsn));
    values[2] = CStringGetTextDatum(lsn_str);
    
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    
    state->records_read++;
    
    SRF_RETURN_NEXT(funcctx, result);
}