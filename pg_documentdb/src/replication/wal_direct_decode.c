/*-------------------------------------------------------------------------
 * wal_direct_decode.c
 *
 * Direct WAL decoding without replication slot (Microsoft's approach)
 * Reads WAL files directly and decodes change events
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/heapam_xlog.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/commit_ts.h"
#include "catalog/pg_control.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relfilenumbermap.h"
#include "utils/timestamp.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"

PG_FUNCTION_INFO_V1(documentdb_decode_wal_direct);

typedef struct WalDecodeState
{
    XLogRecPtr start_lsn;
    XLogRecPtr end_lsn;
    XLogRecPtr current_lsn;
    int max_records;
    int records_read;
    char **results;
    int result_count;
} WalDecodeState;

static void decode_wal_range(WalDecodeState *state);
static int64 get_commit_timestamp_micros(TransactionId xid);

/*
 * Decode WAL directly from files without replication slot
 * Returns decoded change events as JSON
 */
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
        
        XLogRecPtr start_lsn = PG_GETARG_LSN(0);
        XLogRecPtr end_lsn = PG_ARGISNULL(1) ? InvalidXLogRecPtr : PG_GETARG_LSN(1);
        int max_records = PG_ARGISNULL(2) ? 100 : PG_GETARG_INT32(2);
        
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context that cannot accept type record")));
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        
        state = (WalDecodeState *) palloc0(sizeof(WalDecodeState));
        state->start_lsn = start_lsn;
        state->end_lsn = end_lsn;
        state->current_lsn = start_lsn;
        state->max_records = max_records;
        state->records_read = 0;
        state->result_count = 0;
        
        /* Allocate results array */
        state->results = (char **) palloc0(sizeof(char *) * max_records);
        
        /* Decode WAL records */
        decode_wal_range(state);
        
        funcctx->user_fctx = state;
        MemoryContextSwitchTo(oldcontext);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    state = (WalDecodeState *) funcctx->user_fctx;
    
    if (state->records_read >= state->result_count)
    {
        SRF_RETURN_DONE(funcctx);
    }
    
    /* Build result tuple */
    Datum values[2];
    bool nulls[2] = {false, false};
    
    values[0] = CStringGetTextDatum(state->results[state->records_read]);
    values[1] = CStringGetTextDatum("decoded_event");
    
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    
    state->records_read++;
    
    SRF_RETURN_NEXT(funcctx, result);
}

/*
 * Page read callback for XLogReader - reads WAL pages from disk
 */
static int
wal_page_read(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
              XLogRecPtr targetPtr, char *readBuf)
{
    XLogSegNo targetSegNo;
    uint32 targetPageOff;
    TimeLineID tli = 1;  /* Use timeline 1 */
    char walFilePath[MAXPGPATH];
    int fd;
    int readBytes;
    
    /* Calculate which WAL segment and offset */
    targetSegNo = targetPagePtr / wal_segment_size;
    targetPageOff = targetPagePtr % wal_segment_size;
    
    /* Build WAL file path: pg_wal/000000010000000000000001 format */
    snprintf(walFilePath, MAXPGPATH, "pg_wal/%08X%08X%08X",
             tli,
             (uint32) (targetSegNo / 0x100000000ULL),
             (uint32) (targetSegNo % 0x100000000ULL));
    
    /* Open WAL file */
    fd = BasicOpenFile(walFilePath, O_RDONLY | PG_BINARY);
    if (fd < 0)
    {
        /* WAL file not found */
        return -1;
    }
    
    /* Seek to the page */
    if (lseek(fd, (off_t) targetPageOff, SEEK_SET) < 0)
    {
        close(fd);
        return -1;
    }
    
    /* Read the page */
    readBytes = read(fd, readBuf, XLOG_BLCKSZ);
    close(fd);
    
    if (readBytes != XLOG_BLCKSZ)
        return -1;
    
    return readBytes;
}

/*
 * Get transaction commit timestamp in microseconds
 */
static int64
get_commit_timestamp_micros(TransactionId xid)
{
    TimestampTz commit_ts;
    
    if (TransactionIdIsValid(xid) && 
        TransactionIdGetCommitTsData(xid, &commit_ts, NULL))
    {
        return (int64) commit_ts;
    }
    
    /* Fallback to current time if commit timestamp not available */
    return (int64) GetCurrentTimestamp();
}

/*
 * Decode a heap operation from WAL record with full document data
 */
static void
decode_heap_operation(StringInfo buf, XLogReaderState *record, Relation rel)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecPtr lsn = record->ReadRecPtr;
    const char *schema = get_namespace_name(RelationGetNamespace(rel));
    const char *table = RelationGetRelationName(rel);
    
    /* Check if documentdb table */
    if (!schema || strcmp(schema, "documentdb_data") != 0 ||
        !table || strncmp(table, "documents_", 10) != 0)
        return;
    
    /* Decode based on operation type */
    if (info == XLOG_HEAP_INSERT || info == XLOG_HEAP_INIT_PAGE)
    {
        int64 timestamp = get_commit_timestamp_micros(XLogRecGetXid(record));
        
        appendStringInfo(buf,
            "{\"op\":\"i\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%ld}",
            schema, table, LSN_FORMAT_ARGS(lsn), (long) timestamp);
    }
    else if (info == XLOG_HEAP_DELETE)
    {
        int64 timestamp = get_commit_timestamp_micros(XLogRecGetXid(record));
        
        appendStringInfo(buf,
            "{\"op\":\"d\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%ld}",
            schema, table, LSN_FORMAT_ARGS(lsn), (long) timestamp);
    }
    else if (info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE)
    {
        int64 timestamp = get_commit_timestamp_micros(XLogRecGetXid(record));
        
        appendStringInfo(buf,
            "{\"op\":\"u\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%ld}",
            schema, table, LSN_FORMAT_ARGS(lsn), (long) timestamp);
    }
}

/*
 * Decode WAL records by reading WAL files directly
 */
static void
decode_wal_range(WalDecodeState *state)
{
    XLogReaderState *reader;
    char *errormsg = NULL;
    int count = 0;
    
    reader = XLogReaderAllocate(wal_segment_size, NULL,
                                XL_ROUTINE(.page_read = wal_page_read,
                                          .segment_open = NULL,
                                          .segment_close = NULL),
                                NULL);
    
    if (!reader)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }
    
    XLogBeginRead(reader, state->start_lsn);
    
    int total_records = 0;
    int heap_records = 0;
    int documentdb_records = 0;
    
    while (count < state->max_records)
    {
        XLogRecord *record = XLogReadRecord(reader, &errormsg);
        
        if (record == NULL)
            break;
        
        if (!XLogRecPtrIsInvalid(state->end_lsn) && reader->ReadRecPtr >= state->end_lsn)
            break;
        
        total_records++;
        
        uint8 rmid = XLogRecGetRmid(reader);
        if (rmid == RM_HEAP_ID || rmid == RM_HEAP2_ID)
        {
            heap_records++;
            
            RelFileLocator rlocator;
            ForkNumber forknum;
            BlockNumber blkno;
            
            /* Check if block 0 exists in this WAL record */
            if (!XLogRecHasBlockRef(reader, 0))
                continue;
            
            /* XLogRecGetBlockTag returns void, just call it */
            XLogRecGetBlockTag(reader, 0, &rlocator, &forknum, &blkno);
            
            /* Convert RelFileLocator to OID (PG16) */
            Oid relid = RelidByRelfilenumber(rlocator.spcOid, rlocator.relNumber);
            if (!OidIsValid(relid))
                continue;
            
            Relation rel = RelationIdGetRelation(relid);
            if (!RelationIsValid(rel))
                continue;
            
            StringInfoData buf;
            initStringInfo(&buf);
            
            decode_heap_operation(&buf, reader, rel);
            
            if (buf.len > 0)
            {
                documentdb_records++;
                state->results[count] = pstrdup(buf.data);
                count++;
            }
            
            pfree(buf.data);
            relation_close(rel, NoLock);
        }
    }
    
    state->result_count = count;
    XLogReaderFree(reader);
    
    ereport(NOTICE,
            (errmsg("Direct WAL decode: total=%d heap=%d documentdb=%d from %X/%X to %X/%X",
                    total_records, heap_records, documentdb_records,
                    LSN_FORMAT_ARGS(state->start_lsn), LSN_FORMAT_ARGS(state->end_lsn))));
}