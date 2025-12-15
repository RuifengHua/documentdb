/*-------------------------------------------------------------------------
 * src/replication/documentdb_decoder.c
 *
 * Logical decoding output plugin for DocumentDB changestreams.
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <replication/logical.h>
#include <replication/output_plugin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <access/heapam.h>
#include <access/htup_details.h>

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void decoder_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void decoder_shutdown(LogicalDecodingContext *ctx);
static void decoder_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void decoder_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr lsn);
static void decoder_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, ReorderBufferChange *change);
static void decode_insert(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple tuple);
static void decode_update(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple newtuple, HeapTuple oldtuple);
static void decode_delete(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple tuple);
static void output_json_header(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, const char *op);
static void output_document_key(LogicalDecodingContext *ctx, HeapTuple tuple, TupleDesc tupdesc);
static void output_document(LogicalDecodingContext *ctx, HeapTuple tuple, TupleDesc tupdesc, const char *field_name);
static void finalize_and_write(LogicalDecodingContext *ctx);

/*
 * Register callback functions that PostgreSQL will invoke at different points in the replication process
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	cb->startup_cb = decoder_startup;
	cb->begin_cb = decoder_begin;
	cb->change_cb = decoder_change;
	cb->commit_cb = decoder_commit;
	cb->shutdown_cb = decoder_shutdown;
}

/*
 * Tells PostgreSQL we'll output text (JSON strings), not binary during startup.
 */
static void
decoder_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void decoder_shutdown(LogicalDecodingContext *ctx) {}
static void decoder_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {}
static void decoder_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr lsn) {}

/*
 * Given a relation, return true if it is a DocumentDB table
 */
static bool
is_documentdb_table(Relation relation)
{
	const char *schema = get_namespace_name(RelationGetNamespace(relation));
	const char *table = RelationGetRelationName(relation);
	return schema && strcmp(schema, "documentdb_data") == 0 && table && strncmp(table, "documents_", 10) == 0;
}

/*
 * Decode a logical message and route to appropriate handler
 */
static void
decoder_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, ReorderBufferChange *change)
{
	HeapTuple tuple = NULL;

	if (!is_documentdb_table(relation))
		return;

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (change->data.tp.newtuple)
			{
				tuple = &change->data.tp.newtuple->tuple;
				decode_insert(ctx, txn, relation, tuple);
			}
			break;

		case REORDER_BUFFER_CHANGE_UPDATE:
			if (change->data.tp.newtuple)
			{
				HeapTuple newtuple = &change->data.tp.newtuple->tuple;
				HeapTuple oldtuple = change->data.tp.oldtuple ? &change->data.tp.oldtuple->tuple : NULL;
				decode_update(ctx, txn, relation, newtuple, oldtuple);
			}
			break;

		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				tuple = &change->data.tp.oldtuple->tuple;
				decode_delete(ctx, txn, relation, tuple);
			}
			break;

		default:
			break;
	}
}

static void
output_json_header(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, const char *op)
{
	// converts PostgreSQL's timestamp to Unix timestamp since PostgreSQL epoch is 2000-01-01
	// and Unix epoch is 1970-01-01, so add offset 946684800000000L
	int64 unix_time_us = txn->xact_time.commit_time + 946684800000000L;

	OutputPluginPrepareWrite(ctx, true);
	appendStringInfo(ctx->out, "{\"op\":\"%s\",\"ns\":\"%s.%s\",\"lsn\":\"%X/%X\",\"timestamp\":%lld",
					 op,
					 get_namespace_name(RelationGetNamespace(relation)),
					 RelationGetRelationName(relation),
					 LSN_FORMAT_ARGS(txn->final_lsn),
					 (long long)unix_time_us);
}

/*
 * Output documentKey field with hex-encoded BSON ObjectId from column 2
 */
static void
output_document_key(LogicalDecodingContext *ctx, HeapTuple tuple, TupleDesc tupdesc)
{
	bool isnull;
	/* Column 2 is object_id (BSON) */
	Datum object_id_datum = heap_getattr(tuple, 2, tupdesc, &isnull);
	if (!isnull)
	{
		bytea *object_id_bson = DatumGetByteaP(object_id_datum);
		int oid_len = VARSIZE_ANY_EXHDR(object_id_bson);
		char *oid_data = VARDATA_ANY(object_id_bson);

		appendStringInfoString(ctx->out, ",\"documentKey\":{\"_id\":\"");
		for (int i = 0; i < oid_len; i++)
			appendStringInfo(ctx->out, "%02x", (unsigned char)oid_data[i]);
		appendStringInfoString(ctx->out, "\"}");
	}
}

/*
 * Output document field with hex-encoded BSON from column 3
 * field_name can be "fullDocument" or "oldDocument"
 */
static void
output_document(LogicalDecodingContext *ctx, HeapTuple tuple, TupleDesc tupdesc, const char *field_name)
{
	bool isnull;
	/* Column 3 is document (BSON) */
	Datum document_datum = heap_getattr(tuple, 3, tupdesc, &isnull);
	if (!isnull)
	{
		bytea *document = DatumGetByteaP(document_datum);
		int doc_len = VARSIZE_ANY_EXHDR(document);
		char *doc_data = VARDATA_ANY(document);

		appendStringInfo(ctx->out, ",\"%s\":\"", field_name);
		for (int i = 0; i < doc_len; i++)
			appendStringInfo(ctx->out, "%02x", (unsigned char)doc_data[i]);
		appendStringInfoChar(ctx->out, '"');
	}
}

/*
 * Close JSON object and write output
 */
static void
finalize_and_write(LogicalDecodingContext *ctx)
{
	appendStringInfoChar(ctx->out, '}');
	OutputPluginWrite(ctx, true);
}

static void
decode_insert(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple tuple)
{
	TupleDesc tupdesc = RelationGetDescr(relation);

	output_json_header(ctx, txn, relation, "i");
	output_document_key(ctx, tuple, tupdesc);
	output_document(ctx, tuple, tupdesc, "fullDocument");
	finalize_and_write(ctx);
}

static void
decode_update(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple newtuple, HeapTuple oldtuple)
{
	TupleDesc tupdesc = RelationGetDescr(relation);

	output_json_header(ctx, txn, relation, "u");
	output_document_key(ctx, newtuple, tupdesc);
	if (oldtuple)
		output_document(ctx, oldtuple, tupdesc, "oldDocument");
	output_document(ctx, newtuple, tupdesc, "fullDocument");
	finalize_and_write(ctx);
}

static void
decode_delete(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, HeapTuple tuple)
{
	TupleDesc tupdesc = RelationGetDescr(relation);

	output_json_header(ctx, txn, relation, "d");
	output_document_key(ctx, tuple, tupdesc);
	finalize_and_write(ctx);
}
