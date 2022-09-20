#include "postgres.h"
#include "strings.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "clickhousedb_fdw.h"

// in PostgreSQL 14 source code those variables has other names,
// see postgres/src/backend/utils/fmgroids.h
#define F_TIMESTAMP_TRUNC 2020
#define F_TIMESTAMP_PART 2021
#define F_TIMESTAMPTZ_TRUNC 1217
#define F_TIMESTAMP_ZONE 2069
#define F_TIMESTAMPTZ_ZONE 1159
#define F_TIMESTAMPTZ_PART 1171
#define F_ARRAY_POSITION 3277
#define F_STRPOS 868
#define F_BTRIM 884
#define F_BTRIM1 885

static HTAB *custom_objects_cache = NULL;
static HTAB *custom_columns_cache = NULL;

static HTAB *
create_custom_objects_cache(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(CustomObjectDef);

	return hash_create("clickhouse_fdw custom functions", 20, &ctl, HASH_ELEM | HASH_BLOBS);
}

static void
invalidate_custom_columns_cache(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	CustomColumnInfo *entry;

	hash_seq_init(&status, custom_columns_cache);
	while ((entry = (CustomColumnInfo *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(custom_columns_cache,
						(void *) &entry->relid,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

static HTAB *
create_custom_columns_cache(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid) + sizeof(int);
	ctl.entrysize = sizeof(CustomColumnInfo);

	CacheRegisterSyscacheCallback(ATTNUM,
								  invalidate_custom_columns_cache,
								  (Datum) 0);

	return hash_create("clickhouse_fdw custom functions", 20, &ctl, HASH_ELEM | HASH_BLOBS);
}

inline static void
init_custom_entry(CustomObjectDef *entry)
{
	entry->cf_type = CF_USUAL;
	entry->custom_name[0] = '\0';
	entry->cf_context = NULL;
	entry->rowfunc = InvalidOid;
}

CustomObjectDef *chfdw_check_for_custom_function(Oid funcid)
{
	bool special_builtin = false;
	CustomObjectDef	*entry;

	if (chfdw_is_builtin(funcid))
	{
		switch (funcid)
		{
			case F_TIMESTAMP_TRUNC:
			case F_TIMESTAMPTZ_TRUNC:
			case F_TIMESTAMP_ZONE:
			case F_TIMESTAMPTZ_ZONE:
			case F_TIMESTAMP_PART:
			case F_TIMESTAMPTZ_PART:
			case F_ARRAY_POSITION:
			case F_STRPOS:
			case F_BTRIM:
			case F_BTRIM1:
				special_builtin = true;
				break;
			default:
				return NULL;
		}
	}

	if (!custom_objects_cache)
		custom_objects_cache = create_custom_objects_cache();

	entry = hash_search(custom_objects_cache, (void *) &funcid, HASH_FIND, NULL);
	if (!entry)
	{
		Oid			extoid;
		char	   *extname;
		char	   *proname;

		entry = hash_search(custom_objects_cache, (void *) &funcid, HASH_ENTER, NULL);
		entry->cf_oid = funcid;
		init_custom_entry(entry);

		switch (funcid)
		{
			case F_TIMESTAMPTZ_TRUNC:
			case F_TIMESTAMP_TRUNC:
			{
				entry->cf_type = CF_DATE_TRUNC;
				entry->custom_name[0] = '\1';
				break;
			}
			case F_TIMESTAMPTZ_PART:
			case F_TIMESTAMP_PART:
			{
				entry->cf_type = CF_DATE_PART;
				entry->custom_name[0] = '\1';
				break;
			}
			case F_TIMESTAMP_ZONE:
			case F_TIMESTAMPTZ_ZONE:
			{
				entry->cf_type = CF_TIMEZONE;
				strcpy(entry->custom_name, "toTimeZone");
				break;
			}
			case F_ARRAY_POSITION:
			{
				strcpy(entry->custom_name, "indexOf");
				break;
			}
			case F_BTRIM:
			case F_BTRIM1:
			{
				strcpy(entry->custom_name, "trimBoth");
				break;
			}
			case F_STRPOS:
			{
				strcpy(entry->custom_name, "position");
				break;
			}
		}

		if (special_builtin)
			return entry;

		extoid = getExtensionOfObject(ProcedureRelationId, funcid);
		extname = get_extension_name(extoid);
		if (extname)
		{
			HeapTuple	proctup;
			Form_pg_proc procform;
			char		*proname;

			proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
			if (!HeapTupleIsValid(proctup))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			procform = (Form_pg_proc) GETSTRUCT(proctup);
			proname = NameStr(procform->proname);

			if (strcmp(extname, "istore") == 0)
			{
				if (strcmp(NameStr(procform->proname), "sum") == 0)
				{
					entry->cf_type = CF_ISTORE_SUM;
					strcpy(entry->custom_name, "sumMap");
				}
				if (strcmp(NameStr(procform->proname), "sum_up") == 0)
				{
					entry->cf_type = CF_ISTORE_SUM_UP;
					strcpy(entry->custom_name, "arraySum");
				}
				else if (strcmp(NameStr(procform->proname), "istore_seed") == 0)
				{
					entry->cf_type = CF_ISTORE_SEED;
					entry->custom_name[0] = '\1';	/* complex */
				}
				else if (strcmp(NameStr(procform->proname), "accumulate") == 0)
				{
					entry->cf_type = CF_ISTORE_ACCUMULATE;
					entry->custom_name[0] = '\1';	/* complex */
				}
				else if (strcmp(NameStr(procform->proname), "slice") == 0)
				{
					entry->cf_type = CF_UNSHIPPABLE;
					entry->custom_name[0] = '\0';	/* complex */
				}
			}
			else if (strcmp(extname, "country") == 0)
			{
				if (strcmp(NameStr(procform->proname), "country_common_name") == 0)
				{
					entry->cf_type = CF_UNSHIPPABLE;
					entry->custom_name[0] = '\0';	/* complex */
				}
			}
			else if (strcmp(extname, "ajtime") == 0)
			{
				if (strcmp(NameStr(procform->proname), "ajtime_to_timestamp") == 0)
				{
					entry->cf_type = CF_AJTIME_TO_TIMESTAMP;
					strcpy(entry->custom_name, "");
				}
				else if (strcmp(NameStr(procform->proname), "ajtime_pl_interval") == 0)
				{
					entry->cf_type = CF_AJTIME_PL_INTERVAL;
					strcpy(entry->custom_name, "addSeconds");
				}
				else if (strcmp(NameStr(procform->proname), "ajtime_mi_interval") == 0)
				{
					entry->cf_type = CF_AJTIME_MI_INTERVAL;
					strcpy(entry->custom_name, "subtractSeconds");
				}
				else if (strcmp(NameStr(procform->proname), "day_diff") == 0)
				{
					entry->cf_type = CF_AJTIME_DAY_DIFF;
					strcpy(entry->custom_name, "toInt32");
				}
				else if (strcmp(NameStr(procform->proname), "ajdate") == 0)
				{
					entry->cf_type = CF_AJTIME_AJDATE;
					strcpy(entry->custom_name, "toDate");
				}
				else if (strcmp(NameStr(procform->proname), "ajtime_out") == 0)
				{
					entry->cf_type = CF_AJTIME_OUT;
				}
			}
			else if (strcmp(extname, "ajbool") == 0)
			{
				if (strcmp(NameStr(procform->proname), "ajbool_out") == 0)
					entry->cf_type = CF_AJBOOL_OUT;
			}
			else if (strcmp(extname, "intarray") == 0)
			{
				if (strcmp(NameStr(procform->proname), "idx") == 0)
				{
					entry->cf_type = CF_INTARRAY_IDX;
					strcpy(entry->custom_name, "indexOf");
				}
			}
			else if (strcmp(extname, "clickhouse_fdw") == 0)
			{
				entry->cf_type = CF_CH_FUNCTION;
				if (strcmp(proname, "argmax") == 0)
					strcpy(entry->custom_name, "argMax");
				else if (strcmp(proname, "argmin") == 0)
					strcpy(entry->custom_name, "argMin");
				else if (strcmp(proname, "dictget") == 0)
					strcpy(entry->custom_name, "dictGet");
				else if (strcmp(proname, "uniq_exact") == 0)
					strcpy(entry->custom_name, "uniqExact");
				else
					strcpy(entry->custom_name, proname);
			}
			ReleaseSysCache(proctup);
			pfree(extname);
		}
	}

	return entry;
}

static Oid
find_rowfunc(char *procname, Oid rettype)
{
	Oid		argtypes[1] = {RECORDOID};
	Oid		procOid;
	List   *funcname = NIL;

	funcname = list_make1(makeString(procname));
	procOid = LookupFuncName(funcname, 1, argtypes, false);
	if (!OidIsValid(procOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function %s does not exist",
						func_signature_string(funcname, 1, NIL, argtypes))));


	if (get_func_rettype(procOid) != rettype)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("typmod_in function %s must return type %s",
						procname, format_type_be(rettype))));

	list_free_deep(funcname);
	return procOid;
}

CustomObjectDef *chfdw_check_for_custom_type(Oid typeoid)
{
	const char *proname;

	CustomObjectDef	*entry;
	if (!custom_objects_cache)
		custom_objects_cache = create_custom_objects_cache();

	if (chfdw_is_builtin(typeoid))
		return NULL;

	entry = hash_search(custom_objects_cache, (void *) &typeoid, HASH_FIND, NULL);
	if (!entry)
	{
		HeapTuple	tp;

		entry = hash_search(custom_objects_cache, (void *) &typeoid, HASH_ENTER, NULL);
		init_custom_entry(entry);

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
		if (HeapTupleIsValid(tp))
		{
			Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
			char *name = NameStr(typtup->typname);
			if (strcmp(name, "istore") == 0)
			{
				entry->cf_type = CF_ISTORE_TYPE; /* bigistore or istore */
				strcpy(entry->custom_name, "Tuple(Array(Int32), Array(Int64))");
				entry->rowfunc = find_rowfunc("row_to_istore", typeoid);
			}
			else if (strcmp(name, "bigistore") == 0)
			{
				entry->cf_type = CF_ISTORE_TYPE; /* bigistore or istore */
				strcpy(entry->custom_name, "Tuple(Array(Int32), Array(Int64))");
				entry->rowfunc = find_rowfunc("row_to_bigistore", typeoid);
			}
			else if (strcmp(name, "ajtime") == 0)
			{
				entry->cf_type = CF_AJTIME_TYPE; /* ajtime */
				strcpy(entry->custom_name, "timestamp");
			}
			else if (strcmp(name, "country") == 0)
			{
				entry->cf_type = CF_COUNTRY_TYPE; /* country type */
				strcpy(entry->custom_name, "text");
			}
			ReleaseSysCache(tp);
		}
	}

	return entry;
}

CustomObjectDef *chfdw_check_for_custom_operator(Oid opoid, Form_pg_operator form)
{
	HeapTuple	tuple = NULL;
	const char *proname;

	CustomObjectDef	*entry;
	if (!custom_objects_cache)
		custom_objects_cache = create_custom_objects_cache();

	if (chfdw_is_builtin(opoid))
	{
		switch (opoid) {
			/* timestamptz + interval */
			case F_TIMESTAMPTZ_PL_INTERVAL:
				break;
			default:
				return NULL;
		}
	}

	if (!form)
	{
		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", opoid);
		form = (Form_pg_operator) GETSTRUCT(tuple);
	}

	entry = hash_search(custom_objects_cache, (void *) &opoid, HASH_FIND, NULL);
	if (!entry)
	{
		entry = hash_search(custom_objects_cache, (void *) &opoid, HASH_ENTER, NULL);
		init_custom_entry(entry);

		if (opoid == F_TIMESTAMPTZ_PL_INTERVAL)
			entry->cf_type = CF_TIMESTAMPTZ_PL_INTERVAL;
		else
		{
			Oid		extoid = getExtensionOfObject(OperatorRelationId, opoid);
			char   *extname = get_extension_name(extoid);

			if (extname)
			{
				if (strcmp(extname, "ajtime") == 0)
					entry->cf_type = CF_AJTIME_OPERATOR;
				else if (strcmp(extname, "istore") == 0)
				{
					if (form && strcmp(NameStr(form->oprname), "->") == 0)
						entry->cf_type = CF_ISTORE_FETCHVAL;
				}
				else if (strcmp(extname, "hstore") == 0)
				{
					if (form && strcmp(NameStr(form->oprname), "->") == 0)
						entry->cf_type = CF_HSTORE_FETCHVAL;
				}
				pfree(extname);
			}
		}
	}

	if (tuple)
		ReleaseSysCache(tuple);

	return entry;
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
void
chfdw_apply_custom_table_options(CHFdwRelationInfo *fpinfo, Oid relid)
{
	ListCell	*lc;
	TupleDesc	tupdesc;
	int			attnum;
	Relation	rel;
	List	   *options;

	foreach(lc, fpinfo->table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "engine") == 0)
		{
			static char *collapsing_text = "collapsingmergetree",
						*aggregating_text = "aggregatingmergetree";

			char *val = defGetString(def);
			if (strncasecmp(val, collapsing_text, strlen(collapsing_text)) == 0)
			{
				char   *start = index(val, '('),
					   *end = rindex(val, ')');

				fpinfo->ch_table_engine = CH_COLLAPSING_MERGE_TREE;
				if (start == end)
				{
					strcpy(fpinfo->ch_table_sign_field, "sign");
					continue;
				}

				if (end - start > NAMEDATALEN)
					elog(ERROR, "invalid format of ClickHouse engine");

				strncpy(fpinfo->ch_table_sign_field, start + 1, end - start - 1);
				fpinfo->ch_table_sign_field[end - start] = '\0';
			}
			else if (strncasecmp(val, aggregating_text, strlen(aggregating_text)) == 0)
			{
				fpinfo->ch_table_engine = CH_AGGREGATING_MERGE_TREE;
			}
		}
	}

	if (custom_columns_cache == NULL)
		custom_columns_cache = create_custom_columns_cache();

	rel = table_open_compat(relid, NoLock);
	tupdesc = RelationGetDescr(rel);

	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		bool				found;
		CustomObjectDef	   *cdef;
		CustomColumnInfo	entry_key,
						   *entry;
		custom_object_type	cf_type = CF_ISTORE_ARR;

		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
		entry_key.relid = relid;
		entry_key.varattno = attnum;

		entry = hash_search(custom_columns_cache,
				(void *) &entry_key.relid, HASH_ENTER, &found);
		if (found)
			continue;

		entry->relid = relid;
		entry->varattno = attnum;
		entry->table_engine = fpinfo->ch_table_engine;
		entry->coltype = CF_USUAL;
		entry->is_AggregateFunction = CF_AGGR_USUAL;
		strcpy(entry->colname, NameStr(attr->attname));
		strcpy(entry->signfield, fpinfo->ch_table_sign_field);

		/* If a column has the column_name FDW option, use that value */
		options = GetForeignColumnOptions(relid, attnum);
		foreach (lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0)
			{
				strncpy(entry->colname, defGetString(def), NAMEDATALEN);
				entry->colname[NAMEDATALEN - 1] = '\0';
			}
			else if (strcmp(def->defname, "aggregatefunction") == 0)
			{
				entry->is_AggregateFunction = CF_AGGR_FUNC;
				cf_type = CF_ISTORE_COL;
			}
			else if (strcmp(def->defname, "simpleaggregatefunction") == 0)
			{
				entry->is_AggregateFunction = CF_AGGR_SIMPLE;
				cf_type = CF_ISTORE_COL;
			}
			else if (strcmp(def->defname, "arrays") == 0)
				cf_type = CF_ISTORE_ARR;
		}

		cdef = chfdw_check_for_custom_type(attr->atttypid);
		if (cdef && cdef->cf_type == CF_ISTORE_TYPE)
			entry->coltype = cf_type;
	}
	table_close_compat(rel, NoLock);
}

/* Get foreign relation options */
CustomColumnInfo *
chfdw_get_custom_column_info(Oid relid, uint16 varattno)
{
	CustomColumnInfo	entry_key,
					   *entry;

	entry_key.relid = relid;
	entry_key.varattno = varattno;

	if (custom_columns_cache == NULL)
		custom_columns_cache = create_custom_columns_cache();

	entry = hash_search(custom_columns_cache,
			(void *) &entry_key.relid, HASH_FIND, NULL);

	return entry;
}
