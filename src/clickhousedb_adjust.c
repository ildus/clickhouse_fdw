#include "postgres.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/defrem.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "catalog/dependency.h"

#include "clickhousedb_fdw.h"

static HTAB *custom_objects_cache = NULL;

static HTAB *
create_custom_objects_cache(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(CustomObjectDef);

	return hash_create("clickhouse_fdw custom functions", 20, &ctl, HASH_ELEM);
}

CustomObjectDef *checkForCustomName(Oid funcid)
{
	const char *proname;

	CustomObjectDef	*entry;
	if (!custom_objects_cache)
		custom_objects_cache = create_custom_objects_cache();

	if (is_builtin(funcid))
		return NULL;

	entry = hash_search(custom_objects_cache, (void *) &funcid, HASH_FIND, NULL);
	if (!entry)
	{
		HeapTuple	proctup;
		Form_pg_proc procform;

		Oid extoid = getExtensionOfObject(ProcedureRelationId, funcid);
		char *extname = get_extension_name(extoid);

		entry = hash_search(custom_objects_cache, (void *) &funcid, HASH_ENTER, NULL);
		entry->cf_type = CF_USUAL;
		entry->cf_arg_type = CF_USUAL_ARG;

		if (extname && strcmp(extname, "istore") == 0)
		{
			proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
			if (!HeapTupleIsValid(proctup))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			procform = (Form_pg_proc) GETSTRUCT(proctup);

			proname = NameStr(procform->proname);
			if (strcmp(proname, "sum") == 0)
			{
				entry->cf_type = CF_ISTORE_SUM;
				strcpy(entry->custom_name, "sumMap");
			}

			ReleaseSysCache(proctup);
		}
	}

	return entry;
}

CustomObjectDef *checkForCustomType(Oid typeoid)
{
	const char *proname;

	CustomObjectDef	*entry;
	if (!custom_objects_cache)
		custom_objects_cache = create_custom_objects_cache();

	if (is_builtin(typeoid))
		return NULL;

	entry = hash_search(custom_objects_cache, (void *) &typeoid, HASH_FIND, NULL);
	if (!entry)
	{
		Oid extoid = getExtensionOfObject(TypeRelationId, typeoid);
		char *extname = get_extension_name(extoid);

		entry = hash_search(custom_objects_cache, (void *) &typeoid, HASH_ENTER, NULL);
		entry->cf_arg_type = CF_USUAL_ARG;

		if (extname && strcmp(extname, "istore") == 0)
		{
			entry->cf_type = InvalidOid;
			entry->cf_arg_type = CF_ISTORE_ARR;
		}
	}

	return entry;
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
void
ApplyCustomTableOptions(CHFdwRelationInfo *fpinfo)
{
	ListCell	*lc;
	foreach(lc, fpinfo->table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "engine") == 0)
		{
			char *val = defGetString(def);
			if (strcasecmp(val, "collapsingmergetree") == 0)
				fpinfo->ch_table_engine = CH_COLLAPSING_MERGE_TREE;
		}
	}
}
