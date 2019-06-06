#include "postgres.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "catalog/dependency.h"

#include "clickhousedb_fdw.h"

static HTAB *functions_cache = NULL;

static HTAB *
create_functions_cache(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(CustomFunctionDef);

	return hash_create("clickhouse_fdw custom functions", 20, &ctl, HASH_ELEM);
}

CustomFunctionDef *checkForCustomName(Oid funcid)
{
	const char *proname;

	CustomFunctionDef	*entry;
	if (!functions_cache)
		functions_cache = create_functions_cache();

	entry = hash_search(functions_cache, (void *) &funcid, HASH_FIND, NULL);
	if (!entry)
	{
		HeapTuple	proctup;
		Form_pg_proc procform;

		Oid extoid = getExtensionOfObject(ProcedureRelationId, funcid);
		char *extname = get_extension_name(extoid);

		entry = hash_search(functions_cache, (void *) &funcid, HASH_ENTER, NULL);

		if (strcmp(extname, "istore") == 0)
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

