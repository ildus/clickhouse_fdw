#include "postgres.h"

#include "funcapi.h"
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"

#include "clickhousedb_fdw.h"
#include "clickhouse_binary.hh"

typedef struct ch_convert_state ch_convert_state;
typedef Datum (*convert_func)(ch_convert_state *, Datum);
typedef struct ch_convert_state {
	Oid				intype;
	Oid				outtype;
	convert_func	func;

	// record
	TupleConversionMap	*tupmap;
	bool			pinned;
	CustomObjectDef	*cdef;
	TupleDesc		indesc;		/* for RECORD */
	TupleDesc		outdesc;	/* for RECORD */

	// array
	int16		typlen;
	bool		typbyval;
	char		typalign;

	// text
	Type		baseType;
	int32		typmod;

	// generic
	CoercionPathType	ctype;
	Oid					castfunc;
} ch_convert_state;


static Datum
convert_record(ch_convert_state *state, Datum val)
{
	HeapTuple		temptup;
	HeapTuple		htup;
	ch_binary_tuple_t	*slot = (ch_binary_tuple_t *) DatumGetPointer(val);

	htup = heap_form_tuple(state->indesc, slot->datums, slot->nulls);
	if (!state->outdesc)
	{
		val = heap_copy_tuple_as_datum(htup, state->indesc);

		if (state->cdef && state->cdef->rowfunc != InvalidOid)
		{
			/* there is convertor from row to outtype */
			val = OidFunctionCall1(state->cdef->rowfunc, val);
		}
		else if (state->outtype == TEXTOID)
		{
			/* a lot of allocations, not so efficient */
			val = CStringGetTextDatum(DatumGetCString(
						OidFunctionCall1(F_RECORD_OUT, val)));
		}
	}
	else
	{
		if (state->tupmap)
			temptup = execute_attr_map_tuple(htup, state->tupmap);
		else
			temptup = htup;

		val = heap_copy_tuple_as_datum(temptup, state->outdesc);
	}

	return val;
}

inline static Datum
convert_generic(ch_convert_state *state, Datum val)
{
	Assert(state->castfunc != InvalidOid);

	if (state->ctype == COERCION_PATH_FUNC)
		val = OidFunctionCall1(state->castfunc, val);

	return val;
}

static Datum
convert_array(ch_convert_state *state, Datum val)
{
	ch_binary_array_t	*slot = (ch_binary_array_t *) DatumGetPointer(val);

	if (slot->len == 0)
		val = PointerGetDatum(construct_empty_array(state->intype));
	else
	{
		void *arrout = construct_array(slot->datums, slot->len, slot->item_type,
			state->typlen, state->typbyval, state->typalign);

		val = PointerGetDatum(arrout);
	}

	return convert_generic(state, val);
}

static Datum
convert_remote_text(ch_convert_state *state, Datum val)
{
	val = stringTypeDatum(state->baseType, TextDatumGetCString(val), state->typmod);
	return val;
}

/*
 * We imply that corresponding type for UInt8 (bool in ClickHouse) is
 * SMALLINT and this function covers this case
 */
static Datum
convert_bool(ch_convert_state *state, Datum val)
{
	int16 dat = DatumGetInt16(val);
	return BoolGetDatum(dat);
}

static Datum
convert_date(ch_convert_state *state, Datum val)
{
	val = DirectFunctionCall1(timestamp_date, val);
	return convert_generic(state, val);
}

Datum
ch_binary_convert_datum(void *state, Datum val)
{
	return state ? ((ch_convert_state *) state)->func(state, val) : val;
}

void *
ch_binary_init_convert_state(Datum val, Oid intype, Oid outtype)
{
	ch_convert_state *state = palloc0(sizeof(ch_convert_state));

	state->intype = intype;
	state->outtype = outtype;
	state->cdef = chfdw_check_for_custom_type(outtype);
	state->typmod = -1;

	if (intype == RECORDOID)
	{
		ch_binary_tuple_t	*slot = (ch_binary_tuple_t *) DatumGetPointer(val);

		state->func = convert_record;

#if PG_VERSION_NUM < 120000
		state->indesc = CreateTemplateTupleDesc(slot->len, false);
#else
		state->indesc = CreateTemplateTupleDesc(slot->len);
#endif
		for (size_t i = 0; i < slot->len; ++i)
			TupleDescInitEntry(state->indesc, (AttrNumber) i + 1, "",
					slot->types[i], -1, 0);

		state->indesc = BlessTupleDesc(state->indesc);

		if (!(state->cdef || outtype == RECORDOID || outtype == TEXTOID))
		{
			TypeCacheEntry	   *typentry;
			HeapTuple			temptup;

			typentry = lookup_type_cache(outtype,
										 TYPECACHE_TUPDESC |
										 TYPECACHE_DOMAIN_BASE_INFO);

			if (typentry->typtype == TYPTYPE_DOMAIN)
				state->outdesc = lookup_rowtype_tupdesc_noerror(typentry->domainBaseType,
													  typentry->domainBaseTypmod,
													  false);
			else
			{
				if (typentry->tupDesc == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("type %s is not composite",
									format_type_be(outtype))));

				state->pinned = true;
				state->outdesc = typentry->tupDesc;
				PinTupleDesc(state->outdesc);
			}

			state->tupmap = convert_tuples_by_position(state->indesc, state->outdesc,
				"clickhouse_fdw: could not map tuple to returned type");
		}
	}
	else if (intype != outtype)
	{
		state->func = convert_generic;

		if (intype == DATEOID)
			state->func = convert_date;
		else if (intype == ANYARRAYOID)
		{
			ch_binary_array_t	*slot = (ch_binary_array_t *) DatumGetPointer(val);
			get_typlenbyvalalign(slot->item_type, &state->typlen, &state->typbyval,
					&state->typalign);

			/* restore intype */
			state->intype = slot->array_type;
			state->func = convert_array;
		}

		if (intype == TEXTOID)
		{
			Type		baseType;
			Oid			baseTypeId;

			baseTypeId = getBaseTypeAndTypmod(outtype, &state->typmod);
			if (baseTypeId != INTERVALOID)
				state->typmod = -1;

			state->baseType = typeidType(baseTypeId);
			state->func = convert_remote_text;
		}
		else if (outtype == BOOLOID && intype == INT2OID)
		{
			int16 val = DatumGetInt16(val);
			val = BoolGetDatum(val);
			state->func = convert_bool;
		}
		else
		{
			/* try to convert */
			state->ctype = find_coercion_pathway(outtype, intype,
										  COERCION_EXPLICIT,
										  &state->castfunc);
			switch (state->ctype)
			{
				case COERCION_PATH_FUNC:
					break;
				case COERCION_PATH_RELABELTYPE:
					/* if the conversion func was not previously set,
					 * then no conversion needed */
					if (state->func == NULL)
						goto no_conversion;

					/* all good */
					break;
				default:
					elog(ERROR, "clickhouse_fdw: could not cast value from %s to %s",
							format_type_be(intype), format_type_be(outtype));
			}
		}
	}
	else
	{
no_conversion:
		/* no conversion needed */
		pfree(state);
		return NULL;
	}

	return state;
}

void
ch_binary_free_convert_state(void *s)
{
	ch_convert_state *state = s;

	if (state->pinned)
		ReleaseTupleDesc(state->outdesc);

	if (state->baseType)
		ReleaseSysCache(state->baseType);

	pfree(state);
}
