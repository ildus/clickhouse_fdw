/*-------------------------------------------------------------------------
 *
 * clickhousedb_deparse.c
 *		  Query deparser for clickhousedb_fdw
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/clickhousedb_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/fmgroids.h"

#include "clickhousedb_fdw.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

typedef struct foreign_loc_cxt
{
	int	a;
} foreign_loc_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
	CustomObjectDef	*func;	/* custom function deparse */
	CHFdwRelationInfo *fpinfo;			/* fdw relation info */
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt);
static char *deparse_type_name(Oid type_oid, int32 typemod);

/*
 * Functions to construct string representation of a node tree.
 */
static void deparseTargetList(StringInfo buf,
				  RangeTblEntry *rte,
				  Index rtindex,
				  Relation rel,
				  bool is_returning,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs);
static void deparseExplicitTargetList(List *tlist,
						  bool is_returning,
						  List **retrieved_attrs,
						  deparse_expr_cxt *context);
static void deparseSubqueryTargetList(deparse_expr_cxt *context);
static void deparseReturningList(StringInfo buf, RangeTblEntry *rte,
					 Index rtindex, Relation rel,
					 bool trig_after_row,
					 List *returningList,
					 List **retrieved_attrs);
static void deparseColumnRef(StringInfo buf, CustomObjectDef *cdef,
	int varno, int varattno, RangeTblEntry *rte, bool qualify_col);
static void deparseRelation(StringInfo buf, Relation rel);
static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
static void deparseVar(Var *node, deparse_expr_cxt *context);
static void deparseConst(Const *node, deparse_expr_cxt *context, int showtype);
static void deparseParam(Param *node, deparse_expr_cxt *context);
static void deparseArrayRef(ArrayRef *node, deparse_expr_cxt *context);
static void deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context);
static void deparseOpExpr(OpExpr *node, deparse_expr_cxt *context);
static void deparseOperatorName(StringInfo buf, Form_pg_operator opform);
static void deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context);
static void deparseScalarArrayOpExpr(ScalarArrayOpExpr *node,
                                     deparse_expr_cxt *context);
static void deparseCaseExpr(CaseExpr *node, deparse_expr_cxt *context);
static void deparseCaseWhen(CaseWhen *node, deparse_expr_cxt *context);
static void deparseRelabelType(RelabelType *node, deparse_expr_cxt *context);
static void deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context);
static void deparseNullTest(NullTest *node, deparse_expr_cxt *context);
static void deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context);
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod,
				 deparse_expr_cxt *context);
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod,
					   deparse_expr_cxt *context);
static void deparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs,
				 deparse_expr_cxt *context);
static void deparseLockingClause(deparse_expr_cxt *context);
static void appendOrderByClause(List *pathkeys, deparse_expr_cxt *context);
static void appendConditions(List *exprs, deparse_expr_cxt *context);
static void deparseFromExprForRel(StringInfo buf, PlannerInfo *root,
					  RelOptInfo *foreignrel, bool use_alias,
					  Index ignore_rel, List **ignore_conds,
					  List **params_list);
static void deparseFromExpr(List *quals, deparse_expr_cxt *context);
static void deparseRangeTblRef(StringInfo buf, PlannerInfo *root,
				   RelOptInfo *foreignrel, bool make_subquery,
				   Index ignore_rel, List **ignore_conds, List **params_list);
static void deparseAggref(Aggref *node, deparse_expr_cxt *context);
static void appendGroupByClause(List *tlist, deparse_expr_cxt *context);
static void appendAggOrderBy(List *orderList, List *targetList,
				 deparse_expr_cxt *context);
static CustomObjectDef *appendFunctionName(Oid funcid, deparse_expr_cxt *context);
static Node *deparseSortGroupClause(Index ref, List *tlist, bool force_colno,
					   deparse_expr_cxt *context);
static void deparseCoalesceExpr(CoalesceExpr *node, deparse_expr_cxt *context);

/*
 * Helper functions
 */
static bool is_subquery_var(Var *node, RelOptInfo *foreignrel,
				int *relno, int *colno);
static void get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
							  int *relno, int *colno);


/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
classifyConditions(PlannerInfo *root,
				   RelOptInfo *baserel,
				   List *input_conds,
				   List **remote_conds,
				   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

		if (is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *)(baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/* 1: '=', 2: '<>', 0 - false */
int
is_equal_op(Oid opno)
{
	Form_pg_operator	operform;
	HeapTuple			opertup;
	int					res = 0;

	opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (!HeapTupleIsValid(opertup))
		elog(ERROR, "cache lookup failed for operator %u", opno);

	operform = (Form_pg_operator) GETSTRUCT(opertup);

	if (NameStr(operform->oprname)[0] == '=' && NameStr(operform->oprname)[1] == '\0')
		res = 1;
	else if (NameStr(operform->oprname)[0] == '<' && NameStr(operform->oprname)[1] == '>')
		res = 2;

	ReleaseSysCache(opertup);
	return res;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool
foreign_expr_walker(Node *node,
                    foreign_glob_cxt *glob_cxt,
                    foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	CHFdwRelationInfo *fpinfo;
	foreign_loc_cxt inner_cxt;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* May need server info from baserel's fdw_private struct */
	fpinfo = (CHFdwRelationInfo *)(glob_cxt->foreignrel->fdw_private);

	switch (nodeTag(node))
	{
	case T_Var:
	{
		Var		   *var = (Var *) node;

		/*
		 * If the Var is from the foreign table, we consider its
		 * collation (if any) safe to use.  If it is from another
		 * table, we treat its collation the same way as we would a
		 * Param's collation, ie it's not safe for it to have a
		 * non-default collation.
		 */
		if (bms_is_member(var->varno, glob_cxt->relids) &&
		        var->varlevelsup == 0)
		{
			/* Var belongs to foreign table */

			/*
			 * System columns other than ctid and oid should not be
			 * sent to the remote, since we don't make any effort to
			 * ensure that local and remote values match (tableoid, in
			 * particular, almost certainly doesn't match).
			 */
			if (var->varattno < 0 &&
			        var->varattno != SelfItemPointerAttributeNumber &&
			        var->varattno != ObjectIdAttributeNumber)
			{
				return false;
			}
		}
	}
	break;
	case T_Const:
	break;
	case T_Param:
	{
		/* We are not supporting param push down*/
		return false;
	}
	break;
	case T_ArrayRef:
	{
		ArrayRef   *ar = (ArrayRef *) node;

		/* Assignment should not be in restrictions. */
		if (ar->refassgnexpr != NULL)
		{
			return false;
		}

		/*
		 * Recurse to remaining subexpressions.  Since the array
		 * subscripts must yield (noncollatable) integers, they won't
		 * affect the inner_cxt state.
		 */
		if (!foreign_expr_walker((Node *) ar->refupperindexpr,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
		if (!foreign_expr_walker((Node *) ar->reflowerindexpr,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
		if (!foreign_expr_walker((Node *) ar->refexpr,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_FuncExpr:
	{
		FuncExpr   *fe = (FuncExpr *) node;

		/* not in ClickHouse */
		if (fe->funcvariadic)
			return false;

		/*
		 * If function used by the expression is not shippable, it
		 * can't be sent to remote because it might have incompatible
		 * semantics on remote side.
		 */
		if (!is_shippable(fe->funcid, ProcedureRelationId, fpinfo))
			return false;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) fe->args,
		                         glob_cxt, &inner_cxt))
			return false;
	}
	break;
	case T_OpExpr:
	case T_DistinctExpr:	/* struct-equivalent to OpExpr */
	{
		OpExpr	   *oe = (OpExpr *) node;

		/*
		 * Similarly, only shippable operators can be sent to remote.
		 * (If the operator is shippable, we assume its underlying
		 * function is too.)
		 */
		if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
			return false;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) oe->args,
		                         glob_cxt, &inner_cxt))
			return false;
	}
	break;
	case T_ScalarArrayOpExpr:
	{
		ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

		if (!is_equal_op(oe->opno))
			return false;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) oe->args,
								 glob_cxt, &inner_cxt))
			return false;
	}
	break;
	case T_RelabelType:
	{
		RelabelType *r = (RelabelType *) node;

		/*
		 * Recurse to input subexpression.
		 */
		if (!foreign_expr_walker((Node *) r->arg,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_BoolExpr:
	{
		BoolExpr   *b = (BoolExpr *) node;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) b->args,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_NullTest:
	{
		NullTest   *nt = (NullTest *) node;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) nt->arg,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_ArrayExpr:
	{
		ArrayExpr  *a = (ArrayExpr *) node;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!foreign_expr_walker((Node *) a->elements,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_List:
	{
		List	   *l = (List *) node;
		ListCell   *lc;

		/*
		 * Recurse to component subexpressions.
		 */
		foreach (lc, l)
		{
			if (!foreign_expr_walker((Node *) lfirst(lc),
			                         glob_cxt, &inner_cxt))
			{
				return false;
			}
		}

		/* Don't apply exprType() to the list. */
		check_type = false;
	}
	break;
	case T_Aggref:
	{
		Aggref	   *agg = (Aggref *) node;
		ListCell   *lc;

		/* Not safe to pushdown when not in grouping context */
		if (!IS_UPPER_REL(glob_cxt->foreignrel))
			return false;

		/* Only non-split aggregates are pushable. */
		if (agg->aggsplit != AGGSPLIT_SIMPLE)
			return false;

		/* As usual, it must be shippable. */
		if (!is_shippable(agg->aggfnoid, ProcedureRelationId, fpinfo))
			return false;

		/* Features that ClickHouse doesn't support */
		if (agg->aggorder)
			return false;

		if (agg->aggdistinct && agg->aggfilter)
			return false;

		if (agg->aggvariadic)
			return false;

		/*
		 * Recurse to input args. aggdirectargs, aggorder and
		 * aggdistinct are all present in args, so no need to check
		 * their shippability explicitly.
		 */
		foreach (lc, agg->args)
		{
			Node	   *n = (Node *) lfirst(lc);

			/* If TargetEntry, extract the expression from it */
			if (IsA(n, TargetEntry))
			{
				TargetEntry *tle = (TargetEntry *) n;

				n = (Node *) tle->expr;
			}

			if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
			{
				return false;
			}
		}

		/* Check aggregate filter */
		if (!foreign_expr_walker((Node *) agg->aggfilter,
		                         glob_cxt, &inner_cxt))
		{
			return false;
		}
	}
	break;
	case T_CaseExpr:
	{
		CaseExpr   *caseexpr = (CaseExpr *) node;
		ListCell   *lc;

		if (!foreign_expr_walker((Node *) caseexpr->arg, glob_cxt, &inner_cxt))
			return true;

		foreach(lc, caseexpr->args)
		{
			CaseWhen   *when = lfirst_node(CaseWhen, lc);

			if (!foreign_expr_walker((Node *) when->expr, glob_cxt, &inner_cxt))
				return false;
			if (!foreign_expr_walker((Node *) when->result, glob_cxt, &inner_cxt))
				return false;
		}
		if (!foreign_expr_walker((Node *) caseexpr->defresult, glob_cxt, &inner_cxt))
			return false;
	}
	break;
	case T_CoalesceExpr:
	{
		CoalesceExpr   *ce = (CoalesceExpr *) node;

		if (!foreign_expr_walker((Node *) ce->args, glob_cxt, &inner_cxt))
			return false;
	}
	break;
	default:

		/*
		 * If it's anything else, assume it's unsafe.  This list can be
		 * expanded later, but don't forget to add deparse support below.
		 */
		return false;
	}

	/*
	 * If result type of given expression is not shippable, it can't be sent
	 * to remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !is_shippable(exprType(node), TypeRelationId, fpinfo))
	{
		return false;
	}

	/* It looks OK */
	return true;
}

/*
 * Convert type OID + typmod info into a type name we can ship to the remote
 * server.  Someplace else had better have verified that this type name is
 * expected to be known on the remote end.
 *
 * This is almost just format_type_with_typemod(), except that if left to its
 * own devices, that function will make schema-qualification decisions based
 * on the local search_path, which is wrong.  We must schema-qualify all
 * type names that are not in pg_catalog.  We assume here that built-in types
 * are all in pg_catalog and need not be qualified; otherwise, qualify.
 */
static char *
deparse_type_name(Oid type_oid, int32 typemod)
{
	bits16		flags = FORMAT_TYPE_TYPEMOD_GIVEN;

	if (!is_builtin(type_oid))
		flags |= FORMAT_TYPE_FORCE_QUALIFY;

	return format_type_extended(type_oid, typemod, flags);
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contain expressions to be evaluated on
 * foreign server.
 */
List *
build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List	   *tlist = NIL;
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;
	ListCell   *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fpinfo->grouped_tlist;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}

/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
						List *tlist, List *remote_conds, List *pathkeys,
						bool is_subquery, List **retrieved_attrs,
						List **params_list)
{
	deparse_expr_cxt context;
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) rel->fdw_private;
	List	   *quals;

	elog(DEBUG2, "> %s:%d", __FUNCTION__, __LINE__);
	/*
	 * We handle relations for foreign tables, joins between those and upper
	 * relations.
	 */
	Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.scanrel = IS_UPPER_REL(rel) ? fpinfo->outerrel : rel;
	context.params_list = params_list;
	context.func = NULL;

	/* Construct SELECT clause */
	deparseSelectSql(tlist, is_subquery, retrieved_attrs, &context);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(rel))
	{
		CHFdwRelationInfo *ofpinfo;

		ofpinfo = (CHFdwRelationInfo *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	deparseFromExpr(quals, &context);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		appendGroupByClause(tlist, &context);

		/* Append HAVING clause */
		if (remote_conds)
		{
			appendStringInfoString(buf, " HAVING ");
			appendConditions(remote_conds, &context);
		}
	}

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		appendOrderByClause(pathkeys, &context);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void
deparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs,
				 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	PlannerInfo *root = context->root;
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;

	elog(DEBUG2, "> %s:%d", __FUNCTION__, __LINE__);
	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

	if (is_subquery)
	{
		/*
		 * For a relation that is deparsed as a subquery, emit expressions
		 * specified in the relation's reltarget.  Note that since this is for
		 * the subquery, no need to care about *retrieved_attrs.
		 */
		deparseSubqueryTargetList(context);
	}
	else if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		/*
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		deparseExplicitTargetList(tlist, false, retrieved_attrs, context);
	}
	else
	{
		/*
		 * For a base relation fpinfo->attrs_used gives the list of columns
		 * required to be fetched from the foreign server.
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = heap_open(rte->relid, NoLock);

		deparseTargetList(buf, rte, foreignrel->relid, rel, false,
						  fpinfo->attrs_used, false, retrieved_attrs);
		heap_close(rel, NoLock);
	}
	elog(DEBUG2, "< %s:%d", __FUNCTION__, __LINE__);
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void
deparseFromExpr(List *quals, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(!IS_UPPER_REL(context->foreignrel) ||
		   IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	deparseFromExprForRel(buf, context->root, scanrel,
						  (bms_num_members(scanrel->relids) > 1),
						  (Index) 0, NULL, context->params_list);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
		appendConditions(quals, context);
	}
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 *
 * If qualify_col is true, add relation alias before the column name.
 */
static void
deparseTargetList(StringInfo buf,
				  RangeTblEntry *rte,
				  Index rtindex,
				  Relation rel,
				  bool is_returning,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	if (is_returning)
		elog(ERROR, "clickhouse does not support RETURNING expresssions");

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		CustomObjectDef	*cdef;
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");

			first = false;

			cdef = checkForCustomType(attr->atttypid);
			deparseColumnRef(buf, cdef, rtindex, i, rte, qualify_col);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/*
	 * check for ctid and oid
	 */
	if (bms_is_member(SelfItemPointerAttributeNumber -
	                  FirstLowInvalidHeapAttributeNumber,
	                  attrs_used))
		elog(ERROR, "clickhouse does not support system columns");

	if (bms_is_member(ObjectIdAttributeNumber - FirstLowInvalidHeapAttributeNumber,
	                  attrs_used))
		elog(ERROR, "clickhouse does not support system columns");

	/* Don't generate bad syntax if no undropped columns */
	if (first && !is_returning)
	{
		appendStringInfoString(buf, "NULL");
	}
}

/*
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void
appendConditions(List *exprs, deparse_expr_cxt *context)
{
	int			nestlevel;
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	foreach (lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
		{
			expr = ((RestrictInfo *) expr)->clause;
		}

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
		{
			appendStringInfoString(buf, " AND ");
		}

		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

	reset_transmission_modes(nestlevel);
}

/* Output join name for given join type */
const char *
get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
	case JOIN_INNER:
		return "INNER";

	case JOIN_LEFT:
		return "LEFT";

	case JOIN_RIGHT:
		return "RIGHT";

	case JOIN_FULL:
		return "FULL";

	default:
		/* Shouldn't come here, but protect from buggy code. */
		elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 *
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 */
static void
deparseExplicitTargetList(List *tlist,
                          bool is_returning,
                          List **retrieved_attrs,
                          deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	elog(DEBUG2, "> %s:%d", __FUNCTION__, __LINE__);
	*retrieved_attrs = NIL;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (i > 0)
		{
			appendStringInfoString(buf, ", ");
		}
		else if (is_returning)
		{
			appendStringInfoString(buf, " RETURNING ");
		}

		deparseExpr((Expr *) tle->expr, context);

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0 && !is_returning)
	{
		appendStringInfoString(buf, "NULL");
	}
	elog(DEBUG2, "< %s:%d", __FUNCTION__, __LINE__);
}

/*
 * Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void
deparseSubqueryTargetList(deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	bool		first;
	ListCell   *lc;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	first = true;
	foreach(lc, foreignrel->reltarget->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);

		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		deparseExpr((Expr *) node, context);
	}

	/* Don't generate bad syntax if no expressions */
	if (first)
	{
		appendStringInfoString(buf, "NULL");
	}
}

/*
 * Construct FROM clause for given relation
 *
 * The function constructs ... JOIN ... ON ... for join relation. For a base
 * relation it just returns schema-qualified tablename, with the appropriate
 * alias if so requested.
 *
 * 'ignore_rel' is either zero or the RT index of a target relation.  In the
 * latter case the function constructs FROM clause of UPDATE or USING clause
 * of DELETE; it deparses the join relation as if the relation never contained
 * the target relation, and creates a List of conditions to be deparsed into
 * the top-level WHERE clause, which is returned to *ignore_conds.
 */
static void
deparseFromExprForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
                      bool use_alias, Index ignore_rel, List **ignore_conds,
                      List **params_list)
{
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;

	if (IS_JOIN_REL(foreignrel))
	{
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;
		RelOptInfo *outerrel = fpinfo->outerrel;
		RelOptInfo *innerrel = fpinfo->innerrel;
		bool		outerrel_is_target = false;
		bool		innerrel_is_target = false;

		if (ignore_rel > 0 && bms_is_member(ignore_rel, foreignrel->relids))
		{
			/*
			 * If this is an inner join, add joinclauses to *ignore_conds and
			 * set it to empty so that those can be deparsed into the WHERE
			 * clause.  Note that since the target relation can never be
			 * within the nullable side of an outer join, those could safely
			 * be pulled up into the WHERE clause (see foreign_join_ok()).
			 * Note also that since the target relation is only inner-joined
			 * to any other relation in the query, all conditions in the join
			 * tree mentioning the target relation could be deparsed into the
			 * WHERE clause by doing this recursively.
			 */
			if (fpinfo->jointype == JOIN_INNER)
			{
				*ignore_conds = list_concat(*ignore_conds,
				                            list_copy(fpinfo->joinclauses));
				fpinfo->joinclauses = NIL;
			}

			/*
			 * Check if either of the input relations is the target relation.
			 */
			if (outerrel->relid == ignore_rel)
			{
				outerrel_is_target = true;
			}
			else if (innerrel->relid == ignore_rel)
			{
				innerrel_is_target = true;
			}
		}

		/* Deparse outer relation if not the target relation. */
		if (!outerrel_is_target)
		{
			initStringInfo(&join_sql_o);
			deparseRangeTblRef(&join_sql_o, root, outerrel,
			                   fpinfo->make_outerrel_subquery,
			                   ignore_rel, ignore_conds, params_list);

			/*
			 * If inner relation is the target relation, skip deparsing it.
			 * Note that since the join of the target relation with any other
			 * relation in the query is an inner join and can never be within
			 * the nullable side of an outer join, the join could be
			 * interchanged with higher-level joins (cf. identity 1 on outer
			 * join reordering shown in src/backend/optimizer/README), which
			 * means it's safe to skip the target-relation deparsing here.
			 */
			if (innerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendStringInfo(buf, "%s", join_sql_o.data);
				return;
			}
		}

		/* Deparse inner relation if not the target relation. */
		if (!innerrel_is_target)
		{
			initStringInfo(&join_sql_i);
			deparseRangeTblRef(&join_sql_i, root, innerrel,
			                   fpinfo->make_innerrel_subquery,
			                   ignore_rel, ignore_conds, params_list);

			/*
			 * If outer relation is the target relation, skip deparsing it.
			 * See the above note about safety.
			 */
			if (outerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendStringInfo(buf, "%s", join_sql_i.data);
				return;
			}
		}

		/* Neither of the relations is the target relation. */
		Assert(!outerrel_is_target && !innerrel_is_target);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * ((outer relation) <join type> (inner relation) ON (joinclauses))
		 */
		appendStringInfo(buf, " %s ALL %s JOIN %s ON ", join_sql_o.data,
		                 get_jointype_name(fpinfo->jointype), join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.scanrel = foreignrel;
			context.root = root;
			context.params_list = params_list;
			context.func = NULL;

			appendStringInfoChar(buf, '(');
			appendConditions(fpinfo->joinclauses, &context);
			appendStringInfoChar(buf, ')');
		}
		else
		{
			appendStringInfoString(buf, "(TRUE)");
		}
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = heap_open(rte->relid, NoLock);

		deparseRelation(buf, rel);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
		{
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
		}

		heap_close(rel, NoLock);
	}
}

/*
 * Append FROM clause entry for the given relation into buf.
 */
static void
deparseRangeTblRef(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
                   bool make_subquery, Index ignore_rel, List **ignore_conds,
                   List **params_list)
{
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	Assert(fpinfo->local_conds == NIL);

	/* If make_subquery is true, deparse the relation as a subquery. */
	if (make_subquery)
	{
		List	   *retrieved_attrs;
		int			ncols;

		/*
		 * The given relation shouldn't contain the target relation, because
		 * this should only happen for input relations for a full join, and
		 * such relations can never contain an UPDATE/DELETE target.
		 */
		Assert(ignore_rel == 0 ||
		       !bms_is_member(ignore_rel, foreignrel->relids));

		/* Deparse the subquery representing the relation. */
		appendStringInfoChar(buf, '(');
		deparseSelectStmtForRel(buf, root, foreignrel, NIL,
		                        fpinfo->remote_conds, NIL, true,
		                        &retrieved_attrs, params_list);
		appendStringInfoChar(buf, ')');

		/* Append the relation alias. */
		appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX,
		                 fpinfo->relation_index);

		/*
		 * Append the column aliases if needed.  Note that the subquery emits
		 * expressions specified in the relation's reltarget (see
		 * deparseSubqueryTargetList).
		 */
		ncols = list_length(foreignrel->reltarget->exprs);
		if (ncols > 0)
		{
			int			i;

			appendStringInfoChar(buf, '(');
			for (i = 1; i <= ncols; i++)
			{
				if (i > 1)
				{
					appendStringInfoString(buf, ", ");
				}

				appendStringInfo(buf, "%s%d", SUBQUERY_COL_ALIAS_PREFIX, i);
			}
			appendStringInfoChar(buf, ')');
		}
	}
	else
		deparseFromExprForRel(buf, root, foreignrel, true, ignore_rel,
		                      ignore_conds, params_list);
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
deparseInsertSql(StringInfo buf, RangeTblEntry *rte,
                 Index rtindex, Relation rel,
                 List *targetAttrs, bool doNothing,
                 List *returningList, List **retrieved_attrs)
{
	bool    first;
	ListCell   *lc;

	appendStringInfoString(buf, "INSERT INTO ");
	deparseRelation(buf, rel);

	if (targetAttrs)
	{
		appendStringInfoChar(buf, '(');

		first = true;
		foreach (lc, targetAttrs)
		{
			int			attnum = lfirst_int(lc);

			if (!first)
			{
				appendStringInfoString(buf, ", ");
			}
			first = false;

			deparseColumnRef(buf, NULL, rtindex, attnum, rte, false);
		}
		appendStringInfoChar(buf, ')');
	}
}


/*
 * Construct SELECT statement to acquire size in blocks of given relation.
 *
 * Note: we use local definition of block size, not remote definition.
 * This is perhaps debatable.
 *
 * Note: pg_relation_size() exists in 8.1 and later.
 */
void
deparseAnalyzeSizeSql(StringInfo buf, Relation rel)
{
	StringInfoData relname;

	/* We'll need the remote relation name as a literal. */
	initStringInfo(&relname);
	deparseRelation(&relname, rel);
}

/*
 * Construct SELECT statement to acquire sample rows of given relation.
 *
 * SELECT command is appended to buf, and list of columns retrieved
 * is returned to *retrieved_attrs.
 */
void
deparseAnalyzeSql(StringInfo buf, Relation rel, List **retrieved_attrs)
{
	Oid			relid = RelationGetRelid(rel);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;
	char	   *colname;
	List	   *options;
	ListCell   *lc;
	bool		first = true;

	*retrieved_attrs = NIL;

	appendStringInfoString(buf, "SELECT ");
	for (i = 0; i < tupdesc->natts; i++)
	{
		/* Ignore dropped columns. */
		if (TupleDescAttr(tupdesc, i)->attisdropped)
		{
			continue;
		}

		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		/* Use attribute name or column_name option. */
		colname = NameStr(TupleDescAttr(tupdesc, i)->attname);
		options = GetForeignColumnOptions(relid, i + 1);

		foreach (lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0)
			{
				colname = defGetString(def);
				break;
			}
		}

		appendStringInfoString(buf, quote_identifier(colname));

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
	}

	/* Don't generate bad syntax for zero-column relation. */
	if (first)
	{
		appendStringInfoString(buf, "NULL");
	}

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	deparseRelation(buf, rel);
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 *
 * If qualify_col is true, qualify column name with the alias of relation.
 */
static void
deparseColumnRef(StringInfo buf, CustomObjectDef *cdef,
				 int varno, int varattno, RangeTblEntry *rte,
                 bool qualify_col)
{
	char	   *colname = NULL;
	CustomColumnInfo	*cinfo;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	if (varattno <= 0)
		elog(ERROR, "ClickHouse does not support system attributes");

	/* Get FDW specific options for this column */
	cinfo = GetCustomColumnInfo(rte->relid, varattno);
	if (cinfo)
		colname = cinfo->colname;

	/*
	 * If it's a column of a regular table or it doesn't have column_name
	 * FDW option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_attname(rte->relid, varattno, false);

	if (cinfo && cinfo->coltype == CF_ISTORE_ARR)
	{
		char *colkey = psprintf("%s_ids", colname);
		char *colval = psprintf("%s_values", colname);

		if (cdef && cdef->cf_type == CF_ISTORE_SUM)
		{
			/* SUM context */
			if (qualify_col)
				ADD_REL_QUALIFIER(buf, varno);
			appendStringInfoString(buf, quote_identifier(colkey));
			appendStringInfoChar(buf, ',');

			if (cinfo->table_engine == CH_COLLAPSING_MERGE_TREE)
			{
				appendStringInfoString(buf, "arrayMap(x -> x * ");
				appendStringInfoString(buf, cinfo->signfield);
				appendStringInfoChar(buf, ',');
				if (qualify_col)
					ADD_REL_QUALIFIER(buf, varno);
				appendStringInfoString(buf, quote_identifier(colval));
				appendStringInfoChar(buf, ')');
			}
			else
			{
				if (qualify_col)
					ADD_REL_QUALIFIER(buf, varno);
				appendStringInfoString(buf, quote_identifier(colval));
			}
		}
		else
		{
			appendStringInfoChar(buf, '(');
			if (qualify_col)
				ADD_REL_QUALIFIER(buf, varno);
			appendStringInfoString(buf, quote_identifier(colkey));
			appendStringInfoChar(buf, ',');
			if (qualify_col)
				ADD_REL_QUALIFIER(buf, varno);
			appendStringInfoString(buf, quote_identifier(colval));
			appendStringInfoChar(buf, ')');
		}

		pfree(colkey);
		pfree(colval);
	}
	else
	{
		if (qualify_col)
			ADD_REL_QUALIFIER(buf, varno);
		appendStringInfoString(buf, quote_identifier(colname));
	}
}

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
deparseRelation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	char       *driver;
	char       *host;
	int        port;
	char       *username;
	char       *password;
	char       *dbname;
	ForeignServer *server = get_foreign_server(rel);
	ListCell    *lc;

	ExtractConnectionOptions(server->options, &driver, &host, &port, &dbname,
	                         &username, &password);

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach (lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table_name") == 0)
		{
			relname = defGetString(def);
		}
	}
	if (relname == NULL)
	{
		relname = RelationGetRelationName(rel);
	}

	appendStringInfo(buf, "%s.%s", quote_identifier(dbname),
	                 quote_identifier(relname));
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
static void
deparseStringLiteral(StringInfo buf, const char *val, bool quote)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
	{
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
	}
	if (quote)
		appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
		{
			appendStringInfoChar(buf, ch);
		}
		appendStringInfoChar(buf, ch);
	}
	if (quote)
		appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
	{
		return;
	}

	switch (nodeTag(node))
	{
	case T_Var:
		deparseVar((Var *) node, context);
		break;
	case T_Const:
		deparseConst((Const *) node, context, 0);
		break;
	case T_Param:
		deparseParam((Param *) node, context);
		break;
	case T_ArrayRef:
		deparseArrayRef((ArrayRef *) node, context);
		break;
	case T_FuncExpr:
		deparseFuncExpr((FuncExpr *) node, context);
		break;
	case T_OpExpr:
		deparseOpExpr((OpExpr *) node, context);
		break;
	case T_DistinctExpr:
		deparseDistinctExpr((DistinctExpr *) node, context);
		break;
	case T_ScalarArrayOpExpr:
		deparseScalarArrayOpExpr((ScalarArrayOpExpr *) node, context);
		break;
	case T_RelabelType:
		deparseRelabelType((RelabelType *) node, context);
		break;
	case T_BoolExpr:
		deparseBoolExpr((BoolExpr *) node, context);
		break;
	case T_NullTest:
		deparseNullTest((NullTest *) node, context);
		break;
	case T_ArrayExpr:
		deparseArrayExpr((ArrayExpr *) node, context);
		break;
	case T_Aggref:
		deparseAggref((Aggref *) node, context);
		break;
	case T_CaseExpr:
		deparseCaseExpr((CaseExpr *) node, context);
		break;
	case T_CaseWhen:
		deparseCaseWhen((CaseWhen *) node, context);
		break;
	case T_CoalesceExpr:
		deparseCoalesceExpr((CoalesceExpr *) node, context);
		break;
	default:
		elog(ERROR, "unsupported expression type for deparse: %d",
		     (int) nodeTag(node));
		break;
	}
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
deparseVar(Var *node, deparse_expr_cxt *context)
{
	CustomObjectDef	*cdef;
	Relids		relids = context->scanrel->relids;
	int			relno;
	int			colno;

	/* Qualify columns when multiple relations are involved. */
	bool		qualify_col = (bms_num_members(relids) > 1);

	/*
	 * If the Var belongs to the foreign relation that is deparsed as a
	 * subquery, use the relation and column alias to the Var provided by the
	 * subquery, instead of the remote name.
	 */
	if (is_subquery_var(node, context->scanrel, &relno, &colno))
	{
		appendStringInfo(context->buf, "%s%d.%s%d",
		                 SUBQUERY_REL_ALIAS_PREFIX, relno,
		                 SUBQUERY_COL_ALIAS_PREFIX, colno);
		return;
	}

	cdef = context->func;
	if (!cdef)
		cdef = checkForCustomType(node->vartype);

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
		deparseColumnRef(context->buf, cdef,
						 node->varno, node->varattno,
						 planner_rt_fetch(node->varno, context->root),
		                 qualify_col);
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = 0;
			ListCell   *lc;

			/* find its index in params_list */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
				{
					break;
				}
			}
			if (lc == NULL)
			{
				/* not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}

			printRemoteParam(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			printRemotePlaceholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 * As for that function, showtype can be -1 to never show "::typename" decoration,
 * or +1 to always show it, or 0 to show it only if the constant wouldn't be assumed
 * to be the right type by default.
 */
static void
deparseConst(Const *node, deparse_expr_cxt *context, int showtype)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype,
	                  &typoutput, &typIsVarlena);

	extval = OidOutputFunctionCall(typoutput, node->constvalue);
	if (typoutput == F_ARRAY_OUT)
	{
		extval[0] = '[';
		extval[strlen(extval) - 1] = ']';
		deparseStringLiteral(buf, extval, false);
		return;
	}

	switch (node->consttype)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	{
		/*
		 * No need to quote unless it's a special value such as 'NaN'.
		 * See comments in get_const_expr().
		 */
		if (strspn(extval, "0123456789+-eE.") == strlen(extval))
		{
			if (extval[0] == '+' || extval[0] == '-')
			{
				appendStringInfo(buf, "(%s)", extval);
			}
			else
			{
				appendStringInfoString(buf, extval);
			}
		}
		else
		{
			appendStringInfo(buf, "'%s'", extval);
		}
	}
	break;
	case BITOID:
	case VARBITOID:
		appendStringInfo(buf, "B'%s'", extval);
		break;
	case BOOLOID:
		if (strcmp(extval, "t") == 0)
		{
			appendStringInfoString(buf, "true");
		}
		else
		{
			appendStringInfoString(buf, "false");
		}
		break;
	default:
		deparseStringLiteral(buf, extval, true);
		break;
	}
	pfree(extval);
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void
deparseParam(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int			pindex = 0;
		ListCell   *lc;

		/* find its index in params_list */
		foreach (lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
			{
				break;
			}
		}
		if (lc == NULL)
		{
			/* not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		printRemoteParam(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		printRemotePlaceholder(node->paramtype, node->paramtypmod, context);
	}
}

/*
 * Deparse an array subscript expression.
 */
static void
deparseArrayRef(ArrayRef *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lowlist_item;
	ListCell   *uplist_item;

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/*
	 * Deparse referenced array expression first.  If that expression includes
	 * a cast, we have to parenthesize to prevent the array subscript from
	 * being taken as typename decoration.  We can avoid that in the typical
	 * case of subscripting a Var, but otherwise do it.
	 */
	if (IsA(node->refexpr, Var))
	{
		deparseExpr(node->refexpr, context);
	}
	else
	{
		appendStringInfoChar(buf, '(');
		deparseExpr(node->refexpr, context);
		appendStringInfoChar(buf, ')');
	}

	/* Deparse subscript expressions. */
	lowlist_item = list_head(node->reflowerindexpr);	/* could be NULL */
	foreach (uplist_item, node->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');
		if (lowlist_item)
		{
			deparseExpr(lfirst(lowlist_item), context);
			appendStringInfoChar(buf, ':');
			lowlist_item = lnext(lowlist_item);
		}
		deparseExpr(lfirst(uplist_item), context);
		appendStringInfoChar(buf, ']');
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a function call.
 */
static void
deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first;
	ListCell   *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * If the function call came from a cast, then show the first argument
	 * plus an explicit cast operation.
	 */
	if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		int32		coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	appendFunctionName(node->funcid, context);
	appendStringInfoChar(buf, '(');

	/* ... and all the arguments */
	first = true;
	foreach (arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");

		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
deparseOpExpr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;
	CustomObjectDef	*cdef;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	}
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
	       (oprkind == 'l' && list_length(node->args) == 1) ||
	       (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	cdef = checkForCustomOperator(node->opno);
	if (cdef && cdef->cf_type == CF_AJTIME_OPERATOR)
		appendStringInfoString(buf, NameStr(form->oprname));
	else
		deparseOperatorName(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
deparseOperatorName(StringInfo buf, Form_pg_operator opform)
{
	char	   *opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)",
		                 quote_identifier(opnspname), opname);
	}
	else
	{
		if (strcmp(opname, "~~") == 0)
		{
			appendStringInfoString(buf, "LIKE");
		}
		else if (strcmp(opname, "~~*") == 0)
		{
			appendStringInfoString(buf, "LIKE");
		}
		else if (strcmp(opname, "!~~") == 0)
		{
			appendStringInfoString(buf, "NOT LIKE");
		}
		else if (strcmp(opname, "!~~*") == 0)
		{
			appendStringInfoString(buf, "NOT LIKE");
		}
		else
		{
			appendStringInfoString(buf, opname);
		}
	}
}

/*
 * Deparse IS DISTINCT FROM.
 */
static void
deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	deparseExpr(linitial(node->args), context);
	appendStringInfoString(buf, " IS DISTINCT FROM ");
	deparseExpr(lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Expr	   *arg1;
	Expr	   *arg2;

	/* Retrieve information about the operator from system catalog. */
	int			optype = is_equal_op(node->opno);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	if (node->useOr)
	{
		if (optype == 1)
			appendStringInfoString(buf, "has(");
		else
			appendStringInfoString(buf, "not has(");

		/* Deparse right operand. */
		arg2 = lsecond(node->args);
		deparseExpr(arg2, context);
		appendStringInfoChar(buf, ',');

		/* Deparse left operand. */
		arg1 = linitial(node->args);
		deparseExpr(arg1, context);

		/* Close function call */
		appendStringInfoChar(buf, ')');
	}
	else
	{
		appendStringInfoString(buf, "countEqual(");

		/* Deparse right operand. */
		arg2 = lsecond(node->args);
		deparseExpr(arg2, context);
		appendStringInfoChar(buf, ',');

		/* Deparse left operand. */
		arg1 = linitial(node->args);
		deparseExpr(arg1, context);

		/* Close function call */
		if (optype == 1)
		{
			appendStringInfoString(buf, ") = length(");

			/* Deparse right operand again */
			deparseExpr(arg2, context);
			appendStringInfoChar(buf, ')');
		} else {
			appendStringInfoString(buf, ") = 0");
		}
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
deparseRelabelType(RelabelType *node, deparse_expr_cxt *context)
{
	deparseExpr(node->arg, context);
}

/*
 * Deparse a BoolExpr node.
 */
static void
deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
	case AND_EXPR:
		op = "AND";
		break;
	case OR_EXPR:
		op = "OR";
		break;
	case NOT_EXPR:
		appendStringInfoString(buf, "(NOT ");
		deparseExpr(linitial(node->args), context);
		appendStringInfoChar(buf, ')');
		return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach (lc, node->args)
	{
		if (!first)
		{
			appendStringInfo(buf, " %s ", op);
		}
		deparseExpr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
deparseNullTest(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	deparseExpr(node->arg, context);

	/*
	 * For scalar inputs, we prefer to print as IS [NOT] NULL, which is
	 * shorter and traditional.  If it's a rowtype input but we're applying a
	 * scalar test, must print IS [NOT] DISTINCT FROM NULL to be semantically
	 * correct.
	 */
	if (node->argisrow || !type_is_rowtype(exprType((Node *) node->arg)))
	{
		if (node->nulltesttype == IS_NULL)
		{
			appendStringInfoString(buf, " IS NULL)");
		}
		else
		{
			appendStringInfoString(buf, " IS NOT NULL)");
		}
	}
	else
	{
		if (node->nulltesttype == IS_NULL)
		{
			appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL)");
		}
		else
		{
			appendStringInfoString(buf, " IS DISTINCT FROM NULL)");
		}
	}
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "array(");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');

	/* If the array is empty, we need an explicit cast to the array type. */
	if (node->elements == NIL)
		appendStringInfo(buf, "::%s",
						 deparse_type_name(node->array_typeid, -1));
}

/*
 * Deparse an Aggref node.
 */
static void
deparseAggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	CustomObjectDef	*cdef;
	CHFdwRelationInfo *fpinfo = context->scanrel->fdw_private;
	bool	aggfilter = false;
	bool	sign_count_filter = false;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Find aggregate name from aggfnoid which is a pg_proc entry */
	cdef = context->func;
	context->func = appendFunctionName(node->aggfnoid, context);

	/* 'If' part */
	if (context->func && context->func->cf_type == CF_SIGN_COUNT && !node->aggstar)
		sign_count_filter = true;

	if (node->aggfilter || sign_count_filter)
	{
		aggfilter = true;
		appendStringInfoString(buf, "If");
	}

	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	/* aggstar can be set only in zero-argument aggregates */
	if (node->aggstar)
	{
		if (context->func && context->func->cf_type == CF_SIGN_COUNT)
		{
			Assert(fpinfo && fpinfo->ch_table_engine == CH_COLLAPSING_MERGE_TREE);
			appendStringInfoString(buf, fpinfo->ch_table_sign_field);
		}
		else
			appendStringInfoChar(buf, '*');
	}
	else
	{
		ListCell   *arg;
		bool		first = true;
		bool		signMultiply = (context->func &&
						(context->func->cf_type == CF_SIGN_AVG ||
						 context->func->cf_type == CF_SIGN_SUM));

		/* Add all the arguments */
		if (sign_count_filter)
			/* in case if COUNT(col) we should get countIf(sign, col is not null) */
			appendStringInfoString(buf, fpinfo->ch_table_sign_field);
		else
		{
			/* default columns output */
			foreach (arg, node->args)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(arg);
				Node	   *n = (Node *) tle->expr;

				if (tle->resjunk)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");

				first = false;

				deparseExpr((Expr *) n, context);
			}

			if (signMultiply)
			{
				Assert(fpinfo->ch_table_engine == CH_COLLAPSING_MERGE_TREE);
				appendStringInfoString(buf, " * ");
				appendStringInfoString(buf, fpinfo->ch_table_sign_field);
			}
		}
	}

	/* Add 'If' part condition */
	if (aggfilter)
	{
		appendStringInfoChar(buf, ',');

		if (node->aggfilter)
			deparseExpr((Expr *) node->aggfilter, context);

		if (sign_count_filter)
		{
			if (node->aggfilter)
				appendStringInfoString(buf, " AND ");

			appendStringInfoChar(buf, '(');
			deparseExpr((Expr *) ((TargetEntry *) linitial(node->args))->expr, context);
			appendStringInfoString(buf, ") IS NOT NULL");
		}
	}

	appendStringInfoChar(buf, ')');

	/* AVG stuff */
	if (context->func && context->func->cf_type == CF_SIGN_AVG)
	{
		appendStringInfoString(buf, " / sumIf(");
		appendStringInfoString(buf, fpinfo->ch_table_sign_field);
		appendStringInfoChar(buf, ',');
		if (node->aggfilter)
		{
			deparseExpr((Expr *) node->aggfilter, context);
			appendStringInfoString(buf, " AND ");
		}
		deparseExpr((Expr *) ((TargetEntry *) linitial(node->args))->expr, context);
		appendStringInfoString(buf, " IS NOT NULL");
		appendStringInfoChar(buf, ')');
	}

	/* original */
	context->func = cdef;
}

static void
deparseCaseExpr(CaseExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;

	appendStringInfoString(buf, "CASE");
	if (node->arg)
	{
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(lc), context);
	}
	foreach(lc, node->args)
	{
		deparseExpr(lfirst(lc), context);
	}
	if (node->defresult)
	{
		appendStringInfoString(buf, " ELSE ");
		deparseExpr(node->defresult, context);
	}
	appendStringInfoString(buf, " END");
}

static void
deparseCaseWhen(CaseWhen *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;

	appendStringInfoString(buf, " WHEN ");
	deparseExpr(node->expr, context);
	appendStringInfoString(buf, " THEN ");
	deparseExpr(node->result, context);
}

static void
deparseCoalesceExpr(CoalesceExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first;

	appendStringInfoString(buf, "COALESCE(");

	first = true;
	foreach (lc, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");

		first = false;

		deparseExpr(lfirst(lc), context);
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Append ORDER BY within aggregate function.
 */
static void
appendAggOrderBy(List *orderList, List *targetList, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first = true;

	foreach (lc, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
		Node	   *sortexpr;
		Oid			sortcoltype;
		TypeCacheEntry *typentry;

		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		sortexpr = deparseSortGroupClause(srt->tleSortGroupRef, targetList,
		                                  false, context);
		sortcoltype = exprType(sortexpr);
		/* See whether operator is default < or > for datatype */
		typentry = lookup_type_cache(sortcoltype,
		                             TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
		if (srt->sortop == typentry->lt_opr)
		{
			appendStringInfoString(buf, " ASC");
		}
		else
		{
			appendStringInfoString(buf, " DESC");
		}
	}
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod,
                 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	appendStringInfo(buf, "?");
}

/*
 * Print the representation of a placeholder for a parameter that will be
 * sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query.
 * We don't have the actual value of the runtime parameter yet, and we don't
 * want the remote planner to generate a plan that depends on such a value
 * anyway.  Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of Postgres, the planner will see that as an unknown
 * constant value, which is what we want.  This might need adjustment if we
 * ever make the planner flatten scalar subqueries.  Note: the reason for the
 * apparently useless outer cast is to ensure that the representation as a
 * whole will be parsed as an a_expr and not a select_with_parens; the latter
 * would do the wrong thing in the context "x = ANY(...)".
 */
static void
printRemotePlaceholder(Oid paramtype, int32 paramtypmod,
                       deparse_expr_cxt *context)
{
}

/*
 * Deparse GROUP BY clause.
 */
static void
appendGroupByClause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = context->root->parse;
	ListCell   *lc;
	bool		first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
	{
		return;
	}

	appendStringInfoString(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		deparseSortGroupClause(grp->tleSortGroupRef, tlist, true, context);
	}
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void
appendOrderByClause(List *pathkeys, deparse_expr_cxt *context)
{
	ListCell   *lcell;
	int			nestlevel;
	char	   *delim = " ";
	RelOptInfo *baserel = context->scanrel;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	appendStringInfoString(buf, " ORDER BY");
	foreach (lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		Expr	   *em_expr;

		em_expr = find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		deparseExpr(em_expr, context);
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		delim = ", ";
	}
	reset_transmission_modes(nestlevel);
}

/*
 * appendFunctionName
 *		Deparses function name from given function oid.
 *		Returns was custom or not.
 */
static CustomObjectDef *
appendFunctionName(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	CustomObjectDef	*cdef;
	CHFdwRelationInfo *fpinfo = context->scanrel->fdw_private;

	cdef = checkForCustomFunction(funcid);
	if (cdef && cdef->custom_name[0] != '\0')
	{
		appendStringInfoString(buf, quote_identifier(cdef->custom_name));
		return cdef;
	}

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	procform = (Form_pg_proc) GETSTRUCT(proctup);
	proname = NameStr(procform->proname);

	/* we have some additional conditions on aggregation functions */
	if (is_builtin(funcid) && procform->prokind == PROKIND_AGGREGATE
			&& fpinfo->ch_table_engine == CH_COLLAPSING_MERGE_TREE)
	{
		cdef = palloc(sizeof(CHFdwRelationInfo));
		cdef->cf_oid = funcid;

		if (strcmp(proname, "sum") == 0)
			cdef->cf_type = CF_SIGN_SUM;
		else if (strcmp(proname, "avg") == 0)
		{
			cdef->cf_type = CF_SIGN_AVG;;
			proname = "sum";
		}
		else if (strcmp(proname, "count") == 0)
		{
			cdef->cf_type = CF_SIGN_COUNT;
			proname = "sum";
		}
		else
		{
			pfree(cdef);
			cdef = NULL;
		}
	}

	/* Always print the function name */
	appendStringInfoString(buf, quote_identifier(proname));

	ReleaseSysCache(proctup);

	return cdef;
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node *
deparseSortGroupClause(Index ref, List *tlist, bool force_colno,
                       deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		deparseConst((Const *) expr, context, 1);
	}
	else if (!expr || IsA(expr, Var))
	{
		deparseExpr(expr, context);
	}
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}


/*
 * Returns true if given Var is deparsed as a subquery output column, in
 * which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool
is_subquery_var(Var *node, RelOptInfo *foreignrel, int *relno, int *colno)
{
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;
	RelOptInfo *outerrel = fpinfo->outerrel;
	RelOptInfo *innerrel = fpinfo->innerrel;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	/*
	 * If the given relation isn't a join relation, it doesn't have any lower
	 * subqueries, so the Var isn't a subquery output column.
	 */
	if (!IS_JOIN_REL(foreignrel))
	{
		return false;
	}

	/*
	 * If the Var doesn't belong to any lower subqueries, it isn't a subquery
	 * output column.
	 */
	if (!bms_is_member(node->varno, fpinfo->lower_subquery_rels))
	{
		return false;
	}

	if (bms_is_member(node->varno, outerrel->relids))
	{
		/*
		 * If outer relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_outerrel_subquery)
		{
			get_relation_column_alias_ids(node, outerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the outer relation. */
		return is_subquery_var(node, outerrel, relno, colno);
	}
	else
	{
		Assert(bms_is_member(node->varno, innerrel->relids));

		/*
		 * If inner relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_innerrel_subquery)
		{
			get_relation_column_alias_ids(node, innerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the inner relation. */
		return is_subquery_var(node, innerrel, relno, colno);
	}
}

/*
 * Get the IDs for the relation and column alias to given Var belonging to
 * given relation, which are returned into *relno and *colno.
 */
static void
get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
                              int *relno, int *colno)
{
	CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) foreignrel->fdw_private;
	int			i;
	ListCell   *lc;

	/* Get the relation alias ID */
	*relno = fpinfo->relation_index;

	/* Get the column alias ID */
	i = 1;
	foreach (lc, foreignrel->reltarget->exprs)
	{
		if (equal(lfirst(lc), (Node *) node))
		{
			*colno = i;
			return;
		}
		i++;
	}

	/* Shouldn't get here */
	elog(ERROR, "unexpected expression in subquery output");
}
