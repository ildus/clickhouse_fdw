/*-------------------------------------------------------------------------
 *
 * clickhouse_fdw.h
 *		  Foreign-data wrapper for remote Clickhouse servers
 *
 * IDENTIFICATION
 *		  contrib/clickhouedb_fdw/clickhouse_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRES_FDW_H
#define POSTGRES_FDW_H

#include <clickhouse-client.h>

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"


/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * postgres_fdw foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 * postgresGetForeignJoinPaths creates it for a joinrel, and
 * postgresGetForeignUpperPaths creates it for an upperrel.
 */
typedef struct CHFdwRelationInfo
{
        /*
         * True means that the relation can be pushed down. Always true for simple
         * foreign scan.
         */
        bool		pushdown_safe;

        /*
         * Restriction clauses, divided into safe and unsafe to pushdown subsets.
         * All entries in these lists should have RestrictInfo wrappers; that
         * improves efficiency of selectivity and cost estimation.
         */
        List	   *remote_conds;
        List	   *local_conds;

        /* Actual remote restriction clauses for scan (sans RestrictInfos) */
        List	   *final_remote_exprs;

        /* Bitmap of attr numbers we need to fetch from the remote server. */
        Bitmapset  *attrs_used;

        /* Cost and selectivity of local_conds. */
        QualCost	local_conds_cost;
        Selectivity local_conds_sel;

        /* Selectivity of join conditions */
        Selectivity joinclause_sel;

        /* Estimated size and cost for a scan or join. */
        double		rows;
        int			width;
        Cost		startup_cost;
        Cost		total_cost;
        /* Costs excluding costs for transferring data from the foreign server */
        Cost		rel_startup_cost;
        Cost		rel_total_cost;

        /* Options extracted from catalogs. */
        bool		use_remote_estimate;
        Cost		fdw_startup_cost;
        Cost		fdw_tuple_cost;
        List	   *shippable_extensions;	/* OIDs of whitelisted extensions */

        /* Cached catalog information. */
        ForeignTable *table;
        ForeignServer *server;
        UserMapping *user;			/* only set in use_remote_estimate mode */

        int			fetch_size;		/* fetch size for this remote table */

        /*
         * Name of the relation while EXPLAINing ForeignScan. It is used for join
         * relations but is set for all relations. For join relation, the name
         * indicates which foreign tables are being joined and the join type used.
         */
        StringInfo	relation_name;

        /* Join information */
        RelOptInfo *outerrel;
        RelOptInfo *innerrel;
        JoinType	jointype;
        /* joinclauses contains only JOIN/ON conditions for an outer join */
        List	   *joinclauses;	/* List of RestrictInfo */

        /* Grouping information */
        List	   *grouped_tlist;

        /* Subquery information */
        bool		make_outerrel_subquery; /* do we deparse outerrel as a
                                         * subquery? */
        bool		make_innerrel_subquery; /* do we deparse innerrel as a
                                         * subquery? */
        Relids		lower_subquery_rels;	/* all relids appearing in lower
                                         * subqueries */

        /*
         * Index of the relation.  It is used to create an alias to a subquery
         * representing the relation.
         */
        int			relation_index;
} CHFdwRelationInfo;

/* in clickhouse_fdw.c */
extern int	set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);
extern ForeignServer *get_foreign_server(Relation rel);

/* in connection.c */
extern Conn *GetConnection(UserMapping *user, bool will_prep_stmt, bool read);
extern void ReleaseConnection(Conn *conn);
extern unsigned int GetCursorNumber(Conn *conn);
extern unsigned int GetPrepStmtNumber(Conn *conn);
extern void chfdw_exec_query(Conn *conn, const char *query);
extern void chfdw_report_error(int elevel, Conn *conn,
                               bool clear, const char *sql);

/* in option.c */
extern void
ExtractConnectionOptions(List *defelems, char **driver, char **host, int *port,
                         char **dbname, char **username, char **password);

extern List *ExtractExtensionList(const char *extensionsString,
                                  bool warnOnMissing);

/* in deparse.c */
extern void classifyConditions(PlannerInfo *root,
                               RelOptInfo *baserel,
                               List *input_conds,
                               List **remote_conds,
                               List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Expr *expr);
extern void deparseInsertSql(StringInfo buf, RangeTblEntry *rte,
                             Index rtindex, Relation rel,
                             List *targetAttrs, bool doNothing, List *returningList,
                             List **retrieved_attrs);
extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel,
                              List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);
extern Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root,
                                    RelOptInfo *foreignrel, List *tlist,
                                    List *remote_conds, List *pathkeys, bool is_subquery,
                                    List **retrieved_attrs, List **params_list);
extern const char *get_jointype_name(JoinType jointype);

/* in shippable.c */
extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, CHFdwRelationInfo *fpinfo);

/* libclickhouse_link.c */
typedef void *ch_connection;
typedef ch_connection (*connect_method)(ForeignServer *server, UserMapping *user);
typedef void (*disconnect_method)(ConnCacheEntry *entry);
typedef void (*check_conn_method)(const char *password, UserMapping *user);
typedef void (*simple_query_method)(ch_connection conn, const char *query);

typedef struct {
	connect_method		connect;
	disconnect_method	disconnect;
	check_conn_method	check_conn_params;
	simple_query_method	simple_query;
} libclickhouse_methods;

extern libclickhouse_methods *clickhouse_gate;

#endif							/* POSTGRES_FDW_H */
