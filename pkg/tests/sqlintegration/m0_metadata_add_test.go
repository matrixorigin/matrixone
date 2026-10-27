//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func openM0SQL(t *testing.T, cn embed.ServiceOperator) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf(
		"dump:111@tcp(127.0.0.1:%d)/",
		cn.GetServiceConfig().CN.Frontend.Port,
	))
	require.NoError(t, err)
	return db
}

func m0Exec(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func m0Count(t *testing.T, ctx context.Context, db *sql.DB, statement string) int {
	t.Helper()
	var count int
	require.NoErrorf(t, db.QueryRowContext(ctx, statement).Scan(&count), "query failed: %s", statement)
	return count
}

func m0Columns(t *testing.T, ctx context.Context, db *sql.DB, statement string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	return columns
}

func m0TableGeneration(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) (uint64, uint32) {
	t.Helper()
	var id uint64
	var version uint32
	require.NoError(t, db.QueryRowContext(ctx,
		"select rel_id,rel_version from mo_catalog.mo_tables where reldatabase=? and relname=?",
		schema, table).Scan(&id, &version))
	return id, version
}

func m0StoredColumnLayout(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select attname,attnum,attr_seqnum,att_is_hidden from mo_catalog.mo_columns "+
			"where att_database=? and att_relname=? order by attnum", schema, table)
	require.NoError(t, err)
	defer rows.Close()
	var stored []string
	for rows.Next() {
		var name string
		var attnum int
		var seqnum uint16
		var hidden int8
		require.NoError(t, rows.Scan(&name, &attnum, &seqnum, &hidden))
		stored = append(stored, fmt.Sprintf("%s:%d:%d:%d", name, attnum, seqnum, hidden))
	}
	require.NoError(t, rows.Err())
	return stored
}

func m0TableDefNullability(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	schema, table string,
) map[string]bool {
	t.Helper()
	service := cn.RawService().(cnservice.Service)
	ctx = defines.AttachAccountId(ctx, 0)
	nullability := make(map[string]bool)
	require.NoError(t, service.GetSQLExecutor().ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		database, err := service.GetEngine().Database(ctx, schema, txn.Txn())
		if err != nil {
			return err
		}
		relation, err := database.Relation(ctx, table, nil)
		if err != nil {
			return err
		}
		tableDef := relation.CopyTableDef(ctx)
		for _, column := range tableDef.Cols {
			if column.Hidden {
				continue
			}
			nullability[column.Name] = column.Default != nil && column.Default.NullAbility
		}
		return nil
	}, executor.Options{}.WithWaitCommittedLogApplied()))
	return nullability
}

func m0ServiceOfType(
	t *testing.T,
	cluster embed.Cluster,
	serviceType metadata.ServiceType,
) embed.ServiceOperator {
	t.Helper()
	var service embed.ServiceOperator
	cluster.ForeachServices(func(candidate embed.ServiceOperator) bool {
		if candidate.ServiceType() == serviceType {
			service = candidate
			return false
		}
		return true
	})
	require.NotNil(t, service, serviceType.String())
	return service
}

func m0ObjectFingerprints(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select object_name,obj_loc,rows_cnt,origin_size,compress_size,"+
			"bit_cast(`min` as int),bit_cast(`max` as int),bit_cast(`sum` as bigint) "+
			"from metadata_scan(?, 'c_id') g order by object_name", table)
	require.NoError(t, err)
	defer rows.Close()
	var fingerprints []string
	for rows.Next() {
		var objectName, objectLocation string
		var rowCount, originSize, compressSize int64
		var minimum, maximum, sum sql.NullInt64
		require.NoError(t, rows.Scan(
			&objectName, &objectLocation, &rowCount, &originSize, &compressSize,
			&minimum, &maximum, &sum,
		))
		fingerprints = append(fingerprints, fmt.Sprintf(
			"%s|%s|%d|%d|%d|%v|%v|%v",
			objectName, objectLocation, rowCount, originSize, compressSize,
			minimum, maximum, sum,
		))
	}
	require.NoError(t, rows.Err())
	return fingerprints
}

func m0CustomerEvaluation(t *testing.T, ctx context.Context, db *sql.DB, table, tierCol, creditCol string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select "+tierCol+",count(*),avg("+creditCol+") from "+table+
			" group by "+tierCol+" order by "+tierCol)
	require.NoError(t, err)
	defer rows.Close()
	var evaluation []string
	for rows.Next() {
		var tier string
		var count int
		var average float64
		require.NoError(t, rows.Scan(&tier, &count, &average))
		evaluation = append(evaluation, fmt.Sprintf("%s:%d:%.2f", tier, count, average))
	}
	require.NoError(t, rows.Err())
	return evaluation
}

func m0DataMergeState(t *testing.T, ctx context.Context, db *sql.DB, table string) (completed, pending int) {
	t.Helper()
	var payload string
	require.NoError(t, db.QueryRowContext(ctx,
		"select mo_ctl('dn','inspect',?)", "merge show -t "+table).Scan(&payload))
	for _, line := range strings.Split(payload, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "total data merge tasks:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "total data merge tasks:"))
			var err error
			completed, err = strconv.Atoi(value)
			require.NoError(t, err)
		case strings.HasPrefix(line, "merge tasks in queue:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "merge tasks in queue:"))
			var err error
			pending, err = strconv.Atoi(value)
			require.NoError(t, err)
		}
	}
	return completed, pending
}

func m0RunL0Merge(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	before, _ := m0DataMergeState(t, ctx, db, table)
	_, err := db.ExecContext(ctx,
		"select mo_ctl('dn','inspect',?)",
		"merge trigger -t "+table+" --kind l0 --l0-oneshot")
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			completed, pending := m0DataMergeState(t, ctx, db, table)
			require.FailNowf(t, "L0 merge did not complete",
				"completed=%d pending=%d before=%d: %v", completed, pending, before, waitCtx.Err())
		case <-ticker.C:
			completed, pending := m0DataMergeState(t, waitCtx, db, table)
			if completed > before && pending == 0 {
				return
			}
		}
	}
}

func TestM0MetadataOnlyAddColumnSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer func() { _ = db.Close() }()

		const (
			schema    = "m0_metadata_add"
			customer  = schema + ".customer"
			clone     = schema + ".customer_clone"
			history   = schema + ".customer_history"
			branch    = schema + ".customer_branch"
			snapshot  = "m0_metadata_add_r1"
			tierCol   = "loyalty_tier_t0_s0"
			creditCol = "credit_lim_t0_s0"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists "+snapshot,
			"drop database if exists "+schema,
		)

		m0Exec(t, ctx, db, "create database "+schema)
		t.Run("partition_copy", func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				algorithm string
			}{
				{"partition_default", ""},
				{"partition_explicit_copy", ", algorithm=copy"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					table := schema + "." + tc.name
					m0Exec(t, ctx, db, "create table "+table+
						"(a int not null,b int not null,primary key(a,b)) partition by hash(a) partitions 2")
					m0Exec(t, ctx, db, "insert into "+table+" values(1,1)")
					beforeID, beforeVersion := m0TableGeneration(t, ctx, db, schema, tc.name)
					require.Zero(t, beforeVersion)

					// Compare routing IDs with the actual backing relations, not
					// just names or a successful SELECT of the pre-ALTER rows.
					partitionIDs := func(rootID uint64) map[string]uint64 {
						rows, err := db.QueryContext(ctx,
							"select p.partition_name,p.partition_table_name,p.partition_id,t.rel_id "+
								"from mo_catalog.mo_partition_tables p left join mo_catalog.mo_tables t "+
								"on t.reldatabase=? and t.relname=p.partition_table_name "+
								"where p.primary_table_id=?", schema, rootID)
						require.NoError(t, err)
						defer rows.Close()
						ids := make(map[string]uint64)
						for rows.Next() {
							var logicalName, name string
							var routingID, actualID uint64
							require.NoError(t, rows.Scan(&logicalName, &name, &routingID, &actualID))
							require.Equal(t, actualID, routingID, "stale partition routing: %s", name)
							ids[logicalName] = actualID
						}
						require.NoError(t, rows.Err())
						require.Len(t, ids, 2)
						return ids
					}
					beforeChildren := partitionIDs(beforeID)
					m0Exec(t, ctx, db, "alter table "+table+" add column x int"+tc.algorithm)
					// Preserve the review's exact first terminal consumer, then
					// write the opposite HASH partition as well.
					m0Exec(t, ctx, db, "insert into "+table+"(a,b,x) values(2,2,42)")
					m0Exec(t, ctx, db, "insert into "+table+"(a,b,x) values(3,3,42)")
					require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table))
					require.Equal(t, 1, m0Count(t, ctx, db,
						"select count(*) from "+table+" where a=1 and b=1 and x is null"))
					require.Equal(t, 2, m0Count(t, ctx, db,
						"select count(*) from "+table+" where a in (2,3) and b=a and x=42"))
					afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, tc.name)
					require.NotEqual(t, beforeID, afterID, "partition root must take COPY")
					require.Zero(t, afterVersion)
					afterChildren := partitionIDs(afterID)
					for name, oldID := range beforeChildren {
						require.Contains(t, afterChildren, name)
						require.NotEqual(t, oldID, afterChildren[name], "backing partition must take COPY")
						require.Zero(t, m0Count(t, ctx, db, fmt.Sprintf(
							"select count(*) from mo_catalog.mo_tables where rel_id=%d", oldID)))
					}
					require.Zero(t, m0Count(t, ctx, db, fmt.Sprintf(
						"select count(*) from mo_catalog.mo_partition_tables where primary_table_id=%d", beforeID)))
				})
			}
		})
		m0Exec(t, ctx, db, "create table "+customer+`(
			c_id int not null,
			c_d_id int not null,
			c_w_id int not null,
			c_credit_lim decimal(12,2),
			c_ytd_payment decimal(12,2),
			c_balance decimal(12,2),
			primary key(c_w_id,c_d_id,c_id)
		)`)
		m0Exec(t, ctx, db, "insert into "+customer+" values "+
			"(1,1,1,50000,10000,10),(2,2,2,60000,6000,NULL),(3,3,3,70000,1000,30)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+customer+"')")
		originalObjects := m0ObjectFingerprints(t, ctx, db, customer)
		require.NotEmpty(t, originalObjects)

		tableID, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Zero(t, version)
		m0Exec(t, ctx, db, "alter table "+customer+" add column "+tierCol+" varchar(8)")
		require.Equal(t, originalObjects, m0ObjectFingerprints(t, ctx, db, customer))
		afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, tableID, afterID)
		require.Equal(t, uint32(1), afterVersion)
		var nullable string
		require.NoError(t, db.QueryRowContext(ctx,
			"select is_nullable from information_schema.columns where table_schema=? and table_name='customer' and column_name=?",
			schema, tierCol).Scan(&nullable))
		require.Equal(t, "YES", nullable)
		var notNull int8
		require.NoError(t, db.QueryRowContext(ctx,
			"select attnotnull from mo_catalog.mo_columns where att_database=? and att_relname='customer' and attname=?",
			schema, tierCol).Scan(&notNull))
		require.Zero(t, notNull)
		var projected sql.NullString
		require.NoError(t, db.QueryRowContext(ctx,
			"select "+tierCol+" from "+customer+" limit 1").Scan(&projected))
		require.False(t, projected.Valid)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+" is null"))
		require.Equal(t, 0, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+" is not null"))
		require.Equal(t, 0, m0Count(t, ctx, db, "select count("+tierCol+") from "+customer))

		// A later non-null ADD that cannot use M0 must fail before changing the
		// relation; the first successful independent ALTER remains installed.
		_, err = db.ExecContext(ctx,
			"alter table "+customer+" add column bad int not null, algorithm=inplace")
		require.Error(t, err)
		require.Equal(t, 1, m0Count(t, ctx, db,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='"+tierCol+"'"))
		require.Equal(t, 0, m0Count(t, ctx, db,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='bad'"))

		m0Exec(t, ctx, db, "create snapshot "+snapshot+" for table "+schema+" customer")
		m0Exec(t, ctx, db, "alter table "+customer+" add column "+creditCol+" decimal(10,2)")
		require.Equal(t, originalObjects, m0ObjectFingerprints(t, ctx, db, customer))
		afterID, afterVersion = m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, tableID, afterID)
		require.Equal(t, uint32(2), afterVersion)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+creditCol+" is null"))
		require.Equal(t, 3, m0Count(t, ctx, db,
			"select count(*) from "+customer+"{snapshot='"+snapshot+"'} where "+tierCol+" is null"))
		_, err = db.ExecContext(ctx,
			"select count(*) from "+customer+"{snapshot='"+snapshot+"'} where "+creditCol+" is not null")
		require.Error(t, err)

		m0Exec(t, ctx, db, "insert into "+customer+
			"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance,"+tierCol+
			") values(4,4,4,40000,12000,40,'Gold')")
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+" is null"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+"='Gold'"))

		// One old object is updated before the remaining old rows, exercising a
		// mixed old/new-object read and the first write into both appended columns.
		m0Exec(t, ctx, db, "update "+customer+" set "+tierCol+"='Silver',"+creditCol+"=12.50 where c_id=2")
		require.Equal(t, 1, m0Count(t, ctx, db,
			"select count(*)=1 from "+customer+" where "+tierCol+"='Silver' and "+creditCol+"=12.50"))
		require.Equal(t, []string{
			"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance", tierCol, creditCol,
		}, m0Columns(t, ctx, db, "select * from "+customer+" order by c_id"))

		// Clone must retain version, hidden key position/seqnum and both appended
		// columns, then accept another metadata-only append.
		m0Exec(t, ctx, db, "create table "+clone+" clone "+customer)
		var sourceVersion, cloneVersion uint32
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_version from mo_catalog.mo_tables where reldatabase=? and relname='customer'",
			schema).Scan(&sourceVersion))
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_version from mo_catalog.mo_tables where reldatabase=? and relname='customer_clone'",
			schema).Scan(&cloneVersion))
		require.Equal(t, sourceVersion, cloneVersion)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+clone+" where "+creditCol+" is null"))
		m0Exec(t, ctx, db, "alter table "+clone+" add column clone_only int")
		_, afterCloneVersion := m0TableGeneration(t, ctx, db, schema, "customer_clone")
		require.Equal(t, sourceVersion+1, afterCloneVersion)
		m0Exec(t, ctx, db, "update "+clone+" set clone_only=7 where c_id=1")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+clone+" where clone_only=7"))
		require.Equal(t, 0, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+"='clone_only'"))

		// Internal layout: the hidden composite key remains before all appended
		// columns, and the visible suffix follows user-visible SELECT * order.
		require.Equal(t, []string{
			"c_id:1:0:0", "c_d_id:2:1:0", "c_w_id:3:2:0",
			"c_credit_lim:4:3:0", "c_ytd_payment:5:4:0", "c_balance:6:5:0",
			"__mo_cpkey_col:7:6:1", "loyalty_tier_t0_s0:8:7:0",
			"credit_lim_t0_s0:9:8:0", "__mo_rowid:10:9:1",
		}, m0StoredColumnLayout(t, ctx, db, schema, "customer"))

		// Restart the CN before a global checkpoint. This proves catalog
		// publication and logtail/WAL state through the public SQL path while the
		// TN and existing object files remain online.
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, cn.Start())
		db = openM0SQL(t, cn)
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+customer))
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+customer+" where "+tierCol+" is null"))
		require.Equal(t, 1, m0Count(t, ctx, db,
			"select count(*) from "+customer+" where "+tierCol+"='Silver' and "+creditCol+"=12.50"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+clone+" where clone_only=7"))
		var metadataRows, metadataNulls int
		require.NoError(t, db.QueryRowContext(ctx,
			"select sum(rows_cnt),sum(null_cnt) from metadata_scan(?, ?) g",
			customer, tierCol).Scan(&metadataRows, &metadataNulls))
		require.Positive(t, metadataRows)
		require.GreaterOrEqual(t, metadataNulls, 2)
		var statsRows, statsObjects int
		require.NoError(t, db.QueryRowContext(ctx,
			"select table_cnt,accurate_object_number from table_stats(?,'refresh','full') g",
			customer).Scan(&statsRows, &statsObjects))
		require.Positive(t, statsRows)
		require.Positive(t, statsObjects)

		require.Equal(t, 2, m0Count(t, ctx, db,
			"select count(*) from "+customer+" where "+tierCol+" is null"))
		require.Equal(t, 3, m0Count(t, ctx, db,
			"select count(*) from "+customer+" where "+creditCol+" is null"))
		m0Exec(t, ctx, db, "update "+customer+" set "+
			tierCol+" = CASE WHEN c_ytd_payment > 9000 THEN 'Gold' "+
			"WHEN c_ytd_payment > 5000 THEN 'Silver' ELSE 'Bronze' END, "+
			creditCol+" = c_credit_lim")
		require.Equal(t, 0, m0Count(t, ctx, db,
			"select count(*) from "+customer+" where "+tierCol+" is null or "+creditCol+" is null"))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+customer))
		require.Equal(t, []string{
			"Bronze:1:70000.00", "Gold:2:45000.00", "Silver:1:60000.00",
		}, m0CustomerEvaluation(t, ctx, db, customer, tierCol, creditCol))

		// The first post-restart ADD must publish a new generation while old
		// objects remain readable through the same relation.
		m0Exec(t, ctx, db, "alter table "+customer+" add column post_restart int")
		_, afterRestartVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, uint32(3), afterRestartVersion)
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+customer+" where post_restart is null"))
		require.Equal(t, 3, m0Count(t, ctx, db,
			"select count(*) from "+customer+"{snapshot='"+snapshot+"'} where "+tierCol+" is null"))
		_, err = db.ExecContext(ctx,
			"select count(*) from "+customer+"{snapshot='"+snapshot+"'} where "+creditCol+" is not null")
		require.Error(t, err)

		// A named-snapshot clone starts from the historical definition. The
		// active data branch starts from the current version-three definition.
		m0Exec(t, ctx, db, "create table "+history+" clone "+customer+" {snapshot='"+snapshot+"'}")
		_, historyVersion := m0TableGeneration(t, ctx, db, schema, "customer_history")
		require.Equal(t, uint32(1), historyVersion)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+history+" where "+tierCol+" is null"))
		_, err = db.ExecContext(ctx, "select "+creditCol+" from "+history+" limit 1")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "select post_restart from "+history+" limit 1")
		require.Error(t, err)

		m0Exec(t, ctx, db, "data branch create table "+branch+" from "+customer)
		_, branchVersion := m0TableGeneration(t, ctx, db, schema, "customer_branch")
		require.Equal(t, uint32(3), branchVersion)
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+branch+" where post_restart is null"))

		// Non-autocommit ALTER is outside the M0 transition. The existing
		// branch-lineage guard rejects it and the schema must remain unchanged.
		txn, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = txn.ExecContext(ctx, "alter table "+customer+" add column txn_only int")
		require.Error(t, err)
		require.NoError(t, txn.Rollback())
		require.Equal(t, 0, m0Count(t, ctx, db,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='txn_only'"))
		_, afterRollbackVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, uint32(3), afterRollbackVersion)

		m0Exec(t, ctx, db, "drop snapshot "+snapshot)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+history+" where "+tierCol+" is null"))
		m0Exec(t, ctx, db, "drop table "+customer)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+clone+" where "+tierCol+" is not null"))
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+history+" where "+tierCol+" is null"))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+branch+" where post_restart is null"))

		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+clone+"')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+history+"')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+branch+"')")
		m0RunL0Merge(t, ctx, db, clone)
		m0Exec(t, ctx, db, "select mo_ctl('dn','globalcheckpoint','')")

		tn := m0ServiceOfType(t, cluster, metadata.ServiceType_TN)
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, tn.Close())
		require.NoError(t, tn.Start())
		require.NoError(t, cn.Start())
		db = openM0SQL(t, cn)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+clone+" where "+tierCol+" is not null"))
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+history+" where "+tierCol+" is null"))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+branch+" where post_restart is null"))
	})
}

func TestM0ConcurrentAddColumnSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer func() { _ = db.Close() }()

		const (
			schema   = "m0_concurrent_add"
			customer = schema + ".customer"
			leftCol  = "concurrent_left"
			rightCol = "concurrent_right"
		)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+customer+`(
			c_id int not null,
			c_d_id int not null,
			c_w_id int not null,
			c_credit_lim decimal(12,2),
			c_ytd_payment decimal(12,2),
			c_balance decimal(12,2),
			primary key(c_w_id,c_d_id,c_id)
		)`)
		m0Exec(t, ctx, db, "insert into "+customer+" values "+
			"(1,1,1,50000,10000,10),(2,2,2,60000,6000,NULL),(3,3,3,70000,1000,30)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+customer+"')")
		originalObjects := m0ObjectFingerprints(t, ctx, db, customer)
		require.NotEmpty(t, originalObjects)

		type addResult struct {
			column string
			err    error
		}
		start := make(chan struct{})
		results := make(chan addResult, 2)
		for _, column := range []string{leftCol, rightCol} {
			go func() {
				<-start
				_, execErr := db.ExecContext(
					ctx, "alter table "+customer+" add column "+column+" int",
				)
				results <- addResult{column: column, err: execErr}
			}()
		}
		close(start)

		succeeded := make(map[string]bool)
		for range 2 {
			result := <-results
			if result.err == nil {
				succeeded[result.column] = true
			}
		}
		require.NotEmpty(t, succeeded)
		_, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, uint32(len(succeeded)), version)

		for _, column := range []string{leftCol, rightCol} {
			count := m0Count(t, ctx, db,
				"select count(*) from information_schema.columns where table_schema='"+schema+
					"' and table_name='customer' and column_name='"+column+"'")
			if succeeded[column] {
				require.Equal(t, 1, count)
				var nullable string
				require.NoError(t, db.QueryRowContext(ctx,
					"select is_nullable from information_schema.columns "+
						"where table_schema=? and table_name='customer' and column_name=?",
					schema, column).Scan(&nullable))
				require.Equal(t, "YES", nullable)
				var notNull, hasDef int8
				require.NoError(t, db.QueryRowContext(ctx,
					"select attnotnull,atthasdef from mo_catalog.mo_columns "+
						"where att_database=? and att_relname='customer' and attname=?",
					schema, column).Scan(&notNull, &hasDef))
				require.Zero(t, notNull)
				require.Equal(t, int8(1), hasDef)
				require.Equal(t, 3, m0Count(t, ctx, db,
					"select count(*) from "+customer+" where "+column+" is null"))
			} else {
				require.Zero(t, count)
			}
		}
		require.Equal(t, originalObjects, m0ObjectFingerprints(t, ctx, db, customer))
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+customer))
		tableDefNullability := m0TableDefNullability(t, ctx, cn, schema, "customer")
		for column := range succeeded {
			require.True(t, tableDefNullability[column], "column %s in current transaction TableDef", column)
		}
		stored := m0StoredColumnLayout(t, ctx, db, schema, "customer")
		require.NotEmpty(t, stored)
		require.True(t, strings.HasPrefix(stored[len(stored)-1], "__mo_rowid:"), stored)
		for column := range succeeded {
			require.Equal(t, 1, m0Count(t, ctx, db,
				"select count(*) from mo_catalog.mo_columns where att_database='"+schema+
					"' and att_relname='customer' and attname='"+column+
					"' and attnum<=(select min(attnum) from mo_catalog.mo_columns "+
					"where att_database='"+schema+"' and att_relname='customer' and attname='__mo_rowid')-1"))
		}
		m0Exec(t, ctx, db, "insert into "+customer+
			"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values(4,4,4,40000,12000,40)")
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+customer))
		for column := range succeeded {
			require.Equal(t, 4, m0Count(t, ctx, db,
				"select count(*) from "+customer+" where "+column+" is null"))
		}
	})
}
