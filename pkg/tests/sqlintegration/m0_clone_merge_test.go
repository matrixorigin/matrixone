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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestM0CloneMergeProofUsesDerivedVersion(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer func() { _ = db.Close() }()

		const (
			parent   = "m0_clone_merge_parent"
			child    = "m0_clone_merge_child"
			snapshot = "m0_clone_merge_snapshot"
			parentT  = parent + ".customer"
			childT   = child + ".customer"
			tier0    = "loyalty_tier_t0_s0"
			credit0  = "credit_lim_t0_s0"
			tier1    = "loyalty_tier_t1_s1"
			credit1  = "credit_lim_t1_s1"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists "+snapshot,
			"drop database if exists "+child,
			"drop database if exists "+parent,
		)

		m0Exec(t, ctx, db, "create database "+parent)
		m0Exec(t, ctx, db, "create table "+parentT+m0RollbackCustomerSchema)
		m0Exec(t, ctx, db, "insert into "+parentT+" values "+
			"(1,1,1,50000,10000,10),(2,2,2,60000,6000,NULL),(3,3,3,70000,1000,30)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+parentT+"')")

		m0Exec(t, ctx, db, "alter table "+parentT+" add column "+tier0+" varchar(8)")
		m0Exec(t, ctx, db, "alter table "+parentT+" add column "+credit0+" decimal(10,2)")
		parentID, parentVersion := m0TableGeneration(t, ctx, db, parent, "customer")
		require.Equal(t, uint32(2), parentVersion)
		require.Equal(t, parentVersion, m0TNCatalogSchemaVersion(t, ctx, db, parentT))
		m0Exec(t, ctx, db, "update "+parentT+" set "+
			tier0+"=case when c_ytd_payment > 9000 then 'Gold' "+
			"when c_ytd_payment > 5000 then 'Silver' else 'Bronze' end,"+
			credit0+"=c_credit_lim")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+parentT+"')")

		m0Exec(t, ctx, db, "create snapshot "+snapshot+" for database "+parent)
		m0Exec(t, ctx, db, "create database "+child+" clone "+parent+" {snapshot='"+snapshot+"'}")
		m0Exec(t, ctx, db, "drop snapshot "+snapshot)
		childID, cloneVersion := m0TableGeneration(t, ctx, db, child, "customer")
		require.Equal(t, uint32(2), cloneVersion)
		require.Equal(t, cloneVersion, m0TNCatalogSchemaVersion(t, ctx, db, childT))

		m0Exec(t, ctx, db, "alter table "+childT+" add column "+tier1+" varchar(8)")
		m0Exec(t, ctx, db, "alter table "+childT+" add column "+credit1+" decimal(10,2)")
		childIDAfterAlter, childVersion := m0TableGeneration(t, ctx, db, child, "customer")
		require.Equal(t, childID, childIDAfterAlter)
		require.Equal(t, uint32(4), childVersion)
		require.Equal(t, childVersion, m0TNCatalogSchemaVersion(t, ctx, db, childT))
		t.Logf(
			"CN catalog schema identity parent=%d:v%d child=%d:v%d->v%d",
			parentID, parentVersion, childID, cloneVersion, childVersion,
		)
		t.Logf("parent column identity: %s", m0StoredColumnIdentity(
			t, ctx, db, parent, "customer", tier0,
		))
		t.Logf("parent column identity: %s", m0StoredColumnIdentity(
			t, ctx, db, parent, "customer", credit0,
		))
		t.Logf("child inherited column identity: %s", m0StoredColumnIdentity(
			t, ctx, db, child, "customer", tier0,
		))
		t.Logf("child column identity: %s", m0StoredColumnIdentity(
			t, ctx, db, child, "customer", tier1,
		))
		t.Logf("child column identity: %s", m0StoredColumnIdentity(
			t, ctx, db, child, "customer", credit1,
		))
		parentLayout := m0StoredColumnLayout(t, ctx, db, parent, "customer")
		childLayout := m0StoredColumnLayout(t, ctx, db, child, "customer")
		require.Len(t, parentLayout, 10)
		require.Len(t, childLayout, 12)
		require.Equal(t,
			"attnum=8 seqnum=7 notnull=0 hidden=0 type=varchar(8)",
			m0StoredColumnIdentity(t, ctx, db, child, "customer", tier0),
		)
		require.Equal(t,
			"attnum=9 seqnum=8 notnull=0 hidden=0 type=decimal(10,2)",
			m0StoredColumnIdentity(t, ctx, db, child, "customer", credit0),
		)
		require.Equal(t,
			"attnum=10 seqnum=9 notnull=0 hidden=0 type=varchar(8)",
			m0StoredColumnIdentity(t, ctx, db, child, "customer", tier1),
		)
		require.Equal(t,
			"attnum=11 seqnum=10 notnull=0 hidden=0 type=decimal(10,2)",
			m0StoredColumnIdentity(t, ctx, db, child, "customer", credit1),
		)
		m0Exec(t, ctx, db, "update "+childT+" set "+
			tier1+"=case when c_ytd_payment > 9000 then 'Gold' "+
			"when c_ytd_payment > 5000 then 'Silver' else 'Bronze' end,"+
			credit1+"=c_credit_lim")
		m0Exec(t, ctx, db,
			"insert into "+childT+" values(5,5,5,80000,9500,50,'Gold',80000,'Gold',80000)")
		m0Exec(t, ctx, db,
			"insert into "+parentT+" values(4,4,4,40000,3000,40,'Parent',40000)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+childT+"')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+parentT+"')")

		beforeHeaders := m0ObjectHeaderProofs(t, ctx, db, cn, childT)
		beforeFingerprints := m0ObjectFingerprints(t, ctx, db, childT)
		parentBeforeFingerprints := m0ObjectFingerprints(t, ctx, db, parentT)
		parentBeforeHeaders := m0ObjectHeaderProofs(t, ctx, db, cn, parentT)
		require.NotEmpty(t, beforeHeaders)
		require.NotEmpty(t, beforeFingerprints)
		require.NotEmpty(t, parentBeforeFingerprints)
		require.NotEmpty(t, parentBeforeHeaders)
		require.Equal(t, []string{
			"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance",
			tier0, credit0,
		}, m0Columns(t, ctx, db, "select * from "+parentT+" limit 0"))
		require.Equal(t, []string{
			"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance",
			tier0, credit0, tier1, credit1,
		}, m0Columns(t, ctx, db, "select * from "+childT+" limit 0"))
		t.Logf("child objects before merge: %v", beforeHeaders)
		t.Logf("parent objects before child merge: %v", parentBeforeHeaders)

		m0RunL0Merge(t, ctx, db, childT)
		afterHeaders := m0ObjectHeaderProofs(t, ctx, db, cn, childT)
		afterFingerprints := m0ObjectFingerprints(t, ctx, db, childT)
		parentAfterFingerprints := m0ObjectFingerprints(t, ctx, db, parentT)
		parentAfterHeaders := m0ObjectHeaderProofs(t, ctx, db, cn, parentT)
		require.NotEmpty(t, afterHeaders)
		require.NotEqual(t, beforeFingerprints, afterFingerprints)
		require.Equal(t, parentBeforeFingerprints, parentAfterFingerprints)
		require.Equal(t, parentBeforeHeaders, parentAfterHeaders)
		t.Logf("child objects after merge: %v", afterHeaders)
		require.Condition(t, func() bool {
			for _, header := range afterHeaders {
				if !strings.Contains(header, ":4:") {
					return false
				}
			}
			return true
		}, "merge output did not carry the derived schema version: %v", afterHeaders)
		completed, pending := m0DataMergeState(t, ctx, db, childT)
		require.Positive(t, completed)
		require.Zero(t, pending)

		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+parentT))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+childT))
		require.Equal(t, 1, m0Count(t, ctx, db,
			"select count(*) from "+parentT+" where "+tier0+"='Parent'"))
		require.Zero(t, m0Count(t, ctx, db,
			"select count(*) from "+childT+" where "+tier0+"='Parent'"))
		require.Equal(t, 2, m0Count(t, ctx, db,
			"select count(*) from "+childT+" where "+tier0+"='Gold'"))
		require.Equal(t, 4, m0Count(t, ctx, db,
			"select count(*) from "+childT+" where "+credit1+" is not null"))
		require.Equal(t, 1, m0Count(t, ctx, db,
			"select count(*) from "+childT+" where "+tier1+"='Gold' and c_id=5"))
		_, err = db.ExecContext(ctx, "select "+tier1+" from "+parentT+" limit 1")
		require.Error(t, err)

		withoutExtra := func(columns []string) []string {
			return columns[:len(columns)-2]
		}
		require.Equal(t, withoutExtra(m0Columns(t, ctx, db, "select * from "+childT+" limit 0")),
			m0Columns(t, ctx, db, "select * from "+parentT+" limit 0"))
	})
}

func m0TNCatalogSchemaVersion(t *testing.T, ctx context.Context, db *sql.DB, table string) uint32 {
	t.Helper()
	var dump string
	require.NoError(t, db.QueryRowContext(ctx, "select mo_ctl('dn','inspect',?)",
		"catalog -t "+table+" -vv").Scan(&dump))
	t.Logf("TN catalog raw %s: %s", table, dump)
	matches := regexp.MustCompile(`schema\.v(\d+)\.s\d+\.c\d+`).FindAllStringSubmatch(dump, -1)
	require.NotEmpty(t, matches, "TN schema version unavailable: %s", table)
	var latest uint32
	for _, match := range matches {
		version, err := strconv.ParseUint(match[1], 10, 32)
		require.NoError(t, err)
		latest = max(latest, uint32(version))
	}
	return latest
}

func m0StoredColumnIdentity(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	schema, table, column string,
) string {
	t.Helper()
	var name, typ, visibleType string
	var attnum int
	var seqnum uint16
	var notNull, hidden int8
	require.NoError(t, db.QueryRowContext(ctx,
		"select attname,attnum,attr_seqnum,attnotnull,att_is_hidden,atttyp "+
			"from mo_catalog.mo_columns "+
			"where att_database=? and att_relname=? and attname=?",
		schema, table, column,
	).Scan(&name, &attnum, &seqnum, &notNull, &hidden, &typ))
	require.Equal(t, column, name)
	require.NoError(t, db.QueryRowContext(ctx,
		"select column_type from information_schema.columns "+
			"where table_schema=? and table_name=? and column_name=?",
		schema, table, column,
	).Scan(&visibleType))
	return fmt.Sprintf(
		"attnum=%d seqnum=%d notnull=%d hidden=%d type=%s",
		attnum, seqnum, notNull, hidden, strings.ToLower(visibleType),
	)
}
