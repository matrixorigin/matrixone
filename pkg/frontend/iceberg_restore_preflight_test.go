// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestIcebergAccountRestorePreflight(t *testing.T) {
	const list = "select relname from mo_catalog.mo_tables {MO_TS = 123} where account_id = 42 and reldatabase = 'mo_catalog' and relname in ('mo_iceberg_catalogs','mo_iceberg_principal_map','mo_iceberg_residency_policy','mo_iceberg_tables','mo_iceberg_refs','mo_iceberg_publish_jobs','mo_iceberg_orphan_files','mo_iceberg_maintenance_jobs')"
	tables := []string{"mo_iceberg_catalogs", "mo_iceberg_principal_map", "mo_iceberg_residency_policy", "mo_iceberg_tables", "mo_iceberg_refs", "mo_iceberg_publish_jobs", "mo_iceberg_orphan_files", "mo_iceberg_maintenance_jobs"}
	for _, tc := range []struct {
		name            string
		target          uint32
		exists          bool
		present         []string
		populated, fail string
		want            string
	}{
		{"same account catalog only", 42, true, tables, "", "", ""},
		{"cross account catalog", 43, true, tables, "historical catalog", "", "cannot safely rebind"},
		{"same account table mapping", 42, true, tables, "historical mapping", "", "cannot safely rebind"},
		{"legacy snapshot and empty target", 42, true, nil, "", "", ""},
		{"legacy snapshot and target catalog", 42, true, nil, "target catalog", "", "absent from the historical snapshot"},
		{"new target and old snapshot", 0, false, nil, "", "", ""},
		{"historical read failure", 42, true, tables, "", "historical", "read failed"},
		{"target read failure", 42, true, nil, "", "target", "read failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			var rows [][]interface{}
			for _, name := range tc.present {
				rows = append(rows, []interface{}{name})
			}
			bh.sql2result[list] = newMrsForRestoreStringRows([]string{"relname"}, rows)
			for _, table := range tables {
				historic := fmt.Sprintf("select '1' from %s {MO_TS = 123}", qualifiedTableName(moCatalog, table))
				if tc.exists && tc.target == 42 && table == "mo_iceberg_catalogs" {
					historic += " where account_id <> 42"
				}
				historic += " limit 1"
				current := fmt.Sprintf("select '1' from %s limit 1", qualifiedTableName(moCatalog, table))
				bh.sql2result[historic] = newMrsForRestoreStringRows([]string{"1"}, nil)
				bh.sql2result[current] = newMrsForRestoreStringRows([]string{"1"}, nil)
				if (tc.populated == "historical catalog" && table == "mo_iceberg_catalogs") || (tc.populated == "historical mapping" && table == "mo_iceberg_tables") {
					bh.sql2result[historic] = newMrsForRestoreStringRows([]string{"1"}, [][]interface{}{{"1"}})
				}
				if tc.populated == "target catalog" && table == "mo_iceberg_catalogs" {
					bh.sql2result[current] = newMrsForRestoreStringRows([]string{"1"}, [][]interface{}{{"1"}})
				}
				if tc.fail == "historical" && table == "mo_iceberg_catalogs" {
					bh.sql2err[historic] = moerr.NewInternalErrorNoCtx("read failed")
				}
				if tc.fail == "target" && table == "mo_iceberg_catalogs" {
					bh.sql2err[current] = moerr.NewInternalErrorNoCtx("read failed")
				}
			}
			err := preflightIcebergAccountRestore(context.Background(), bh, 123, 42, tc.target, tc.exists)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestIcebergPartialRestorePreflight(t *testing.T) {
	historical := "select relname, rel_createsql from mo_catalog.mo_tables {MO_TS = 123} where account_id = 42 and reldatabase = 'db' and relkind = 'e' and relname = 'orders'"
	current := "select relname, rel_createsql from mo_catalog.mo_tables where account_id = 42 and reldatabase = 'db' and relkind = 'e' and relname = 'orders'"
	iceberg := "/* MO_ICEBERG: version=1; kind=iceberg_table; catalog=c; namespace=n; table=t */"
	for _, tc := range []struct{ name, source, target, want string }{
		{"ordinary external tables", "create external table orders (a int)", "", ""},
		{"historical iceberg", iceberg, "", "restore cannot safely"},
		{"current iceberg", "", iceberg, "restore cannot safely"},
		{"malformed historical metadata", "/* MO_ICEBERG: broken */", "", "iceberg rel_createsql"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			var source, target [][]interface{}
			if tc.source != "" {
				source = [][]interface{}{{"orders", tc.source}}
			}
			if tc.target != "" {
				target = [][]interface{}{{"orders", tc.target}}
			}
			bh.sql2result[historical] = newMrsForRestoreStringRows([]string{"relname", "rel_createsql"}, source)
			bh.sql2result[current] = newMrsForRestoreStringRows([]string{"relname", "rel_createsql"}, target)
			err := preflightIcebergPartialRestore(context.Background(), bh, 123, 42, "db", "orders")
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestIcebergPartialRestoreDatabaseScopeAndFailures(t *testing.T) {
	historical := "select relname, rel_createsql from mo_catalog.mo_tables {MO_TS = 123} where account_id = 42 and reldatabase = 'db' and relkind = 'e'"
	current := "select relname, rel_createsql from mo_catalog.mo_tables where account_id = 42 and reldatabase = 'db' and relkind = 'e'"
	for _, fail := range []string{"", historical, current} {
		t.Run(fmt.Sprintf("failure=%s", fail), func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[historical] = newMrsForRestoreStringRows([]string{"relname", "rel_createsql"}, nil)
			bh.sql2result[current] = newMrsForRestoreStringRows([]string{"relname", "rel_createsql"}, nil)
			if fail != "" {
				bh.sql2err[fail] = moerr.NewInternalErrorNoCtx("read failed")
			}
			err := preflightIcebergPartialRestore(context.Background(), bh, 123, 42, "db", "")
			if fail == "" {
				require.NoError(t, err)
				require.Equal(t, []string{historical, current}, bh.executedSQLs)
			} else {
				require.ErrorContains(t, err, "read failed")
			}
		})
	}
}
