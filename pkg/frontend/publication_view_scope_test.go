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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

const testPublicationScopeDatabase = "view_pub_db"

// generalSubscriberAccounts is the target account set of every scope test: one
// general account, so system relations need general-account availability.
var generalSubscriberAccounts = []uint32{1}

func newMrsForPublicationRelations(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, name := range []string{"relname", "relkind", "viewdef", "rel_id", "rel_version"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		// Fixtures that carry no schema identity read as rel_id 0 and
		// rel_version 0; the stability comparison sees the same zeros on
		// both reads.
		for len(row) < 5 {
			row = append(append([]interface{}{}, row...), uint64(0))
		}
		mrs.AddRow(row)
	}
	return mrs
}

func publicationRelationsSQL(dbName string, accountId uint32) string {
	return fmt.Sprintf(getPublicationRelationsFormat, sanitizeSQLInput(dbName), accountId)
}

func publicationSystemRelationSQL(database, relation string) string {
	return fmt.Sprintf(
		getPublicationSystemRelationFormat, sanitizeSQLInput(database), sanitizeSQLInput(relation))
}

// publicationViewDefWithDependencies builds the persisted view definition of a
// view created by the current binder, which records its resolved dependencies.
func publicationViewDefWithDependencies(
	t *testing.T,
	stmt string,
	dependencies []plan.ViewDependency,
) string {
	return publicationViewDefInDatabase(t, testPublicationScopeDatabase, stmt, dependencies)
}

func publicationViewDefInDatabase(
	t *testing.T,
	database string,
	stmt string,
	dependencies []plan.ViewDependency,
) string {
	t.Helper()
	viewData := plan.ViewData{
		Stmt:                stmt,
		DefaultDatabase:     database,
		LowerCaseTableNames: func(v int64) *int64 { return &v }(1),
		Dependencies:        dependencies,
	}
	encoded, err := json.Marshal(viewData)
	require.NoError(t, err)
	return string(encoded)
}

// publicationLegacyViewDef builds a persisted view definition whose creator did
// not record dependencies, so scope validation must parse the view SQL.
func publicationLegacyViewDef(t *testing.T, stmt string) string {
	t.Helper()
	viewData := plan.ViewData{
		Stmt:                stmt,
		DefaultDatabase:     testPublicationScopeDatabase,
		LowerCaseTableNames: func(v int64) *int64 { return &v }(1),
	}
	encoded, err := json.Marshal(viewData)
	require.NoError(t, err)
	return string(encoded)
}

// publicationLegacyViewDefWithoutCaseMode builds a legacy persisted
// definition whose creator recorded neither dependencies nor the case mode
// the instance ran with - the shape left by clusters upgraded past the rows
// that persist both fields. Scope validation must resolve such a
// definition's relations through the validating session's own case mode:
// guessing a different mode certifies a relation name the subscriber never
// resolves.
func publicationLegacyViewDefWithoutCaseMode(t *testing.T, stmt string) string {
	t.Helper()
	viewData := plan.ViewData{
		Stmt:            stmt,
		DefaultDatabase: testPublicationScopeDatabase,
	}
	encoded, err := json.Marshal(viewData)
	require.NoError(t, err)
	return string(encoded)
}

func publicationDependency(database, relation string, mutate func(*plan.ViewDependency)) plan.ViewDependency {
	dependency := plan.ViewDependency{
		AccountID:    sysAccountID,
		DatabaseName: database,
		RelationName: relation,
		RelationKind: catalog.SystemOrdinaryRel,
	}
	if mutate != nil {
		mutate(&dependency)
	}
	return dependency
}

// addPublicationSystemRelation mocks the catalog rows of one system relation
// for the system-relation availability read.
func addPublicationSystemRelation(bh *backgroundExecTest, database, relation, relKind string, accountIDs ...uint32) {
	rows := make([][]interface{}, 0, len(accountIDs))
	for _, accountId := range accountIDs {
		rows = append(rows, []interface{}{relKind, uint64(accountId)})
	}
	mrs := &MysqlResultSet{}
	for _, name := range []string{"relkind", "account_id"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	bh.sql2result[publicationSystemRelationSQL(database, relation)] = mrs
}

func TestValidatePublicationTableScope(t *testing.T) {
	viewOnlyDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "base", nil),
		})
	nestedViewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_nested AS SELECT id FROM v_only",
		[]plan.ViewDependency{
			func() plan.ViewDependency {
				dependency := publicationDependency(testPublicationScopeDatabase, "v_only", nil)
				dependency.RelationKind = catalog.SystemViewRel
				return dependency
			}(),
		})
	joinViewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_join AS SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "t1", nil),
			publicationDependency(testPublicationScopeDatabase, "t2", nil),
			publicationDependency(testPublicationScopeDatabase, "t1", nil),
		})
	constantViewDef := publicationLegacyViewDef(t,
		"CREATE VIEW v_const AS SELECT 1 AS one")

	for _, testCase := range []struct {
		name string
		// sessionCaseMode is the validating session's case mode; nil runs
		// the validation at the mode-1 session context every fixture not
		// named case-distinct was certified under.
		sessionCaseMode *int64
		relations       [][]interface{}
		tableNames      []string
		targets         []uint32
		wantAccepted    bool
		wantError       string
	}{
		{
			name:         "empty table list",
			relations:    nil,
			tableNames:   nil,
			wantAccepted: true,
		},
		{
			name: "plain tables without views are not validated further",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
			},
			tableNames:   []string{"base"},
			wantAccepted: true,
		},
		{
			name: "view with its base table in the list is accepted",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
			},
			tableNames:   []string{"base", "v_only"},
			wantAccepted: true,
		},
		{
			name: "view without its base table is rejected",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
			},
			tableNames: []string{"v_only"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'",
		},
		{
			name: "missing transitive base table through a nested view is rejected",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
				{"v_nested", catalog.SystemViewRel, nestedViewDef},
			},
			tableNames: []string{"v_nested", "v_only"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'",
		},
		{
			name: "missing intermediate view is rejected even with the base table",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
				{"v_nested", catalog.SystemViewRel, nestedViewDef},
			},
			tableNames: []string{"base", "v_nested"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 'v_only' required by view 'v_nested'",
		},
		{
			name: "view joining two base tables needs both",
			relations: [][]interface{}{
				{"t1", catalog.SystemOrdinaryRel, ""},
				{"t2", catalog.SystemOrdinaryRel, ""},
				{"v_join", catalog.SystemViewRel, joinViewDef},
			},
			tableNames: []string{"t1", "v_join"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 't2' required by view 'v_join'",
		},
		{
			name: "view joining two base tables is accepted with both",
			relations: [][]interface{}{
				{"t1", catalog.SystemOrdinaryRel, ""},
				{"t2", catalog.SystemOrdinaryRel, ""},
				{"v_join", catalog.SystemViewRel, joinViewDef},
			},
			tableNames:   []string{"t1", "t2", "v_join"},
			wantAccepted: true,
		},
		{
			name: "constant view without relations is accepted",
			relations: [][]interface{}{
				{"v_const", catalog.SystemViewRel, constantViewDef},
			},
			tableNames:   []string{"v_const"},
			wantAccepted: true,
		},
		{
			name: "legacy view definition parses its SQL for dependencies",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_legacy", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_legacy AS SELECT id FROM base WHERE id > 1")},
			},
			tableNames: []string{"v_legacy"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_legacy'",
		},
		{
			name: "legacy view with complete dependency is accepted",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_legacy", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_legacy AS SELECT id FROM base WHERE id > 1")},
			},
			tableNames:   []string{"base", "v_legacy"},
			wantAccepted: true,
		},
		{
			name: "inherited table list naming a missing relation is rejected",
			relations: [][]interface{}{
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
			},
			tableNames: []string{"absent", "v_only"},
			wantError:  "table 'absent' not exists",
		},
		{
			name: "dependency on a dropped relation is reported explicitly",
			relations: [][]interface{}{
				{"v_only", catalog.SystemViewRel, viewOnlyDef},
			},
			tableNames: []string{"v_only"},
			wantError:  "view 'v_only' depends on table 'base' which does not exist in database 'view_pub_db'",
		},
		{
			name: "dependency on another database is rejected",
			relations: [][]interface{}{
				{"v_foreign", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_foreign AS SELECT a FROM other_db.src",
					[]plan.ViewDependency{
						publicationDependency("other_db", "src", nil),
					})},
			},
			tableNames: []string{"v_foreign"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_foreign': it depends on relation 'other_db.src' outside the publication database",
		},
		{
			name: "dependency bound through a subscription is rejected",
			relations: [][]interface{}{
				{"v_sub", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_sub AS SELECT a FROM sub_db.src",
					[]plan.ViewDependency{
						publicationDependency(testPublicationScopeDatabase, "src", func(dependency *plan.ViewDependency) {
							dependency.SubscriptionName = "sub_db"
						}),
					})},
			},
			tableNames: []string{"v_sub"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_sub': it depends on relation 'view_pub_db.src' bound to another account, subscription, or snapshot",
		},
		{
			name: "dependency owned by another account is rejected",
			relations: [][]interface{}{
				{"v_foreign_account", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_foreign_account AS SELECT a FROM src",
					[]plan.ViewDependency{
						publicationDependency(testPublicationScopeDatabase, "src", func(dependency *plan.ViewDependency) {
							dependency.AccountID = sysAccountID + 1
						}),
					})},
			},
			tableNames: []string{"v_foreign_account"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_foreign_account': it depends on relation 'view_pub_db.src' bound to another account, subscription, or snapshot",
		},
		{
			name: "persisted dependency on a snapshot time travel is rejected",
			relations: [][]interface{}{
				{"v_snapshot", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_snapshot AS SELECT a FROM src",
					[]plan.ViewDependency{
						publicationDependency(testPublicationScopeDatabase, "src", func(dependency *plan.ViewDependency) {
							dependency.SnapshotName = "snap"
						}),
					})},
			},
			tableNames: []string{"v_snapshot"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_snapshot': it depends on relation 'view_pub_db.src' bound to another account, subscription, or snapshot",
		},
		{
			name: "legacy dependency on a snapshot time travel is rejected like the persisted form",
			relations: [][]interface{}{
				{"src", catalog.SystemOrdinaryRel, ""},
				{"v_snapshot_legacy", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_snapshot_legacy AS SELECT a FROM src {snapshot = 'sp1'}")},
			},
			tableNames: []string{"src", "v_snapshot_legacy"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_snapshot_legacy': it depends on relation 'view_pub_db.src' bound to another account, subscription, or snapshot",
		},
		{
			name: "system relation owned by every account is accepted",
			relations: [][]interface{}{
				{"v_system", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_system AS SELECT user_name FROM mo_catalog.mo_user",
					[]plan.ViewDependency{
						publicationDependency("mo_catalog", "mo_user", nil),
					})},
			},
			tableNames:   []string{"v_system"},
			wantAccepted: true,
		},
		{
			name: "shared mo_catalog relation is accepted",
			relations: [][]interface{}{
				{"v_shared", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_shared AS SELECT relname FROM mo_catalog.mo_tables",
					[]plan.ViewDependency{
						publicationDependency("mo_catalog", "mo_tables", nil),
					})},
			},
			tableNames:   []string{"v_shared"},
			wantAccepted: true,
		},
		{
			name: "sys-only system relation is rejected for a general subscriber",
			relations: [][]interface{}{
				{"v_account", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_account AS SELECT account_name FROM mo_catalog.mo_account",
					[]plan.ViewDependency{
						publicationDependency("mo_catalog", "mo_account", nil),
					})},
			},
			tableNames: []string{"v_account"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_account': system relation 'mo_catalog.mo_account' is not available to its subscriber accounts",
		},
		{
			name: "sys-only system database is rejected for a general subscriber",
			relations: [][]interface{}{
				{"v_task", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_task AS SELECT a FROM mo_task.sys_mo_usage",
					[]plan.ViewDependency{
						publicationDependency("mo_task", "sys_mo_usage", nil),
					})},
			},
			tableNames: []string{"v_task"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_task': system relation 'mo_task.sys_mo_usage' is not available to its subscriber accounts",
		},
		{
			name: "sys-only system relation is accepted when only the system account subscribes",
			relations: [][]interface{}{
				{"v_account_sys", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_account_sys AS SELECT account_name FROM mo_catalog.mo_account",
					[]plan.ViewDependency{
						publicationDependency("mo_catalog", "mo_account", nil),
					})},
			},
			tableNames:   []string{"v_account_sys"},
			targets:      []uint32{sysAccountID},
			wantAccepted: true,
		},
		{
			name: "cluster system relation is accepted",
			relations: [][]interface{}{
				{"v_cluster", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_cluster AS SELECT count(*) FROM mo_catalog.mo_view_refresh",
					[]plan.ViewDependency{
						publicationDependency("mo_catalog", "mo_view_refresh", nil),
					})},
			},
			tableNames:   []string{"v_cluster"},
			wantAccepted: true,
		},
		{
			name: "legacy view referencing an available system relation is accepted",
			relations: [][]interface{}{
				{"v_system_legacy", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_system_legacy AS SELECT relname FROM mo_catalog.mo_tables")},
			},
			tableNames:   []string{"v_system_legacy"},
			wantAccepted: true,
		},
		{
			name: "legacy view with a qualified reference to the publication database is rejected",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_qref", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_qref AS SELECT id FROM view_pub_db.base")},
			},
			tableNames: []string{"base", "v_qref"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_qref': view references relation 'view_pub_db.base' with a qualified name, which its subscriber accounts cannot resolve",
		},
		{
			name: "current-format view with persisted dependencies and a qualified reference is rejected",
			relations: [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_qref_dep", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_qref_dep AS SELECT id FROM view_pub_db.base",
					[]plan.ViewDependency{
						publicationDependency(testPublicationScopeDatabase, "base", nil),
					})},
			},
			tableNames: []string{"base", "v_qref_dep"},
			wantError:  "table-level publication for database 'view_pub_db' cannot publish view 'v_qref_dep': view references relation 'view_pub_db.base' with a qualified name, which its subscriber accounts cannot resolve",
		},
		{
			name:            "legacy case-distinct view over the wrong-case member is rejected on a mode-0 session",
			sessionCaseMode: func(v int64) *int64 { return &v }(0),
			relations: [][]interface{}{
				{"Base", catalog.SystemOrdinaryRel, ""},
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_case_legacy", catalog.SystemViewRel, publicationLegacyViewDefWithoutCaseMode(t,
					"CREATE VIEW v_case_legacy AS SELECT id FROM Base")},
			},
			tableNames: []string{"base", "v_case_legacy"},
			wantError:  "table-level publication for database 'view_pub_db' must include table 'Base' required by view 'v_case_legacy'",
		},
		{
			name:            "legacy case-distinct view over the matching-case member is accepted on a mode-0 session",
			sessionCaseMode: func(v int64) *int64 { return &v }(0),
			relations: [][]interface{}{
				{"Base", catalog.SystemOrdinaryRel, ""},
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_case_legacy", catalog.SystemViewRel, publicationLegacyViewDefWithoutCaseMode(t,
					"CREATE VIEW v_case_legacy AS SELECT id FROM Base")},
			},
			tableNames:   []string{"Base", "v_case_legacy"},
			wantAccepted: true,
		},
		{
			name:            "legacy case-distinct view keeps the mode-1 session's own resolution",
			sessionCaseMode: func(v int64) *int64 { return &v }(1),
			relations: [][]interface{}{
				{"Base", catalog.SystemOrdinaryRel, ""},
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_case_legacy", catalog.SystemViewRel, publicationLegacyViewDefWithoutCaseMode(t,
					"CREATE VIEW v_case_legacy AS SELECT id FROM Base")},
			},
			tableNames:   []string{"base", "v_case_legacy"},
			wantAccepted: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
			bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
				newMrsForPublicationRelations(testCase.relations)
			targets := testCase.targets
			if targets == nil {
				targets = generalSubscriberAccounts
			}
			// Catalog rows of every system relation the fixtures reference:
			// per-account relations exist for the system and a general
			// account, sys-only relations only under the system account.
			addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID, uint32(1))
			addPublicationSystemRelation(bh, "mo_catalog", "mo_tables", catalog.SystemOrdinaryRel, sysAccountID)
			addPublicationSystemRelation(bh, "mo_catalog", "mo_view_refresh", catalog.SystemClusterRel, sysAccountID)
			addPublicationSystemRelation(bh, "mo_catalog", "mo_account", catalog.SystemOrdinaryRel, sysAccountID)
			addPublicationSystemRelation(bh, "mo_task", "sys_mo_usage", catalog.SystemOrdinaryRel, sysAccountID)

			sessionCaseMode := int64(1)
			if testCase.sessionCaseMode != nil {
				sessionCaseMode = *testCase.sessionCaseMode
			}
			err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase, testCase.tableNames, targets, sessionCaseMode)
			if testCase.wantAccepted {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, testCase.wantError)
		})
	}
}

func TestValidatePublicationTableScopeCatalogErrors(t *testing.T) {
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)

	t.Run("catalog read failure propagates", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		wantErr := errors.New("catalog unavailable")
		bh.sql2err[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] = wantErr

		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase, []string{"v_only"}, generalSubscriberAccounts, int64(1))
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("system relation catalog failure propagates", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_system AS SELECT user_name FROM mo_catalog.mo_user",
			[]plan.ViewDependency{publicationDependency("mo_catalog", "mo_user", nil)})
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{{"v_system", catalog.SystemViewRel, viewDef}})
		wantErr := errors.New("system catalog unavailable")
		bh.sql2err[publicationSystemRelationSQL("mo_catalog", "mo_user")] = wantErr

		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase, []string{"v_system"}, generalSubscriberAccounts, int64(1))
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("account must be bound", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		err := validatePublicationTableScope(context.Background(), bh, testPublicationScopeDatabase, []string{"v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
	})

	t.Run("malformed persisted view definition fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{
				{"v_broken", catalog.SystemViewRel, "{"},
			})
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase, []string{"v_broken"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
	})
}

// TestValidatePublicationTableScopeBindsStoredDefinitions proves an accepted
// name and membership walk is followed by an authoritative binding of every
// published view: a base table dropped and recreated with an incompatible
// schema keeps the relation names publishable while the stored view definition
// can no longer bind, and that state must reject the publication and its
// subscription admission instead of creating a subscription that fails on
// every query.
func TestValidatePublicationTableScopeBindsStoredDefinitions(t *testing.T) {
	viewOnlyDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "base", nil),
		})
	nestedViewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_nested AS SELECT id FROM v_only",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "v_only", nil),
		})
	rebindableRelations := newMrsForPublicationRelations([][]interface{}{
		{"base", catalog.SystemOrdinaryRel, ""},
		{"v_only", catalog.SystemViewRel, viewOnlyDef},
		{"v_nested", catalog.SystemViewRel, nestedViewDef},
	})

	newScopeBh := func(t *testing.T) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] = rebindableRelations
		return bh
	}
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)

	t.Run("nested and direct views are bound after the closure walk", func(t *testing.T) {
		bh := newScopeBh(t)
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base", "v_nested", "v_only"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		executed := joinStrings(bh.executedSQLs)
		require.Contains(t, executed, publicationViewBindingSQL(testPublicationScopeDatabase, "v_nested"))
		require.Contains(t, executed, publicationViewBindingSQL(testPublicationScopeDatabase, "v_only"))
	})

	t.Run("a replaced base table with incompatible columns rejects the publication", func(t *testing.T) {
		bh := newScopeBh(t)
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")] =
			errors.New("invalid input: column id does not exist")
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_only': its stored definition no longer binds the published relations: invalid input: column id does not exist")
	})

	t.Run("a broken nested view rejects before any deeper probe succeeds", func(t *testing.T) {
		bh := newScopeBh(t)
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_nested")] =
			errors.New("invalid input: column id does not exist")
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base", "v_nested", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "cannot publish view 'v_nested'")
	})

	t.Run("binding probe failures fail closed for unknown probe errors", func(t *testing.T) {
		bh := newScopeBh(t)
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")] =
			errors.New("catalog read cancelled")
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "cannot publish view 'v_only'")
		require.ErrorContains(t, err, "catalog read cancelled")
		require.NotContains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_pubs")
		require.NotContains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_subs")
	})

	// The probe failures below are execution- and transaction-layer
	// conditions, not properties of the stored definition: they must leave
	// the validator with their original identity and code so the DDL layer
	// can retry or report them, and a validation that failed this way must
	// never fall through into catalog writes.
	assertProbeErrorPropagates := func(t *testing.T, injected error) error {
		bh := newScopeBh(t)
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")] =
			injected
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.NotContains(t, err.Error(), "cannot publish view")
		require.NotContains(t, err.Error(), "no longer binds the published relations")
		require.NotContains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_pubs")
		require.NotContains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_subs")
		return err
	}

	t.Run("context cancellation keeps its identity from the probe", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, context.Canceled)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("context deadlines keep their identity from the probe", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, context.DeadlineExceeded)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("query interruption keeps its code from the probe", func(t *testing.T) {
		injected := moerr.NewQueryInterrupted(ctx)
		err := assertProbeErrorPropagates(t, injected)
		require.Same(t, injected, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("transaction retry keeps its code from the probe", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, moerr.NewTxnNeedRetry(ctx))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("transaction retry after a definition change keeps its code", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, moerr.NewTxnNeedRetryWithDefChanged(ctx))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("transaction write conflicts keep their code from the probe", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, moerr.NewTxnWriteConflict(ctx, "write conflict on base"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("wrapped infrastructure errors keep their code and identity", func(t *testing.T) {
		inner := moerr.NewTxnNeedRetry(ctx)
		err := assertProbeErrorPropagates(t,
			fmt.Errorf("binding probe failed: %w", inner))
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr)
		require.Equal(t, moerr.ErrTxnNeedRetry, moErr.ErrorCode())
		require.ErrorIs(t, err, inner)
	})

	t.Run("deadlock aborts propagate through the rollback classification", func(t *testing.T) {
		err := assertProbeErrorPropagates(t, moerr.NewDeadLockDetected(ctx))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDeadLockDetected))
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("membership errors keep precedence over binding failures", func(t *testing.T) {
		bh := newScopeBh(t)
		// The probe would fail as well; the missing membership must be reported.
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")] =
			errors.New("invalid input: column id does not exist")
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'")
	})

	t.Run("plain tables without views bind nothing", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
			})
		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"base"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.NotContains(t, joinStrings(bh.executedSQLs), "select * from")
	})
}

func TestParsePublicationViewRelationRefs(t *testing.T) {
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)

	for _, testCase := range []struct {
		name string
		stmt string
		want []publicationRelationRef
	}{
		{
			name: "plain unqualified reference",
			stmt: "CREATE VIEW v AS SELECT id FROM base",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "aliases are not relations",
			stmt: "CREATE VIEW v AS SELECT b.id FROM base AS b",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "CTE names are not relations",
			stmt: "CREATE VIEW v AS WITH src AS (SELECT id FROM base) SELECT id FROM src",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "recursive CTE still records only physical sources",
			stmt: "CREATE VIEW v AS WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 3) SELECT n FROM seq JOIN base ON seq.n = base.id",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "derived table sources are collected",
			stmt: "CREATE VIEW v AS SELECT a FROM (SELECT a FROM base) AS derived",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "subqueries in expressions are collected",
			stmt: "CREATE VIEW v AS SELECT (SELECT max(a) FROM t1) AS m FROM t2 WHERE a IN (SELECT a FROM t3)",
			want: []publicationRelationRef{
				{database: testPublicationScopeDatabase, relation: "t2"},
				{database: testPublicationScopeDatabase, relation: "t1"},
				{database: testPublicationScopeDatabase, relation: "t3"},
			},
		},
		{
			name: "qualified references keep their database",
			stmt: "CREATE VIEW v AS SELECT a FROM other_db.src",
			want: []publicationRelationRef{{database: "other_db", relation: "src", qualified: true}},
		},
		{
			name: "unqualified dual is not a relation",
			stmt: "CREATE VIEW v AS SELECT 1 FROM dual",
			want: []publicationRelationRef{},
		},
		{
			name: "constant view has no relations",
			stmt: "CREATE VIEW v AS SELECT 1 AS one",
			want: []publicationRelationRef{},
		},
		{
			name: "alter view form is parsed the same way",
			stmt: "ALTER VIEW v AS SELECT a FROM base",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "snapshot time travel reference is marked foreign",
			stmt: "CREATE VIEW v AS SELECT a FROM src {snapshot = 'sp1'}",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "src", foreign: true}},
		},
		{
			name: "timestamp time travel reference is marked foreign",
			stmt: "CREATE VIEW v AS SELECT a FROM src {timestamp = '2026-01-01 00:00:00'}",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "src", foreign: true}},
		},
		{
			name: "unused CTE body is not descended into",
			stmt: "CREATE VIEW v AS WITH unused AS (SELECT id FROM base) SELECT 1 AS one",
			want: []publicationRelationRef{},
		},
		{
			name: "only the used CTE of a mixed WITH clause is descended into",
			stmt: "CREATE VIEW v AS WITH unused AS (SELECT id FROM base), used AS (SELECT 2 AS two) SELECT * FROM used",
			want: []publicationRelationRef{},
		},
		{
			name: "chained CTEs walk the used chain transitively",
			stmt: "CREATE VIEW v AS WITH a AS (SELECT id FROM base), b AS (SELECT id FROM a) SELECT * FROM b",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "CTE referenced through a derived table is resolved and descended into",
			stmt: "CREATE VIEW v AS WITH c AS (SELECT id FROM base) SELECT * FROM (SELECT * FROM c) AS dt",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "CTE body with its own nested WITH resolves both levels",
			stmt: "CREATE VIEW v AS WITH a AS (WITH inner_cte AS (SELECT id FROM base) SELECT * FROM inner_cte) SELECT * FROM a",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "a CTE name shadowing a later declaration still resolves the physical table",
			stmt: "CREATE VIEW v AS WITH a AS (SELECT id FROM b), b AS (SELECT 1 AS id) SELECT id FROM a",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "b"}},
		},
		{
			name: "declaration order makes the earlier name a CTE reference",
			stmt: "CREATE VIEW v AS WITH b AS (SELECT 1 AS id), a AS (SELECT id FROM b) SELECT id FROM a",
			want: []publicationRelationRef{},
		},
		{
			name: "sibling CTEs stay isolated when only one is used",
			stmt: "CREATE VIEW v AS WITH x AS (SELECT 1 AS id), y AS (SELECT id FROM base) SELECT id FROM x",
			want: []publicationRelationRef{},
		},
		{
			name: "an unused CTE inside a derived table binds no relation",
			stmt: "CREATE VIEW v AS SELECT * FROM (WITH unused AS (SELECT id FROM base) SELECT 1 AS one) AS d",
			want: []publicationRelationRef{},
		},
		{
			name: "a used CTE inside a derived table binds its relations",
			stmt: "CREATE VIEW v AS SELECT * FROM (WITH used AS (SELECT id FROM base) SELECT * FROM used) AS d",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "base"}},
		},
		{
			name: "a later-declared sibling inside a derived table masks the outer CTE of the same name",
			stmt: "CREATE VIEW v AS WITH b AS (SELECT 9 AS id) SELECT * FROM (WITH a AS (SELECT id FROM b), b AS (SELECT 1 AS id) SELECT id FROM a) AS d",
			want: []publicationRelationRef{{database: testPublicationScopeDatabase, relation: "b"}},
		},
		{
			name: "an unmasked outer CTE stays reachable from a derived table's member",
			stmt: "CREATE VIEW v AS WITH ob AS (SELECT 1 AS id) SELECT * FROM (WITH a AS (SELECT id FROM ob) SELECT id FROM a) AS d",
			want: []publicationRelationRef{},
		},
		{
			name: "an inner predecessor resolves before the masked outer CTE",
			stmt: "CREATE VIEW v AS WITH b AS (SELECT 9 AS id) SELECT * FROM (WITH b AS (SELECT 1 AS id), a AS (SELECT id FROM b) SELECT id FROM a) AS d",
			want: []publicationRelationRef{},
		},
		{
			name: "a CTE named dual never captures the unqualified dual value scan",
			stmt: "CREATE VIEW v AS WITH dual AS (SELECT id FROM base) SELECT 1 AS one FROM dual",
			want: []publicationRelationRef{},
		},
		{
			name: "a qualified db.dual reference stays a real table dependency",
			stmt: "CREATE VIEW v AS SELECT id FROM other_db.dual",
			want: []publicationRelationRef{{database: "other_db", relation: "dual", qualified: true}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			refs, err := parsePublicationViewRelationRefs(
				ctx, &plan.ViewData{Stmt: testCase.stmt, DefaultDatabase: testPublicationScopeDatabase},
				testPublicationScopeDatabase, "v", int64(1))
			require.NoError(t, err)
			require.Equal(t, testCase.want, refs)
		})
	}

	t.Run("parse error propagates", func(t *testing.T) {
		_, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{Stmt: "CREATE VIEW", DefaultDatabase: testPublicationScopeDatabase},
			testPublicationScopeDatabase, "v", int64(1))
		require.Error(t, err)
	})

	t.Run("multiple statements are rejected", func(t *testing.T) {
		_, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{Stmt: "CREATE VIEW v AS SELECT 1; CREATE VIEW w AS SELECT 2", DefaultDatabase: testPublicationScopeDatabase},
			testPublicationScopeDatabase, "v", int64(1))
		require.Error(t, err)
	})

	t.Run("non view statements are rejected", func(t *testing.T) {
		_, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{Stmt: "CREATE TABLE t (a int)", DefaultDatabase: testPublicationScopeDatabase},
			testPublicationScopeDatabase, "v", int64(1))
		require.Error(t, err)
	})

	// A legacy definition imported with no persisted case mode resolves its
	// references through the validating session's own lower_case_table_names
	// value, the same mode the DDL session and every subscriber session
	// resolve with: a mode-0 session certifies the relation object named at
	// its source case, while the mode-1 session lowercases exactly as its
	// subscribers do. The rows this walk certifies must name the relation
	// the subscriber's FIRST bind resolves, never a guessed-case twin.
	t.Run("legacy definition without persisted case mode follows the mode-0 session context", func(t *testing.T) {
		refs, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{Stmt: "CREATE VIEW v AS SELECT id FROM Base", DefaultDatabase: testPublicationScopeDatabase},
			testPublicationScopeDatabase, "v", int64(0))
		require.NoError(t, err)
		require.Equal(t, []publicationRelationRef{
			{database: testPublicationScopeDatabase, relation: "Base"},
		}, refs, "a mode-0 session certifies the source-case relation name")
	})

	t.Run("legacy definition without persisted case mode follows the mode-1 session context", func(t *testing.T) {
		refs, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{Stmt: "CREATE VIEW v AS SELECT id FROM Base", DefaultDatabase: testPublicationScopeDatabase},
			testPublicationScopeDatabase, "v", int64(1))
		require.NoError(t, err)
		require.Equal(t, []publicationRelationRef{
			{database: testPublicationScopeDatabase, relation: "base"},
		}, refs, "a mode-1 session certifies the lowercased relation name")
	})

	t.Run("persisted creation-time mode wins over the validating session's mode", func(t *testing.T) {
		refs, err := parsePublicationViewRelationRefs(
			ctx, &plan.ViewData{
				Stmt:                "CREATE VIEW v AS SELECT id FROM Base",
				DefaultDatabase:     testPublicationScopeDatabase,
				LowerCaseTableNames: func(v int64) *int64 { return &v }(1),
			},
			testPublicationScopeDatabase, "v", int64(0))
		require.NoError(t, err)
		require.Equal(t, []publicationRelationRef{
			{database: testPublicationScopeDatabase, relation: "base"},
		}, refs, "a definition recorded at mode 1 keeps its own resolution on any session")
	})
}

// unknownSelectStatement is a SelectStatement form the shared remap_db walker
// does not handle; using it directly proves the walker marks it unsupported.
type unknownSelectStatement struct {
	tree.SelectStatement
}

func TestRemapCollectorReportsUnsupportedForms(t *testing.T) {
	refs := make([]publicationRelationRef, 0, 1)
	unsupported := false
	remapDbInSelect(&tree.Select{Select: unknownSelectStatement{}}, remapDbContext{
		lowerCaseTableNames: 1,
		unsupported:         &unsupported,
		collectTableName: func(name *tree.TableName) {
			refs = append(refs, publicationRelationRef{relation: string(name.ObjectName)})
		},
	})
	require.True(t, unsupported, "the shared walker must report the unknown select form")
	require.Empty(t, refs)
}

func TestIsPublicationSystemDatabase(t *testing.T) {
	require.True(t, isPublicationSystemDatabase("mo_catalog"))
	require.True(t, isPublicationSystemDatabase("MO_CATALOG"))
	require.True(t, isPublicationSystemDatabase("information_schema"))
	require.True(t, isPublicationSystemDatabase("system"))
	require.True(t, isPublicationSystemDatabase("system_metrics"))
	require.True(t, isPublicationSystemDatabase("mysql"))
	require.False(t, isPublicationSystemDatabase(testPublicationScopeDatabase))
	require.False(t, isPublicationSystemDatabase(""))
}

// TestCreatePublicationValidatesViewScope proves the validator is part of the
// CREATE PUBLICATION transaction: an invalid view scope fails before any
// mo_pubs or mo_subs row is written.
func TestCreatePublicationValidatesViewScope(t *testing.T) {
	viewOnlyDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "base", nil),
		})

	lockStub := gostub.StubFunc(&lockPublicationDatabase, "", nil)
	defer lockStub.Reset()

	bh := &backgroundExecTest{}
	bh.init()
	// getAccounts
	bh.sql2result[getAccountIdNamesSql] = newMrsForPublicationColumns(
		[]string{"account_id", "account_name", "status", "version", "suspended_time"},
		[][]interface{}{
			{int64(0), "sys", "open", uint64(1), nil},
			{int64(1), "acc1", "open", uint64(1), nil},
		})
	// resolve publication database
	sql, err := getSqlForGetDbIdAndType(context.Background(), testPublicationScopeDatabase, true, sysAccountID)
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForPublicationColumns(
		[]string{"dat_id", "dat_type"}, [][]interface{}{{uint64(1), ""}})
	// show tables
	bh.sql2result["show tables from "+sqlquote.Ident(testPublicationScopeDatabase)] =
		newMrsForPublicationColumns([]string{"Tables_in_" + testPublicationScopeDatabase},
			[][]interface{}{{"base"}, {"v_only"}})
	// relations of the publication database
	bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
		newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, viewOnlyDef},
		})

	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	stmts, err := mysql.Parse(ctx,
		fmt.Sprintf("create publication p_view database %s table v_only account acc1", testPublicationScopeDatabase), 1)
	require.NoError(t, err)
	defer freeStatements(stmts)

	err = createPublication(ctx, bh, stmts[0].(*tree.CreatePublication), int64(1))
	require.Error(t, err)
	require.ErrorContains(t, err,
		"table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'")
	for _, executed := range bh.executedSQLs {
		require.NotContains(t, executed, "insert into mo_catalog.mo_pubs")
		require.NotContains(t, executed, "insert into mo_catalog.mo_subs")
	}
}

// TestCreatePublicationAcceptsCompleteViewScope proves a view whose dependency
// closure is inside the table list still creates the publication.
func TestCreatePublicationAcceptsCompleteViewScope(t *testing.T) {
	viewOnlyDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "base", nil),
		})

	lockStub := gostub.StubFunc(&lockPublicationDatabase, "", nil)
	defer lockStub.Reset()

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getAccountIdNamesSql] = newMrsForPublicationColumns(
		[]string{"account_id", "account_name", "status", "version", "suspended_time"},
		[][]interface{}{
			{int64(0), "sys", "open", uint64(1), nil},
			{int64(1), "acc1", "open", uint64(1), nil},
		})
	sql, err := getSqlForGetDbIdAndType(context.Background(), testPublicationScopeDatabase, true, sysAccountID)
	require.NoError(t, err)
	bh.sql2result[sql] = newMrsForPublicationColumns(
		[]string{"dat_id", "dat_type"}, [][]interface{}{{uint64(1), ""}})
	bh.sql2result["show tables from "+sqlquote.Ident(testPublicationScopeDatabase)] =
		newMrsForPublicationColumns([]string{"Tables_in_" + testPublicationScopeDatabase},
			[][]interface{}{{"base"}, {"v_only"}})
	bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
		newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, viewOnlyDef},
		})
	// getSubInfosFromPub: no existing subscription rows.
	bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'p_view'"] =
		newMrsForPublicationColumns(nil, nil)

	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	stmts, err := mysql.Parse(ctx,
		fmt.Sprintf("create publication p_view database %s table base,v_only account acc1", testPublicationScopeDatabase), 1)
	require.NoError(t, err)
	defer freeStatements(stmts)

	err = createPublication(ctx, bh, stmts[0].(*tree.CreatePublication), int64(1))
	require.NoError(t, err)
	require.Contains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_pubs")
}

func joinStrings(values []string) string {
	out := ""
	for _, value := range values {
		out += value + "\n"
	}
	return out
}

// TestPublicationViewRelationRefsSharedSystemRelations proves the persisted
// dependency of a general publisher's view over a shared system relation (the
// binder records the system account as its owner) is not misclassified as
// bound to another account, while a foreign user relation still is.
func TestPublicationViewRelationRefsSharedSystemRelations(t *testing.T) {
	generalPublisher := uint32(1)
	for _, shared := range []plan.ViewDependency{
		publicationDependency("system", "statement_info", nil),
		publicationDependency("system_metrics", "metric", nil),
		publicationDependency("mo_catalog", "mo_view_refresh", nil),
	} {
		viewDef := publicationViewDefWithDependencies(t,
			fmt.Sprintf("CREATE VIEW v AS SELECT 1 FROM %s.%s", shared.DatabaseName, shared.RelationName),
			[]plan.ViewDependency{shared})
		refs, err := publicationViewRelationRefs(context.Background(), viewDef, testPublicationScopeDatabase, generalPublisher, "v", int64(1))
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.False(t, refs[0].foreign,
			"%s.%s must stay publishable for a general publisher", shared.DatabaseName, shared.RelationName)
	}

	viewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v AS SELECT a FROM src",
		[]plan.ViewDependency{
			publicationDependency(testPublicationScopeDatabase, "src", func(dependency *plan.ViewDependency) {
				dependency.AccountID = sysAccountID + 7
			}),
		})
	refs, err := publicationViewRelationRefs(context.Background(), viewDef, testPublicationScopeDatabase, generalPublisher, "v", int64(1))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.True(t, refs[0].foreign, "a user relation of another account must stay foreign")
}

// TestValidatePublicationTableScopeSharedSystemRelationFromGeneralPublisher is
// the positive end-to-end form of the shared-system-relation rule: a general
// tenant publishes a view over system.statement_info to another general
// account, and the subscription can resolve the relation per account.
func TestValidatePublicationTableScopeSharedSystemRelationFromGeneralPublisher(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	ctx := defines.AttachAccount(context.Background(), uint32(1), rootID, moAdminRoleID)
	viewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_stmt AS SELECT count(*) FROM system.statement_info",
		[]plan.ViewDependency{publicationDependency("system", "statement_info", nil)})
	bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 1)] =
		newMrsForPublicationRelations([][]interface{}{{"v_stmt", catalog.SystemViewRel, viewDef}})
	addPublicationSystemRelation(bh, "system", "statement_info", catalog.SystemOrdinaryRel, sysAccountID, uint32(1))

	err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase, []string{"v_stmt"}, generalSubscriberAccounts, int64(1))
	require.NoError(t, err)
}

// TestPublicationSystemRelationAvailableForAllAccountsTarget proves an
// ACCOUNT ALL target set is judged for general subscribers, including accounts
// created after the publication: a sys-only relation is unavailable, a
// per-account relation is available.
func TestPublicationSystemRelationAvailableForAllAccountsTarget(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	addPublicationSystemRelation(bh, "mo_catalog", "mo_account", catalog.SystemOrdinaryRel, sysAccountID)
	addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID, uint32(1))
	allAccounts := []uint32{publicationTargetAnyGeneralAccount}

	available, err := publicationSystemRelationAvailable(context.Background(), bh, "mo_catalog", "mo_account", allAccounts)
	require.NoError(t, err)
	require.False(t, available, "mo_account cannot resolve inside a general subscriber")

	available, err = publicationSystemRelationAvailable(context.Background(), bh, "mo_catalog", "mo_user", allAccounts)
	require.NoError(t, err)
	require.True(t, available, "mo_user resolves per subscriber account")
}

// TestPublicationSystemRelationAvailableRequiresTargetOwnership proves the
// availability of a per-account system relation follows the target account's
// own catalog row: a row owned by another account, present in some other
// account or held only by general accounts, cannot make the relation resolve
// for a target that owns no row of it.
func TestPublicationSystemRelationAvailableRequiresTargetOwnership(t *testing.T) {
	ctx := context.Background()

	t.Run("the target account's own row resolves the relation", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID, uint32(1), uint32(2))
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_user", []uint32{1, 2})
		require.NoError(t, err)
		require.True(t, available)
	})

	t.Run("a row of another account does not satisfy the target", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID, uint32(1))
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_user", []uint32{2})
		require.NoError(t, err)
		require.False(t, available, "account 2 owns no mo_user row and cannot resolve the relation")
	})

	t.Run("a mixed target set needs a row for every account", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID, uint32(1))
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_user", []uint32{2, sysAccountID})
		require.NoError(t, err)
		require.False(t, available, "account 2 has no row even though the system account does")
	})

	t.Run("a sys target needs the system account's own row", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, uint32(1))
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_user", []uint32{sysAccountID})
		require.NoError(t, err)
		require.False(t, available, "a general-account-only row must not satisfy the system account")
	})

	t.Run("a shared cluster relation resolves for every target", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		addPublicationSystemRelation(bh, "mo_catalog", "mo_view_refresh", catalog.SystemClusterRel, sysAccountID)
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_view_refresh", []uint32{1, 2})
		require.NoError(t, err)
		require.True(t, available)
	})

	t.Run("a publication judging a view over a mismatched-owner relation is rejected", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_user AS SELECT user_name FROM mo_catalog.mo_user",
			[]plan.ViewDependency{publicationDependency("mo_catalog", "mo_user", nil)})
		ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{{"v_user", catalog.SystemViewRel, viewDef}})
		// The relation exists only for account 1; the publication targets
		// account 2, which owns no row of it. The binding probe stays silent
		// (the source account can bind the view): only the ownership gap
		// rejects the publication.
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, uint32(1))

		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"v_user"}, []uint32{2}, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_user': system relation 'mo_catalog.mo_user' is not available to its subscriber accounts")
	})
}

// TestCheckSubscriptionValidForCreateAdmitsExplicitScope proves subscription
// creation revalidates an explicit table list against the subscribing
// account: a publication whose view scope is broken for this subscriber is
// rejected up front and a complete list is admitted, while the runtime
// metadata check used by ordinary resolves keeps working for both, so one
// broken view cannot fail queries of healthy tables in the same
// subscription.
func TestCheckSubscriptionValidForCreateAdmitsExplicitScope(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tenant := &TenantInfo{
		Tenant:        "acc1",
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      1,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses := newSes(nil, ctrl)
	ses.tenant = tenant

	subscriberCtx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

	publisher := &pubsub.PubInfo{
		PubAccountId:   0,
		PubAccountName: "sys",
		PubName:        "pub1",
		DbName:         testPublicationScopeDatabase,
		DbId:           7,
		TablesStr:      "v_only",
		SubAccountsStr: "acc1",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	viewOnlyDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{publicationDependency(testPublicationScopeDatabase, "base", nil)})
	publisherRelations := newMrsForPublicationRelations([][]interface{}{
		{"base", catalog.SystemOrdinaryRel, ""},
		{"v_only", catalog.SystemViewRel, viewOnlyDef},
	})

	newAdmissionBh := func(t *testing.T) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "sys")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(0), "open"}})
		// the persisted database identity resolves to the publisher's own
		// catalog (account 0) unless a subtest overrides it
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(0), ""}})
		return bh
	}

	t.Run("broken explicit list is rejected at subscription creation", func(t *testing.T) {
		bh := newAdmissionBh(t)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 0)] = publisherRelations

		_, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'")
	})

	t.Run("complete explicit list is admitted", func(t *testing.T) {
		publisher.TablesStr = "base,v_only"
		defer func() { publisher.TablesStr = "v_only" }()
		bh := newAdmissionBh(t)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 0)] = publisherRelations

		subMeta, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, "base,v_only", subMeta.Tables)
		// Admission binds the published view's stored definition in the same
		// transaction before the subscription database is created.
		require.Contains(t, joinStrings(bh.executedSQLs),
			publicationViewBindingSQL(testPublicationScopeDatabase, "v_only"))
	})

	t.Run("runtime metadata check stays usable for a broken list", func(t *testing.T) {
		// checkSubscriptionValidCommon serves ordinary resolves; it must keep
		// answering so a view the publisher broke after subscription creation
		// fails only its own queries, never healthy tables in the same
		// subscription.
		bh := newAdmissionBh(t)

		subMeta, err := checkSubscriptionValidCommon(context.Background(), ses, "s_view", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, "v_only", subMeta.Tables)
		require.NotContains(t, joinStrings(bh.executedSQLs), "from mo_catalog.mo_tables where reldatabase")
	})

	t.Run("database-wide publication skips admission validation", func(t *testing.T) {
		publisher.TablesStr = "*"
		defer func() { publisher.TablesStr = "v_only" }()
		bh := newAdmissionBh(t)

		subMeta, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, "*", subMeta.Tables)
		require.NotContains(t, joinStrings(bh.executedSQLs), "from mo_catalog.mo_tables where reldatabase")
	})

	t.Run("admission validates the physical source account, not the publisher", func(t *testing.T) {
		// The publication owner carries a same-named database with completely
		// different objects: a read against it must fail, so admission can
		// only pass by resolving the persisted physical source identity.
		publisher.TablesStr = "base,v_only"
		defer func() { publisher.TablesStr = "v_only" }()
		physicalOwnerView := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
			[]plan.ViewDependency{
				publicationDependency(testPublicationScopeDatabase, "base", func(dependency *plan.ViewDependency) {
					dependency.AccountID = 2
				}),
			})
		physicalSourceRelations := newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, physicalOwnerView},
		})
		bh := newAdmissionBh(t)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 0)] =
			newMrsForPublicationRelations([][]interface{}{{"decoy", catalog.SystemOrdinaryRel, ""}})
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 2)] = physicalSourceRelations
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(2), ""}})

		subMeta, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, "base,v_only", subMeta.Tables)
		// The subscription metadata itself resolves the persisted physical
		// source account: query, SHOW and clone resolution route through it.
		require.Equal(t, int32(2), subMeta.AccountId)
	})

	t.Run("a broken physical source is rejected even when the publisher's copy looks healthy", func(t *testing.T) {
		// The publisher-owned same-named database holds a healthy variant of
		// the view (no dependencies); the physical source account owns the
		// broken variant. Only reading the physical catalog rejects this.
		healthyPublisherRelations := newMrsForPublicationRelations([][]interface{}{
			{"v_only", catalog.SystemViewRel, publicationLegacyViewDef(t,
				"CREATE VIEW v_only AS SELECT 1 AS one")},
		})
		physicalOwnerView := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
			[]plan.ViewDependency{
				publicationDependency(testPublicationScopeDatabase, "base", func(dependency *plan.ViewDependency) {
					dependency.AccountID = 2
				}),
			})
		physicalSourceRelations := newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, physicalOwnerView},
		})
		bh := newAdmissionBh(t)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 0)] = healthyPublisherRelations
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 2)] = physicalSourceRelations
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(2), ""}})

		_, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.Error(t, err, "the physical source is broken even though the publisher's copy is healthy")
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' must include table 'base' required by view 'v_only'")
	})

	t.Run("a stored definition that no longer binds is rejected at subscription creation", func(t *testing.T) {
		// The base table was replaced under the same name with an
		// incompatible schema after the publication was created: names and
		// membership still pass, and admission must bind the stored view
		// definition - exactly like CREATE/ALTER PUBLICATION - so the
		// subscription cannot be created unusable.
		publisher.TablesStr = "base,v_only"
		defer func() { publisher.TablesStr = "v_only" }()
		bh := newAdmissionBh(t)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 0)] = publisherRelations
		bh.sql2err[publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")] =
			errors.New("invalid input: column id does not exist")

		_, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_view", "sys", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_only': its stored definition no longer binds the published relations: invalid input: column id does not exist")
		require.NotContains(t, joinStrings(bh.executedSQLs), "insert into mo_catalog.mo_subs")
	})
}

// TestCurrentFormatViewDefinitionWithoutDependenciesRoundTrip proves a view
// created by the current binder serializes an empty dependency set away
// (Dependencies carries json omitempty), so such views - constants, views
// whose relations all live behind unused CTEs - take the parse path and are
// validated with the same usage-aware collector.
func TestCurrentFormatViewDefinitionWithoutDependenciesRoundTrip(t *testing.T) {
	encoded, err := json.Marshal(plan.ViewData{
		Stmt:                "CREATE VIEW v AS WITH unused AS (SELECT id FROM base) SELECT 1 AS one",
		DefaultDatabase:     testPublicationScopeDatabase,
		LowerCaseTableNames: func(v int64) *int64 { return &v }(1),
		Dependencies:        []plan.ViewDependency{},
	})
	require.NoError(t, err)

	var roundTripped plan.ViewData
	require.NoError(t, json.Unmarshal(encoded, &roundTripped))
	require.Nil(t, roundTripped.Dependencies,
		"an empty dependency set must not survive serialization")

	refs, err := publicationViewRelationRefs(
		context.Background(), string(encoded), testPublicationScopeDatabase, sysAccountID, "v", int64(1))
	require.NoError(t, err)
	require.Empty(t, refs)
}

// TestDoAlterPublicationAccountChangeValidatesInheritedScope proves an
// accounts-only ALTER validates the inherited explicit table list with the
// final account set: widening to a subscriber that cannot resolve a view is
// rejected before mo_pubs changes, and a complete list still succeeds.
func TestDoAlterPublicationAccountChangeValidatesInheritedScope(t *testing.T) {
	lockStub := gostub.StubFunc(&lockPublicationDatabase, "", nil)
	defer lockStub.Reset()
	invalidateStub := gostub.StubFunc(&invalidatePublicationViewMetadata, nil)
	defer invalidateStub.Reset()

	viewOnlyDef := publicationViewDefInDatabase(t, "db1",
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{publicationDependency("db1", "base", nil)})
	publisher := &pubsub.PubInfo{
		PubAccountId:   0,
		PubAccountName: "sys",
		PubName:        "pub1",
		DbName:         "db1",
		DbId:           7,
		TablesStr:      "v_only",
		SubAccountsStr: "acc1",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)

	newAlterSession := func(t *testing.T) (*Session, *backgroundExecTest) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses := newSes(nil, ctrl)
		ses.tenant = tenant
		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		t.Cleanup(bhStub.Reset)
		bh.sql2result[getAccountIdNamesSql] = newMrsForPublicationColumns(
			[]string{"account_id", "account_name", "status", "version", "suspended_time"},
			[][]interface{}{
				{int64(0), "sys", "open", uint64(1), nil},
				{int64(1), "acc1", "open", uint64(1), nil},
				{int64(2), "acc2", "open", uint64(1), nil},
				{int64(3), "acc3", "open", uint64(1), nil},
			})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), "db1")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(0), ""}})
		bh.sql2result[publicationRelationsSQL("db1", 0)] = newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, viewOnlyDef},
		})
		return ses, bh
	}

	t.Run("account add rejects an inherited incomplete view scope", func(t *testing.T) {
		ses, bh := newAlterSession(t)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account add acc2", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'db1' must include table 'base' required by view 'v_only'")
		require.NotContains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("account drop on a broken inherited scope is allowed", func(t *testing.T) {
		// The publisher has since broken a published view; revoking an
		// account must still succeed, because an authorization reduction
		// cannot expose the broken view to a new subscriber.
		ses, bh := newAlterSession(t)
		bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'pub1'"] =
			newMrsForPublicationColumns(nil, nil)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account drop acc1", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.NoError(t, err)
		require.Contains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("account set to a shrinking subset of subscribers is allowed", func(t *testing.T) {
		publisher.SubAccountsStr = "acc1,acc2"
		defer func() { publisher.SubAccountsStr = "acc1" }()
		ses, bh := newAlterSession(t)
		bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'pub1'"] =
			newMrsForPublicationColumns(nil, nil)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account acc1", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.NoError(t, err)
		require.Contains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("account set on a complete scope succeeds", func(t *testing.T) {
		publisher.TablesStr = "base,v_only"
		defer func() { publisher.TablesStr = "v_only" }()
		ses, bh := newAlterSession(t)
		bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'pub1'"] =
			newMrsForPublicationColumns(nil, nil)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account acc2", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.NoError(t, err)
		require.Contains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("account all with the explicit list covering every account still validates", func(t *testing.T) {
		// The explicit subscription list already names every existing account,
		// so the account maps compare equal; switching to ACCOUNT ALL still
		// extends subscription rights to accounts created after the change, so
		// the reduction rule must not skip the inherited-scope validation.
		publisher.SubAccountsStr = "acc1,acc2"
		defer func() { publisher.SubAccountsStr = "acc1" }()
		ses, bh := newAlterSession(t)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account all", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'db1' must include table 'base' required by view 'v_only'")
		require.NotContains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("account all on a complete scope succeeds", func(t *testing.T) {
		publisher.SubAccountsStr = "acc1,acc2"
		publisher.TablesStr = "base,v_only"
		defer func() {
			publisher.SubAccountsStr = "acc1"
			publisher.TablesStr = "v_only"
		}()
		ses, bh := newAlterSession(t)
		bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'pub1'"] =
			newMrsForPublicationColumns(nil, nil)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account all", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.NoError(t, err)
		require.Contains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})
}

// TestCheckSubscriptionValidCommonResolvesPhysicalSource proves the
// subscription metadata itself carries the account that physically owns the
// published database: admission validates the physical catalog, and every
// runtime resolution of subscription objects (query, SHOW, clone) routes
// relations through this identity, so a publication sourced from a database
// owned by one of its target accounts stops resolving through the publisher.
// The publisher and the physical owner coincide for ordinary publications;
// legacy publications without a persisted database identity keep the
// publisher.
func TestCheckSubscriptionValidCommonResolvesPhysicalSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tenant := &TenantInfo{
		Tenant:        "acc1",
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      1,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses := newSes(nil, ctrl)
	ses.tenant = tenant
	ctx := defines.AttachAccount(context.Background(), uint32(1), rootID, moAdminRoleID)

	publisher := &pubsub.PubInfo{
		PubAccountId:   0,
		PubAccountName: "sys",
		PubName:        "pub1",
		DbName:         testPublicationScopeDatabase,
		DbId:           7,
		TablesStr:      "base,v_only",
		SubAccountsStr: "acc1",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	newCommonBh := func(t *testing.T) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "sys")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(0), "open"}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(2), ""}})
		return bh
	}

	t.Run("the physical owner of the published database is resolved", func(t *testing.T) {
		bh := newCommonBh(t)
		subMeta, err := checkSubscriptionValidCommon(ctx, ses, "s_fsrc", "sys", "pub1", bh)
		require.NoError(t, err)
		require.NotNil(t, subMeta)
		require.Equal(t, int32(2), subMeta.AccountId,
			"subscription resolution must target the account that physically owns the published database")
		require.Contains(t, joinStrings(bh.executedSQLs),
			fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase))
	})

	t.Run("a legacy publication without a persisted identity keeps the publisher", func(t *testing.T) {
		publisher.DbId = 0
		defer func() { publisher.DbId = 7 }()
		bh := newCommonBh(t)

		subMeta, err := checkSubscriptionValidCommon(ctx, ses, "s_legacy", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, int32(0), subMeta.AccountId)
		require.NotContains(t, joinStrings(bh.executedSQLs),
			fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(0), testPublicationScopeDatabase))
	})

	t.Run("a resolution failure of the persisted identity fails closed", func(t *testing.T) {
		bh := newCommonBh(t)
		wantErr := errors.New("catalog unavailable")
		bh.sql2err[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] = wantErr

		_, err := checkSubscriptionValidCommon(ctx, ses, "s_fsrc", "sys", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err, wantErr.Error())
	})
}

// TestPublicationSystemRelationProvisionedForFutureAccounts proves an
// ACCOUNT ALL publication can publish a view over a relation the
// account-initialization contract provisions for every account CREATE
// ACCOUNT creates, before any general account exists to leave catalog
// evidence, and that the sys-only relations stay unavailable. Future
// subscribers resolve such relations from the moment they are created.
func TestPublicationSystemRelationProvisionedForFutureAccounts(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	// A fresh instance: only the system account owns rows.
	addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID)
	addPublicationSystemRelation(bh, "mo_catalog", "mo_account", catalog.SystemOrdinaryRel, sysAccountID)
	allAccounts := []uint32{publicationTargetAnyGeneralAccount}
	ctx := context.Background()

	t.Run("the classification follows the account-initialization contract", func(t *testing.T) {
		require.True(t, publicationSystemRelationProvisionedAtAccountCreation("", "mo_catalog", "mo_user"),
			"CREATE ACCOUNT provisions mo_user for every account it creates")
		require.False(t, publicationSystemRelationProvisionedAtAccountCreation("", "mo_catalog", "mo_account"),
			"mo_account stays with the system account")
	})

	t.Run("a provisioned relation is available before any general account exists", func(t *testing.T) {
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_user", allAccounts)
		require.NoError(t, err)
		require.True(t, available, "mo_user resolves for accounts from the moment they are created")
	})

	t.Run("a sys-only relation stays unavailable on a fresh instance", func(t *testing.T) {
		available, err := publicationSystemRelationAvailable(ctx, bh, "mo_catalog", "mo_account", allAccounts)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("an ACCOUNT ALL publication over a provisioned relation is accepted on a fresh instance", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_sysuser AS SELECT user_name FROM mo_catalog.mo_user",
			[]plan.ViewDependency{publicationDependency("mo_catalog", "mo_user", nil)})
		ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{
				{"v_sysuser", catalog.SystemViewRel, viewDef},
			})
		addPublicationSystemRelation(bh, "mo_catalog", "mo_user", catalog.SystemOrdinaryRel, sysAccountID)

		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"v_sysuser"}, []uint32{publicationTargetAnyGeneralAccount}, int64(1))
		require.NoError(t, err,
			"a fresh system can publish account-provisioned relations to all accounts: the accounts it creates later resolve them")
	})

	t.Run("an ACCOUNT ALL publication over a sys-only relation stays rejected", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_account AS SELECT account_name FROM mo_catalog.mo_account",
			[]plan.ViewDependency{publicationDependency("mo_catalog", "mo_account", nil)})
		ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID)] =
			newMrsForPublicationRelations([][]interface{}{
				{"v_account", catalog.SystemViewRel, viewDef},
			})
		addPublicationSystemRelation(bh, "mo_catalog", "mo_account", catalog.SystemOrdinaryRel, sysAccountID)

		err := validatePublicationTableScope(ctx, bh, testPublicationScopeDatabase,
			[]string{"v_account"}, []uint32{publicationTargetAnyGeneralAccount}, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"system relation 'mo_catalog.mo_account' is not available to its subscriber accounts")
	})
}

func newMrsForPublicationColumns(names []string, rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, name := range names {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(column)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

// TestValidatePublicationTableScopeDefinitionStability proves membership and
// binding are judged against one coherent relation snapshot. The relation
// read, the closure walk and each binding probe are separate statements, RC
// advances the catalog snapshot per statement, and ALTER VIEW needs only the
// shared database lock the publication transaction already holds, so a view
// redefined between those statements could pass membership through its old
// definition and the probe through its new one. The post-probe stability
// re-read detects the change, the whole scope is revalidated against the
// newer definition, and a definition that keeps changing fails closed.
func TestValidatePublicationTableScopeDefinitionStability(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	oldDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id FROM base WHERE id >= 1",
		[]plan.ViewDependency{publicationDependency(db, "base", nil)})
	redefinedDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_only AS SELECT id FROM other WHERE id >= 1",
		[]plan.ViewDependency{publicationDependency(db, "other", nil)})
	relationsWithBase := newMrsForPublicationRelations([][]interface{}{
		{"base", catalog.SystemOrdinaryRel, ""},
		{"other", catalog.SystemOrdinaryRel, ""},
		{"v_only", catalog.SystemViewRel, oldDef},
	})
	relationsWithOther := newMrsForPublicationRelations([][]interface{}{
		{"base", catalog.SystemOrdinaryRel, ""},
		{"other", catalog.SystemOrdinaryRel, ""},
		{"v_only", catalog.SystemViewRel, redefinedDef},
	})

	t.Run("a stable definition set validates with one membership and one stability read", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsWithBase
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql == relationsSQL {
				relationsReads++
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.Equal(t, 2, relationsReads)
	})

	t.Run("a view redefined between the membership walk and the binding probe is revalidated", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsWithBase
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			// An ALTER VIEW commits between the membership read and the
			// binding probe; the stability re-read serves the redefined view.
			if relationsReads == 2 {
				bh.sql2result[relationsSQL] = relationsWithOther
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' must include table 'other' required by view 'v_only'")
	})

	t.Run("a relation dropped between the reads is revalidated against the newer catalog", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsWithBase
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads == 2 {
				bh.sql2result[relationsSQL] = newMrsForPublicationRelations([][]interface{}{
					{"other", catalog.SystemOrdinaryRel, ""},
					{"v_only", catalog.SystemViewRel, oldDef},
				})
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "table 'base' not exists")
	})

	t.Run("definitions that keep changing fail closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsWithBase
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads%2 == 0 {
				bh.sql2result[relationsSQL] = relationsWithOther
			} else {
				bh.sql2result[relationsSQL] = relationsWithBase
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_only"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "definition changed during validation")
	})
}

// TestResolvePublicationDatabaseByNameAuthorizesForeignSource proves a
// database owned by another account is resolved as a publication source only
// for the sys account: an ordinary administrator must not be able to publish
// one of the publication's target accounts into exposing its data.
func TestResolvePublicationDatabaseByNameAuthorizesForeignSource(t *testing.T) {
	dbIDSQL := func(accountID uint64) string {
		sql, err := getSqlForGetDbIdAndType(context.Background(), "fs9db", true, accountID)
		require.NoError(t, err)
		return sql
	}
	dbRows := func(dbID uint64) ExecResult {
		return newMrsForPublicationColumns(
			[]string{"dat_id", "dat_type"}, [][]interface{}{{dbID, ""}})
	}

	t.Run("the publisher's own database resolves directly", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[dbIDSQL(1)] = dbRows(9)
		ctx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

		databaseCtx, dbId, _, err := resolvePublicationDatabaseByName(ctx, bh, "fs9db", []uint32{2})
		require.NoError(t, err)
		require.Equal(t, uint64(9), dbId)
		accountID, err := defines.GetAccountId(databaseCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(1), accountID)
	})

	t.Run("sys may source a database owned by a target account", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[dbIDSQL(2)] = dbRows(7)
		ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)

		databaseCtx, dbId, _, err := resolvePublicationDatabaseByName(ctx, bh, "fs9db", []uint32{2})
		require.NoError(t, err)
		require.Equal(t, uint64(7), dbId)
		accountID, err := defines.GetAccountId(databaseCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(2), accountID)
	})

	t.Run("an owner-only foreign source of a general publisher resolves", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		// The database exists only in the account that owns it, and that
		// owner is the publication's sole target: the publication grants no
		// account visibility the owner lacks.
		bh.sql2result[dbIDSQL(2)] = dbRows(7)
		ctx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

		databaseCtx, dbId, _, err := resolvePublicationDatabaseByName(ctx, bh, "fs9db", []uint32{2})
		require.NoError(t, err)
		require.Equal(t, uint64(7), dbId)
		accountID, err := defines.GetAccountId(databaseCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(2), accountID)
	})

	t.Run("a general publisher cannot expose another account's database to a third account", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[dbIDSQL(2)] = dbRows(7)
		ctx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

		_, _, _, err := resolvePublicationDatabaseByName(ctx, bh, "fs9db", []uint32{2, 4})
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
	})

	t.Run("a database that exists nowhere still reports it missing", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		// Both the publisher and the target account answer with an empty
		// catalog: the resolution keeps its stable missing-database error.
		bh.sql2result[dbIDSQL(1)] = newMrsForPublicationColumns(
			[]string{"dat_id", "dat_type"}, nil)
		bh.sql2result[dbIDSQL(2)] = newMrsForPublicationColumns(
			[]string{"dat_id", "dat_type"}, nil)
		ctx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

		_, _, _, err := resolvePublicationDatabaseByName(ctx, bh, "fs9db", []uint32{2})
		require.Error(t, err)
		require.ErrorContains(t, err, "database 'fs9db' does not exist")
	})
}

// TestCheckSubscriptionValidCommonRejectsUnauthorizedForeignSource proves the
// statement paths fail closed with the stable authorization error for a
// foreign-source row whose publisher is neither the sys account nor within an
// owner-only target set: keeping the publisher identity would silently
// resolve a same-named publisher database (a decoy) instead of the persisted
// source, so the error is returned before any resolution runs.
func TestCheckSubscriptionValidCommonRejectsUnauthorizedForeignSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tenant := &TenantInfo{
		Tenant:        "acc1",
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      1,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses := newSes(nil, ctrl)
	ses.tenant = tenant
	ctx := defines.AttachAccount(context.Background(), uint32(1), rootID, moAdminRoleID)

	publisher := &pubsub.PubInfo{
		PubAccountId:   2,
		PubAccountName: "acc2",
		PubName:        "pub1",
		DbName:         testPublicationScopeDatabase,
		DbId:           7,
		TablesStr:      "base,v_only",
		SubAccountsStr: "acc1",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	newUnauthorizedBh := func(t *testing.T) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc2")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(2), "open"}})
		// the publication's sole target is the subscriber, a third account
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc1")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(1), "open"}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(3), ""}})
		// The publisher owns a same-named decoy database; a substituted
		// resolution would read these objects.
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 2)] =
			newMrsForPublicationRelations([][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_only AS SELECT 1 AS one")},
			})
		return bh
	}

	t.Run("the statement path rejects instead of resolving a publisher decoy", func(t *testing.T) {
		bh := newUnauthorizedBh(t)

		_, err := checkSubscriptionValidCommon(ctx, ses, "s_hij", "acc2", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationRelationsSQL(testPublicationScopeDatabase, 2),
			"the decoy catalog of the publisher is never consulted")
	})

	t.Run("a whole-database row is rejected before admission's early return", func(t *testing.T) {
		publisher.TablesStr = "*"
		defer func() { publisher.TablesStr = "base,v_only" }()
		bh := newUnauthorizedBh(t)

		_, err := checkSubscriptionValidForCreate(ctx, ses, "s_wide", "acc2", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationRelationsSQL(testPublicationScopeDatabase, 2),
			"the decoy catalog of the publisher is never consulted")
	})

	t.Run("an explicit-list row is rejected at admission", func(t *testing.T) {
		bh := newUnauthorizedBh(t)

		_, err := checkSubscriptionValidForCreate(ctx, ses, "s_hij", "acc2", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationRelationsSQL(testPublicationScopeDatabase, 2),
			"the decoy catalog of the publisher is never consulted")
	})
}

// TestCheckSubscriptionValidForCreateRejectsUnauthorizedForeignSource proves
// subscription admission rejects the foreign-source rows a general publisher
// could have created before the CREATE-time authorization existed, while the
// sys-published foreign source stays admitted.
func TestCheckSubscriptionValidForCreateRejectsUnauthorizedForeignSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tenant := &TenantInfo{
		Tenant:        "acc1",
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      1,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses := newSes(nil, ctrl)
	ses.tenant = tenant
	subscriberCtx := defines.AttachAccount(context.Background(), 1, rootID, moAdminRoleID)

	publisher := &pubsub.PubInfo{
		PubAccountId:   2,
		PubAccountName: "acc2",
		PubName:        "pub1",
		DbName:         testPublicationScopeDatabase,
		DbId:           7,
		TablesStr:      "base,v_only",
		SubAccountsStr: "acc1",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	newAdmissionBh := func(t *testing.T, owner uint64) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc2")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(2), "open"}})
		// the publication's sole target is the subscriber, a third account
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc1")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(1), "open"}})
		// an owner account for the owner-only control
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc3")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(3), "open"}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), testPublicationScopeDatabase)] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{owner, ""}})
		return bh
	}

	t.Run("a general publisher's foreign source is rejected at admission", func(t *testing.T) {
		bh := newAdmissionBh(t, 3)

		_, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_hij", "acc2", "pub1", bh)
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
	})

	t.Run("an owner-only target set of a general publisher stays admitted", func(t *testing.T) {
		publisher.SubAccountsStr = "acc3"
		defer func() { publisher.SubAccountsStr = "acc1" }()
		// The owner is the only account the publication targets, so it is
		// also the only subscriber that passes membership; its subscription
		// must resolve through the physical source it already owns.
		ownerCtrl := gomock.NewController(t)
		defer ownerCtrl.Finish()
		ownerTenant := &TenantInfo{
			Tenant:        "acc3",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      3,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ownerSes := newSes(nil, ownerCtrl)
		ownerSes.tenant = ownerTenant
		ownerCtx := defines.AttachAccount(context.Background(), 3, rootID, moAdminRoleID)
		bh := newAdmissionBh(t, 3)
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_only AS SELECT id FROM base",
			[]plan.ViewDependency{
				publicationDependency(testPublicationScopeDatabase, "base", func(dependency *plan.ViewDependency) {
					dependency.AccountID = 3
				}),
			})
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 3)] =
			newMrsForPublicationRelations([][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewDef},
			})

		subMeta, err := checkSubscriptionValidForCreate(ownerCtx, ownerSes, "s_own", "acc2", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, int32(3), subMeta.AccountId,
			"the owner-only target set grants no account visibility the owner lacks, so the physical source resolves")
	})

	t.Run("a sys publisher's foreign source stays admitted", func(t *testing.T) {
		publisher.PubAccountId = 0
		publisher.PubAccountName = "sys"
		defer func() {
			publisher.PubAccountId = 2
			publisher.PubAccountName = "acc2"
		}()
		bh := newAdmissionBh(t, 3)
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "sys")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(0), "open"}})
		viewDef := publicationViewDefWithDependencies(t,
			"CREATE VIEW v_only AS SELECT id FROM base",
			[]plan.ViewDependency{
				publicationDependency(testPublicationScopeDatabase, "base", func(dependency *plan.ViewDependency) {
					dependency.AccountID = 3
				}),
			})
		bh.sql2result[publicationRelationsSQL(testPublicationScopeDatabase, 3)] =
			newMrsForPublicationRelations([][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_only", catalog.SystemViewRel, viewDef},
			})

		subMeta, err := checkSubscriptionValidForCreate(subscriberCtx, ses, "s_fsrc", "sys", "pub1", bh)
		require.NoError(t, err)
		require.Equal(t, int32(3), subMeta.AccountId)
	})
}

// TestDoAlterPublicationForeignSourceAuthorization proves an ALTER that keeps
// the inherited source database enforces the same foreign-source
// authorization CREATE does, for general publishers, while the sys publisher
// keeps validating the inherited scope in the physical source account.
func TestDoAlterPublicationForeignSourceAuthorization(t *testing.T) {
	lockStub := gostub.StubFunc(&lockPublicationDatabase, "", nil)
	defer lockStub.Reset()
	invalidateStub := gostub.StubFunc(&invalidatePublicationViewMetadata, nil)
	defer invalidateStub.Reset()

	viewOnlyDef := publicationViewDefInDatabase(t, "db1",
		"CREATE VIEW v_only AS SELECT id, v FROM base WHERE v >= 10",
		[]plan.ViewDependency{
			publicationDependency("db1", "base", func(dependency *plan.ViewDependency) {
				dependency.AccountID = 2
			}),
		})
	publisher := &pubsub.PubInfo{
		PubAccountId:   1,
		PubAccountName: "acc1",
		PubName:        "pub1",
		DbName:         "db1",
		DbId:           7,
		TablesStr:      "v_only",
		SubAccountsStr: "",
	}
	pubStub := gostub.StubFunc(&getPubInfo, publisher, nil)
	defer pubStub.Reset()

	newForeignAlterSession := func(t *testing.T, tenantID uint32, owner uint64) (*Session, *backgroundExecTest, context.Context) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		tenant := &TenantInfo{
			Tenant:        "acc1",
			User:          rootName,
			DefaultRole:   accountAdminRoleName,
			TenantID:      tenantID,
			UserID:        rootID,
			DefaultRoleID: accountAdminRoleID,
		}
		ses := newSes(nil, ctrl)
		ses.tenant = tenant
		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		t.Cleanup(bhStub.Reset)
		bh.sql2result[getAccountIdNamesSql] = newMrsForPublicationColumns(
			[]string{"account_id", "account_name", "status", "version", "suspended_time"},
			[][]interface{}{
				{int64(0), "sys", "open", uint64(1), nil},
				{int64(1), "acc1", "open", uint64(1), nil},
				{int64(2), "acc2", "open", uint64(1), nil},
				{int64(3), "acc3", "open", uint64(1), nil},
			})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), "db1")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{owner, ""}})
		bh.sql2result[publicationRelationsSQL("db1", uint32(owner))] = newMrsForPublicationRelations([][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_only", catalog.SystemViewRel, viewOnlyDef},
		})
		ctx := defines.AttachAccount(context.Background(), tenantID, rootID, moAdminRoleID)
		return ses, bh, ctx
	}
	t.Run("alter by a general publisher that exposes a foreign source beyond the owner is rejected", func(t *testing.T) {
		ses, bh, ctx := newForeignAlterSession(t, 1, 2)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account add acc3", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"a foreign source may be published by the sys account, or to the owner account alone")
		require.NotContains(t, joinStrings(bh.executedSQLs), "update mo_catalog.mo_pubs")
	})

	t.Run("alter by a general publisher to the owner alone proceeds into scope validation", func(t *testing.T) {
		// The owner-only target set grants no account visibility the owner
		// lacks, so the inherited source stays legal and the ALTER reaches
		// the scope validation every ALTER must pass.
		ses, _, ctx := newForeignAlterSession(t, 1, 2)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account add acc2", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'db1' must include table 'base' required by view 'v_only'")
	})

	t.Run("alter by sys of a foreign-owned source database proceeds into scope validation", func(t *testing.T) {
		ses, _, ctx := newForeignAlterSession(t, sysAccountID, 2)
		stmts, err := mysql.Parse(ctx, "alter publication pub1 account add acc2", 1)
		require.NoError(t, err)
		defer freeStatements(stmts)

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'db1' must include table 'base' required by view 'v_only'")
	})
}

// TestResolveSubscriptionSourceAccounts proves the metadata enumeration path
// agrees with the direct statement paths on the account that physically owns
// the published database: ordinary publications keep their owner, sys
// foreign sources resolve to the physical owner, unauthorized rows keep the
// publisher and fail closed, and catalog failures propagate.
func TestResolveSubscriptionSourceAccounts(t *testing.T) {
	pubInfos := map[string]*pubsub.PubInfo{
		"p_own": {
			PubAccountId: 1, PubAccountName: "acc1", PubName: "p_own",
			DbName: "db1", DbId: 5, TablesStr: "*", SubAccountsStr: "acc2",
		},
		"p_sys": {
			PubAccountId: 0, PubAccountName: "sys", PubName: "p_sys",
			DbName: "fsrcdb", DbId: 7, TablesStr: "base,v_x", SubAccountsStr: "acc2",
		},
		"p_gen": {
			PubAccountId: 1, PubAccountName: "acc1", PubName: "p_gen",
			DbName: "fsrcdb2", DbId: 9, TablesStr: "base", SubAccountsStr: "acc2",
		},
		"p_gen_own": {
			PubAccountId: 1, PubAccountName: "acc1", PubName: "p_gen_own",
			DbName: "fsrcdb3", DbId: 11, TablesStr: "base", SubAccountsStr: "acc3",
		},
		"p_all": {
			PubAccountId: 0, PubAccountName: "sys", PubName: "p_all",
			DbName: "*", DbId: 0, TablesStr: "*", SubAccountsStr: "acc2",
		},
	}
	getPubInfoCalls := 0
	pubStub := gostub.Stub(&getPubInfo, func(ctx context.Context, bh BackgroundExec, pubName string) (*pubsub.PubInfo, error) {
		getPubInfoCalls++
		return pubInfos[pubName], nil
	})
	defer pubStub.Reset()

	newMetaBh := func(t *testing.T) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(5), "db1")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(1), ""}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), "fsrcdb")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(2), ""}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(9), "fsrcdb2")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(3), ""}})
		bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(11), "fsrcdb3")] =
			newMrsForPublicationColumns(
				[]string{"account_id", "dat_type"}, [][]interface{}{{uint64(3), ""}})
		// target-name identities for the general-publisher foreign checks
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc2")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(2), "open"}})
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, "acc3")] = newMrsForPublicationColumns(
			[]string{"account_id", "status"}, [][]interface{}{{int64(3), "open"}})
		return bh
	}
	newMeta := func(subName, pubName string, publisherID int32) *plan.SubscriptionMeta {
		return &plan.SubscriptionMeta{
			Name:        pubName,
			AccountId:   publisherID,
			DbName:      pubInfos[pubName].DbName,
			AccountName: pubInfos[pubName].PubAccountName,
			SubName:     subName,
			Tables:      pubInfos[pubName].TablesStr,
		}
	}

	t.Run("an ordinary publication keeps resolving through its owner", func(t *testing.T) {
		bh := newMetaBh(t)
		metas := []*plan.SubscriptionMeta{newMeta("s_own", "p_own", 1)}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, int32(1), resolved[0].AccountId)
	})

	t.Run("a sys foreign source resolves to the physical owner", func(t *testing.T) {
		bh := newMetaBh(t)
		metas := []*plan.SubscriptionMeta{newMeta("s_fsrc", "p_sys", 0)}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, int32(2), resolved[0].AccountId)
	})

	t.Run("subscriptions of one publication share one resolution", func(t *testing.T) {
		bh := newMetaBh(t)
		callsBefore := getPubInfoCalls
		metas := []*plan.SubscriptionMeta{
			newMeta("s_fsrc1", "p_sys", 0),
			newMeta("s_fsrc2", "p_sys", 0),
		}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 2)
		require.Equal(t, int32(2), resolved[0].AccountId)
		require.Equal(t, int32(2), resolved[1].AccountId)
		require.Equal(t, callsBefore+1, getPubInfoCalls)
	})

	t.Run("an unauthorized foreign source is dropped instead of enumerating the publisher", func(t *testing.T) {
		bh := newMetaBh(t)
		sysMeta := newMeta("s_fsrc", "p_sys", 0)
		metas := []*plan.SubscriptionMeta{sysMeta, newMeta("s_hij", "p_gen", 1)}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1, "only the authorized sys foreign source remains enumerated")
		require.Equal(t, int32(2), resolved[0].AccountId)
		require.Equal(t, "s_fsrc", resolved[0].SubName)
	})

	t.Run("an owner-only foreign source of a general publisher resolves to the owner", func(t *testing.T) {
		bh := newMetaBh(t)
		metas := []*plan.SubscriptionMeta{newMeta("s_own", "p_gen_own", 1)}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, int32(3), resolved[0].AccountId,
			"owner-only targets grant no account visibility the owner lacks, so the physical source resolves")
	})

	t.Run("an account-level publication skips identity resolution", func(t *testing.T) {
		bh := newMetaBh(t)
		metas := []*plan.SubscriptionMeta{newMeta("s_all", "p_all", 0)}
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, int32(0), resolved[0].AccountId)
		require.NotContains(t, joinStrings(bh.executedSQLs), fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(0), "*"))
	})

	t.Run("a dropped publication row keeps the publisher identity", func(t *testing.T) {
		bh := newMetaBh(t)
		metas := []*plan.SubscriptionMeta{newMeta("s_gone", "p_gen", 1)}
		genPub := pubInfos["p_gen"]
		delete(pubInfos, "p_gen")
		defer func() { pubInfos["p_gen"] = genPub }()
		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, int32(1), resolved[0].AccountId)
	})

	t.Run("an identity read failure propagates", func(t *testing.T) {
		bh := newMetaBh(t)
		wantErr := errors.New("catalog unavailable")
		bh.sql2err[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(7), "fsrcdb")] = wantErr
		metas := []*plan.SubscriptionMeta{newMeta("s_fsrc", "p_sys", 0)}

		resolved, err := resolveSubscriptionSourceAccounts(context.Background(), bh, metas)
		require.Nil(t, resolved)
		require.ErrorIs(t, err, wantErr)
	})
}

// TestValidatePublicationTableScopeRejectsSequenceViews proves a published
// view that calls a sequence function is rejected before any catalog write.
// The binder resolves the sequence argument at execution time in the
// executing account (baseBinder defers it past binding), and sequence objects
// are never routed through a subscription, so such a view would subscribe
// successfully and fail on every query. Detection is a pure syntax walk - the
// validator never executes the function, so it can never consume a sequence
// value - and it covers nested views through the same closure walk.
func TestValidatePublicationTableScopeRejectsSequenceViews(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	newScopeBh := func(t *testing.T, relations [][]interface{}) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = newMrsForPublicationRelations(relations)
		return bh
	}

	t.Run("a nextval view is rejected", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_seq", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_seq AS SELECT nextval('sq') AS n", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_seq"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_seq': it calls sequence function 'nextval', and sequence objects do not resolve through subscriptions")
	})

	t.Run("setval and currval are rejected like nextval", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_set", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_set AS SELECT setval('sq', 1)", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_set"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls sequence function 'setval'")

		bh = newScopeBh(t, [][]interface{}{
			{"v_cur", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_cur AS SELECT currval('sq')", nil)},
		})
		err = validatePublicationTableScope(ctx, bh, db, []string{"v_cur"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls sequence function 'currval'")
	})

	t.Run("a nested view over a sequence view is rejected", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_seq", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_seq AS SELECT nextval('sq') AS n", nil)},
			{"v_outer", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_outer AS SELECT n FROM v_seq",
				[]plan.ViewDependency{
					publicationDependency(db, "v_seq", func(dependency *plan.ViewDependency) {
						dependency.RelationKind = catalog.SystemViewRel
					}),
				})},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_outer", "v_seq"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls sequence function 'nextval'")
	})

	t.Run("a sequence call inside a CTE body is rejected", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_cte", catalog.SystemViewRel, publicationLegacyViewDef(t,
				"CREATE VIEW v_cte AS WITH s AS (SELECT nextval('sq') AS n) SELECT n FROM s")},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_cte"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls sequence function 'nextval'")
	})

	t.Run("the rejection happens before the binding probe executes anything", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_seq", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_seq AS SELECT nextval('sq') AS n", nil)},
			{"v_ok", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ok AS SELECT 1 AS one", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_ok", "v_seq"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls sequence function 'nextval'")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_seq"),
			"a sequence function is never executed for validation")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ok"))
	})

	t.Run("a genuine constant view stays accepted", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_const", catalog.SystemViewRel, publicationLegacyViewDef(t,
				"CREATE VIEW v_const AS SELECT 1 AS one")},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_const"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
	})
}

// TestValidatePublicationTableScopeRejectsTableFunctionViews proves the
// round-14 rejection of relation-resolving table functions: buildTableChanges
// resolves (database, table) string literals through the executing account's
// compiler context, so a subscriber re-binds the call with its subscription
// routing and table_changes' own binder rejects subscription sources - after
// publication and subscription creation both succeeded. The walk sees the
// FROM-position call of the persisted statement on both dependency formats
// and rejects before any binding probe runs; literal-driven table functions
// (json_table, generate_series) resolve nothing through the executing
// account and stay publishable.
func TestValidatePublicationTableScopeRejectsTableFunctionViews(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	newScopeBh := func(t *testing.T, relations [][]interface{}) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = newMrsForPublicationRelations(relations)
		return bh
	}

	t.Run("a table_changes view with complete membership is rejected", func(t *testing.T) {
		t.Run("with today's captured-dependency format", func(t *testing.T) {
			bh := newScopeBh(t, [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_tc", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_tc AS SELECT * FROM table_changes('view_pub_db','base','','1-0') c",
					[]plan.ViewDependency{
						publicationDependency(db, "base", nil),
					})},
			})
			err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_tc"}, generalSubscriberAccounts, int64(1))
			require.Error(t, err)
			require.ErrorContains(t, err,
				"table-level publication for database 'view_pub_db' cannot publish view 'v_tc': it reads table function 'table_changes', which resolves its source through the executing account and does not operate through subscriptions")
			require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_tc"),
				"the table function is rejected before any binding probe")
			require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "base"))
		})

		t.Run("with the legacy persisted-format statement", func(t *testing.T) {
			bh := newScopeBh(t, [][]interface{}{
				{"v_tc", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_tc AS SELECT * FROM table_changes('view_pub_db','base','','1-0') c")},
			})
			err := validatePublicationTableScope(ctx, bh, db, []string{"v_tc"}, generalSubscriberAccounts, int64(1))
			require.Error(t, err)
			require.ErrorContains(t, err, "it reads table function 'table_changes'")
		})
	})

	t.Run("a metadata_scan view with complete membership is rejected", func(t *testing.T) {
		// The runtime scanner's planner exposes a static schema and captures
		// no source dependency, so a complete membership list - even with the
		// scanned base published - cannot make the subscriber's copy
		// executable: the executor opens the publisher's database name inside
		// the executing account at query time.
		bh := newScopeBh(t, [][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_ms", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ms AS SELECT * FROM metadata_scan('view_pub_db.base','id') m", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_ms"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_ms': it reads table function 'metadata_scan', which resolves its source through the executing account and does not operate through subscriptions")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ms"),
			"the runtime scanner is rejected before any binding probe")
	})

	t.Run("a table_stats view with complete membership is rejected", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_ts", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ts AS SELECT * FROM table_stats('view_pub_db.base','get') t", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_ts"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_ts': it reads table function 'table_stats', which resolves its source through the executing account and does not operate through subscriptions")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ts"),
			"the runtime scanner is rejected before any binding probe")
	})

	t.Run("the rejection precedes every probe of the scope", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_ok", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ok AS SELECT 1 AS one", nil)},
			{"v_tc", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_tc AS SELECT * FROM table_changes('view_pub_db','base','','1-0') c",
				nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_ok", "v_tc"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it reads table function 'table_changes'")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ok"),
			"no view of the rejected scope is probed")
	})

	t.Run("literal-driven table functions stay publishable", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_gs", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_gs AS SELECT * FROM generate_series(1, 3) g", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_gs"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
	})
}

// TestValidatePublicationTableScopeRejectsRelationResolvingScalarFunctions
// proves the shared source-resolution policy covers the scalar builtins whose
// relation argument is frozen as string literals and resolved only at
// execution through the executing account (mo_table_rows, mo_table_size,
// mo_table_col_max, mo_table_col_min, internal_auto_increment, mo_ctl): the
// walk collects no relation reference for them, so complete membership proves
// nothing, and the publication and its subscription would admit a view whose
// every subscriber query either fails the executing-account database lookup
// or silently reads the subscriber's own same-named catalog - for mo_ctl the
// gating role is sys moadmin itself, and that exact identity may subscribe to
// an ordinary account's publication, so the frozen literal executes for it
// and reads the sys account's own same-named relation or fails its lookup.
// The rejection must fire before any binding probe and catalog write, in
// every expression position, for both persisted formats, while ordinary
// scalar functions stay publishable.
func TestValidatePublicationTableScopeRejectsRelationResolvingScalarFunctions(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	newScopeBh := func(t *testing.T, relations [][]interface{}) *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = newMrsForPublicationRelations(relations)
		return bh
	}

	for _, fn := range []struct {
		name     string
		viewStmt string
	}{
		{"mo_table_col_max", "SELECT mo_table_col_max('view_pub_db','base','id') AS m"},
		{"mo_table_col_min", "SELECT mo_table_col_min('view_pub_db','base','id') AS m"},
		{"mo_table_rows", "SELECT mo_table_rows('view_pub_db','base') AS r"},
		{"mo_table_size", "SELECT mo_table_size('view_pub_db','base') AS s"},
		{"internal_auto_increment", "SELECT internal_auto_increment('view_pub_db','base') AS n"},
		{"mo_ctl", "SELECT mo_ctl('cn','table-extra','view_pub_db.base') AS r"},
	} {
		t.Run(fn.name+" is rejected with complete membership", func(t *testing.T) {
			bh := newScopeBh(t, [][]interface{}{
				{"base", catalog.SystemOrdinaryRel, ""},
				{"v_ssm", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
					"CREATE VIEW v_ssm AS "+fn.viewStmt, nil)},
			})
			err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_ssm"}, generalSubscriberAccounts, int64(1))
			require.Error(t, err)
			require.ErrorContains(t, err,
				"table-level publication for database 'view_pub_db' cannot publish view 'v_ssm': it calls function '"+fn.name+
					"', which resolves its source relation through the executing account and does not operate through subscriptions")
			require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ssm"),
				"the scalar call is rejected before any binding probe")
		})
	}

	t.Run("the rejection covers the legacy persisted format and any position", func(t *testing.T) {
		for _, fn := range []struct {
			name     string
			viewStmt string
		}{
			{"mo_table_rows", "abs(-1) + mo_table_rows('view_pub_db','base')"},
			{"internal_auto_increment", "abs(-1) + internal_auto_increment('view_pub_db','base')"},
			{"mo_ctl", "abs(-1) + mo_ctl('cn','table-extra','view_pub_db.base')"},
		} {
			bh := newScopeBh(t, [][]interface{}{
				{"v_ssm", catalog.SystemViewRel, publicationLegacyViewDef(t,
					"CREATE VIEW v_ssm AS SELECT "+fn.viewStmt+" AS a")},
			})
			err := validatePublicationTableScope(ctx, bh, db, []string{"v_ssm"}, generalSubscriberAccounts, int64(1))
			require.Error(t, err)
			require.ErrorContains(t, err, "it calls function '"+fn.name+"'")
			require.ErrorContains(t, err, "cannot publish view 'v_ssm'")
		}
	})

	t.Run("the rejection precedes every probe of the scope", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"base", catalog.SystemOrdinaryRel, ""},
			{"v_ok", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ok AS SELECT 1 AS one", nil)},
			{"v_ssm", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_ssm AS SELECT mo_table_size('view_pub_db','base') AS s", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"base", "v_ok", "v_ssm"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "it calls function 'mo_table_size'")
		require.NotContains(t, joinStrings(bh.executedSQLs), publicationViewBindingSQL(db, "v_ok"),
			"no view of the rejected scope is probed")
	})

	t.Run("ordinary scalar functions stay publishable", func(t *testing.T) {
		bh := newScopeBh(t, [][]interface{}{
			{"v_plain", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_plain AS SELECT abs(-1) AS a, length('abc') AS l", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_plain"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
	})
}

// TestValidatePublicationTableScopeBaseSchemaStability proves the coherence
// loop sees base-table schema changes: ALTER TABLE replaces the catalog row
// with a newly allocated rel_id while relkind and viewdef stay unchanged, and
// the review's two-view interleaving - v1 bound against t1's old columns, v2
// bound against t2's new columns, neither schema valid for both - is caught
// by the identity comparison, revalidated, and rejected when the second
// binding fails.
func TestValidatePublicationTableScopeBaseSchemaStability(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	v1Def := publicationViewDefWithDependencies(t,
		"CREATE VIEW v1 AS SELECT a FROM t1 WHERE a >= 1",
		[]plan.ViewDependency{publicationDependency(db, "t1", nil)})
	v2Def := publicationViewDefWithDependencies(t,
		"CREATE VIEW v2 AS SELECT b FROM t2 WHERE b >= 1",
		[]plan.ViewDependency{publicationDependency(db, "t2", nil)})
	relationsAtV1 := newMrsForPublicationRelations([][]interface{}{
		{"t1", catalog.SystemOrdinaryRel, "", uint64(101)},
		{"t2", catalog.SystemOrdinaryRel, "", uint64(102)},
		{"v1", catalog.SystemViewRel, v1Def, uint64(111)},
		{"v2", catalog.SystemViewRel, v2Def, uint64(112)},
	})
	// After the concurrent DDL (drop t1.a, add t2.b) both base rows are
	// replaced: new rel_id, same relkind, same empty viewdef.
	relationsAfterDDL := newMrsForPublicationRelations([][]interface{}{
		{"t1", catalog.SystemOrdinaryRel, "", uint64(201)},
		{"t2", catalog.SystemOrdinaryRel, "", uint64(202)},
		{"v1", catalog.SystemViewRel, v1Def, uint64(111)},
		{"v2", catalog.SystemViewRel, v2Def, uint64(112)},
	})

	t.Run("a stable schema set validates once", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsAtV1
		err := validatePublicationTableScope(ctx, bh, db, []string{"t1", "t2", "v1", "v2"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
	})

	t.Run("the two-view interleaving cannot admit incompatible schemas", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsAtV1
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads == 2 {
				// The DDL lands between the two binding probes: v1 already
				// bound against t1's old columns; v2 binds against t2's new
				// columns; both catalog rows carry new rel_ids.
				bh.sql2result[relationsSQL] = relationsAfterDDL
				// On the revalidation, v1's probe fails against t1 without
				// column a.
				bh.sql2err[publicationViewBindingSQL(db, "v1")] =
					errors.New("invalid input: column a does not exist")
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"t1", "t2", "v1", "v2"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v1': its stored definition no longer binds the published relations: invalid input: column a does not exist")
	})

	t.Run("a compatible schema change revalidates and admits", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = relationsAtV1
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads == 2 {
				// A concurrent but compatible change (an added table column)
				// replaces t1's row: the retry re-probes both views against
				// the final schema and admits.
				bh.sql2result[relationsSQL] = relationsAfterDDL
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"t1", "t2", "v1", "v2"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
	})
}

// countPublicationExecutedSQLs counts how often one statement reached the
// background executor.
func countPublicationExecutedSQLs(bh *backgroundExecTest, sql string) int {
	count := 0
	for _, executed := range bh.executedSQLs {
		if executed == sql {
			count++
		}
	}
	return count
}

// TestPublicationViewBindingProbeShape locks the two structural contracts of
// the probe's bind-only dispatch. The marker rides the compiler context into
// every statement the planner nests while binding the probe - the mo_columns
// identity read of the LIMIT 0 shortcut first - and those nested catalog
// readers must still compile and run, so only the probe's literal LIMIT 0
// shape takes the bind-only route. And the dispatch's publication-database
// extraction must find the schema through the parser's implicit join and
// alias wrappers, so the binder resolves the stored definition's unqualified
// function calls against the same database the view was created in.
func TestPublicationViewBindingProbeShape(t *testing.T) {
	parse := func(t *testing.T, sql string, lowerCaseTableNames int64) []tree.Statement {
		t.Helper()
		statements, err := mysql.Parse(context.Background(), sql, lowerCaseTableNames)
		require.NoError(t, err)
		t.Cleanup(func() {
			for _, statement := range statements {
				statement.Free()
			}
		})
		require.Len(t, statements, 1)
		return statements
	}

	probeSQL := publicationViewBindingSQL(testPublicationScopeDatabase, "v_only")
	for _, lowerCaseTableNames := range []int64{0, 1} {
		statements := parse(t, probeSQL, lowerCaseTableNames)
		require.True(t, isPublicationViewBindingProbe(statements[0]),
			"the probe statement is dispatched bind-only (parse mode %d)", lowerCaseTableNames)
		require.Equal(t, testPublicationScopeDatabase,
			publicationProbeDatabase(statements[0].(*tree.Select)),
			"the probe's publication database is extracted through the join and alias wrappers (parse mode %d)",
			lowerCaseTableNames)
	}

	nestedReaders := []string{
		publicationRelationsSQL(testPublicationScopeDatabase, sysAccountID),
		publicationSystemRelationSQL("system", "statement_info"),
		// The planner's mo_columns identity read of the LIMIT 0 shortcut.
		`select col.attname, col.atttyp from mo_catalog.mo_columns col join mo_catalog.mo_tables tbl on col.account_id = tbl.account_id and col.att_relname_id = tbl.rel_id where col.account_id = 1 and col.att_database = 'view_pub_db' and col.att_relname = 'v_only' order by col.attnum;`,
		func() string {
			sql, err := getTableColumnDefSql(1, testPublicationScopeDatabase, "v_only")
			require.NoError(t, err)
			return sql
		}(),
		// The resolveUdfInDatabase catalog lookup.
		`select args, body, language, rettype, db, modified_time, sql_mode from mo_catalog.mo_user_defined_function where name = "f" and db = "view_pub_db";`,
	}
	for i, reader := range nestedReaders {
		statements := parse(t, reader, 0)
		require.False(t, isPublicationViewBindingProbe(statements[0]),
			"nested catalog reader %d must keep its full compile and run", i)
	}

	for _, negative := range []string{
		"select * from `view_pub_db`.`v` limit 5;",
		"select * from `view_pub_db`.`v`;",
		"select * from (select 1 as id limit 0) as d;",
	} {
		statements := parse(t, negative, 1)
		require.False(t, isPublicationViewBindingProbe(statements[0]),
			"only a top-level literal LIMIT 0 is the probe: %q", negative)
	}
}

// wrapperParseSession answers only what parseSql and the wrapper construction
// touch on the session: the session-pinned parser case mode and the parser
// instance that parses the executed statements.
type wrapperParseSession struct {
	FeSession
	lowerCaseTableNames int64
}

func (s *wrapperParseSession) GetSessionSysVar(name string) (interface{}, error) {
	return s.lowerCaseTableNames, nil
}

func (s *wrapperParseSession) GetMySQLParser() *mysql.MySQLParser {
	return &mysql.MySQLParser{}
}

// TestPublicationViewBindingProbeWrapperKeepsIdentifierSourceCase proves the
// round-13 case policy at the parse that produces the executed statement: the
// publication binding probe names the exact catalog object the validation
// snapshot certified, so under the marker GetComputationWrapperInBack reparses
// the probe SQL at identifier source case even when the validating session
// pins lower_case_table_names mode 1, and the wrapper AST (the statement the
// bind-only dispatch receives) names the mixed-case catalog object - while an
// ordinary unmarked statement still parses at the session mode and lowercases.
func TestPublicationViewBindingProbeWrapperKeepsIdentifierSourceCase(t *testing.T) {
	probeSQL := "select * from `ViwDb`.`ViwObj` limit 0"
	proc := testutil.NewProc(t)

	wrap := func(ctx context.Context) *tree.Select {
		session := &wrapperParseSession{lowerCaseTableNames: 1}
		input := &UserInput{sql: probeSQL}
		execCtx := &ExecCtx{reqCtx: ctx, ses: session, input: input}
		wrappers, err := GetComputationWrapperInBack(execCtx, "", input, "", nil, proc, session)
		require.NoError(t, err)
		require.Len(t, wrappers, 1)
		t.Cleanup(func() { wrappers[0].GetAst().Free() })
		require.True(t, isPublicationViewBindingProbe(wrappers[0].GetAst()),
			"the marked mixed-case statement is still the literal LIMIT 0 probe")
		return wrappers[0].GetAst().(*tree.Select)
	}
	firstTableName := func(sel *tree.Select) tree.TableName {
		clause, ok := sel.Select.(*tree.SelectClause)
		require.True(t, ok)
		require.NotEmpty(t, clause.From.Tables)
		for table := clause.From.Tables[0]; ; {
			switch typed := table.(type) {
			case *tree.TableName:
				return *typed
			case *tree.AliasedTableExpr:
				table = typed.Expr
			case *tree.JoinTableExpr:
				require.Nil(t, typed.Right, "the probe has no join")
				table = typed.Left
			default:
				t.Fatalf("unexpected FROM wrapper %T", table)
			}
		}
	}

	marked := firstTableName(wrap(
		defines.WithPublicationViewBinding(context.Background())))
	require.Equal(t, tree.Identifier("ViwDb"), marked.SchemaName,
		"a marked probe's executed statement keeps the schema name's source case")
	require.Equal(t, tree.Identifier("ViwObj"), marked.ObjectName,
		"a marked probe's executed statement keeps the table name's source case")

	unmarked := firstTableName(wrap(context.Background()))
	require.Equal(t, tree.Identifier("viwdb"), unmarked.SchemaName,
		"the ordinary statement of the session still parses at mode 1")
	require.Equal(t, tree.Identifier("viwobj"), unmarked.ObjectName,
		"the ordinary statement of the session still parses at mode 1")
}

// probeAccountSession records the request-account stamping the shared planning
// initializer performs on the back session before a build.
type probeAccountSession struct {
	FeSession
	tcc     *TxnCompilerContext
	account uint32
	stamped []uint32
}

func (s *probeAccountSession) GetTxnCompileCtx() *TxnCompilerContext { return s.tcc }

func (s *probeAccountSession) GetAccountId() uint32 { return s.account }

func (s *probeAccountSession) SetAccountId(id uint32) {
	s.account = id
	s.stamped = append(s.stamped, id)
}

// TestPublicationScopeProbeRestampsSourceAccountAfterSystemCatalogRead is the
// round-13 account-transition assertion: publicationSystemRelationAvailable
// compiles its system-catalog read on the same background executor under the
// attached system account, and that ordinary compile stamps the shared back
// session with account 0; the session's account no longer describes the
// publication's source account when the next probe binds, and the
// account-keyed mo_columns identity read would resolve the view in the wrong
// account - the p_g2g regression the review found baked into the expected
// result. The probe must run through the shared planning initializer
// buildPlanWithStats, exactly like every compiled statement on this session,
// so the request account (the publication's source account) is stamped on the
// session before the build starts and the initializer's planContext keeps the
// binding marker.
func TestPublicationScopeProbeRestampsSourceAccountAfterSystemCatalogRead(t *testing.T) {
	accountCtx, _ := installBackExecStatsInfo(context.Background(), time.Now(), 0)
	requestCtx := defines.WithPublicationViewBinding(
		defines.AttachAccount(accountCtx, sysAccountID+42, rootID, moAdminRoleID))
	proc := testutil.NewProc(t)
	tcc := InitTxnCompilerContext(testPublicationScopeDatabase)
	// The system-catalog read of a preceding system-relation availability
	// check left the shared session stamped with the system account.
	session := &probeAccountSession{tcc: tcc, account: catalog.System_Account}
	require.Equal(t, uint32(0), session.GetAccountId())
	tcc.SetExecCtx(&ExecCtx{reqCtx: requestCtx, ses: session, proc: proc})

	statements, err := mysql.Parse(requestCtx,
		publicationViewBindingSQL(testPublicationScopeDatabase, "v_only"), 0)
	require.NoError(t, err)
	t.Cleanup(func() { statements[0].Free() })
	execCtx := &ExecCtx{reqCtx: requestCtx, ses: session, stmt: statements[0], proc: proc}

	// The unit fixture has no engine: its binding ends in an error regardless
	// of the account transition under test, which the shared initializer has
	// already completed first, so the outcome is only recorded, not asserted.
	_ = func() (bindErr error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				bindErr = fmt.Errorf("%v", recovered)
			}
		}()
		return bindPublicationScopeSelect(execCtx)
	}()

	require.Equal(t, uint32(sysAccountID+42), session.account,
		"the probe stamped the publication's source account back on the session")
	require.Equal(t, []uint32{uint32(sysAccountID) + 42}, session.stamped,
		"the shared initializer stamped the source account exactly once")
	require.True(t, defines.IsPublicationViewBinding(tcc.GetContext()),
		"the initializer's planContext keeps the binding marker into the compiler context")
}

// binding probe is the one decider for user-defined functions: it runs the
// same persisted-view binding a subscriber's first query performs, under the
// defines.PublicationViewBinding marker, and the real binder decides which
// function every call selects - builtin binding by name, arity and argument
// types comes first, and only a call the binder itself sends to user-defined
// functions returns the veto error the validator maps to the stable
// subscription-resolution message. So a same-name user-defined catalog row can
// no longer reject a view that actually calls the builtin, validation carries
// no function-catalog read at all, no function of a validated view executes,
// and a sequence call stays a pre-probe walk rejection because sequence state
// itself, not function resolution, is unresolvable through subscriptions.
func TestValidatePublicationTableScopeRejectsUserDefinedFunctionViews(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	udfViewDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_udf AS SELECT udf27767() AS n", nil)

	newFunctionScopeBh := func(t *testing.T, relations [][]interface{}) *backgroundExecTest {
		t.Helper()
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = newMrsForPublicationRelations(relations)
		return bh
	}

	// vetoView simulates the binder's own verdict for one view's probe: the
	// real binder selected a user-defined function for a call and returned
	// the distinguishable veto instead of binding it.
	vetoView := func(bh *backgroundExecTest, view, function string) {
		bh.sql2err[publicationViewBindingSQL(db, view)] =
			&plan.UdfDisallowedInPublicationViewBinding{Name: function}
	}

	// noFunctionCatalogReads proves the walk carries no function-name
	// pre-check: the binder's choice inside the probe is the only decider,
	// so no validation statement reads the function catalog.
	noFunctionCatalogReads := func(t *testing.T, bh *backgroundExecTest) {
		t.Helper()
		for _, executed := range bh.executedSQLs {
			require.NotContains(t, executed, "mo_user_defined_function",
				"validation must not consult the function catalog")
		}
	}

	t.Run("a view whose binding selects a user-defined function is rejected with the stable message", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_udf", catalog.SystemViewRel, udfViewDef},
		})
		vetoView(bh, "v_udf", "udf27767")
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_udf"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_udf': it calls user-defined function 'udf27767', and user-defined functions do not resolve through subscriptions")
		require.Equal(t, 1, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_udf")),
			"the decision came from the view's own binding probe")
		noFunctionCatalogReads(t, bh)
	})

	t.Run("a same-name catalog row not being a pre-check keeps a builtin call publishable", func(t *testing.T) {
		// The stored definition calls abs(-7); the real binder selects the
		// builtin by name, arity and argument types regardless of any
		// same-name user-defined catalog row, so validation has no
		// function-name pre-check to false-reject with: the probe succeeds
		// and the view publishes. The live cross-account BVT companion
		// proves the same direction against a real same-name catalog row.
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_abs", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_abs AS SELECT abs(-7) AS y", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_abs"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.Equal(t, 1, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_abs")),
			"the view is still bound through its stored definition")
		noFunctionCatalogReads(t, bh)
	})

	t.Run("the binding marker rides every probe statement and only probe statements", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_a", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_a AS SELECT 1 AS one", nil)},
			{"v_b", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_b AS SELECT 2 AS two", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_a", "v_b"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.Equal(t, len(bh.executedSQLs), len(bh.publicationViewBinding),
			"every executed statement records the marker state")
		probeA := publicationViewBindingSQL(db, "v_a")
		probeB := publicationViewBindingSQL(db, "v_b")
		probes := 0
		for i, executed := range bh.executedSQLs {
			if executed == probeA || executed == probeB {
				probes++
				require.True(t, bh.publicationViewBinding[i],
					"probe statements must execute under the publication-view binding marker")
			} else {
				require.False(t, bh.publicationViewBinding[i],
					"catalog reads must keep their ordinary execution mode")
			}
		}
		require.Equal(t, 2, probes)
	})

	t.Run("a nested scope fails on the function view and stops the probe loop", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_udf", catalog.SystemViewRel, udfViewDef},
			{"v_outer", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_outer AS SELECT n FROM v_udf",
				[]plan.ViewDependency{
					publicationDependency(db, "v_udf", func(dependency *plan.ViewDependency) {
						dependency.RelationKind = catalog.SystemViewRel
					}),
				})},
		})
		vetoView(bh, "v_udf", "udf27767")
		vetoView(bh, "v_outer", "udf27767")
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_udf", "v_outer"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, "cannot publish view 'v_udf': it calls user-defined function 'udf27767'")
		require.Equal(t, 0, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_outer")),
			"validation stops at the first failing view")
	})

	t.Run("a probe failure that is not a veto keeps the binding message", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"t1", catalog.SystemOrdinaryRel, ""},
			{"v_bro", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_bro AS SELECT id FROM t1",
				[]plan.ViewDependency{publicationDependency(db, "t1", nil)})},
		})
		bindingErr := errors.New("invalid input: column id does not exist")
		bh.sql2err[publicationViewBindingSQL(db, "v_bro")] = bindingErr
		err := validatePublicationTableScope(ctx, bh, db, []string{"t1", "v_bro"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_bro': its stored definition no longer binds the published relations")
		require.ErrorContains(t, err, bindingErr.Error())
		require.NotContains(t, err.Error(), "user-defined function")
	})

	t.Run("a sequence call is rejected in the walk before any probe", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_mix", catalog.SystemViewRel, publicationViewDefWithDependencies(t,
				"CREATE VIEW v_mix AS SELECT nextval('sq') AS n, udf27767() AS m", nil)},
		})
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_mix"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"it calls sequence function 'nextval', and sequence objects do not resolve through subscriptions")
		require.Equal(t, 0, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_mix")),
			"the sequence rejection precedes every probe")
		noFunctionCatalogReads(t, bh)
	})

	t.Run("a probe backend failure fails closed with the cause visible", func(t *testing.T) {
		bh := newFunctionScopeBh(t, [][]interface{}{
			{"v_udf", catalog.SystemViewRel, udfViewDef},
		})
		backendErr := errors.New("binding backend unavailable")
		bh.sql2err[publicationViewBindingSQL(db, "v_udf")] = backendErr
		err := validatePublicationTableScope(ctx, bh, db, []string{"v_udf"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err, backendErr.Error(),
			"the probe's own failure surfaces with the cause text")
		noFunctionCatalogReads(t, bh)
	})
}

// TestValidatePublicationTableScopeInPlaceRenameStability proves the
// coherence identity sees in-place schema changes: ALTER TABLE RENAME COLUMN
// preserves the relation id and bumps rel_version only (verified live: rel_id
// 272771 kept, rel_version 0 -> 1), so a rename that lands between the
// binding probe and the second relation read mutates exactly the field an
// identity that ignores rel_version misses. The scope revalidates against
// the renamed schema and the removed column fails the probe; a rename the
// view survives revalidates and admits; a relation that keeps renaming under
// validation fails closed instead of looping.
func TestValidatePublicationTableScopeInPlaceRenameStability(t *testing.T) {
	db := testPublicationScopeDatabase
	ctx := defines.AttachAccount(context.Background(), sysAccountID, rootID, moAdminRoleID)
	relationsSQL := publicationRelationsSQL(db, sysAccountID)

	vRenDef := publicationViewDefWithDependencies(t,
		"CREATE VIEW v_ren AS SELECT a FROM t_rename WHERE a >= 1",
		[]plan.ViewDependency{publicationDependency(db, "t_rename", nil)})
	// Both sides of an in-place RENAME COLUMN keep the relation id; only the
	// version moves, exactly like the live catalog rows of the rename.
	const inPlaceRelationID = uint64(272771)
	beforeRename := [][]interface{}{
		{"t_rename", catalog.SystemOrdinaryRel, "", inPlaceRelationID, uint64(0)},
		{"v_ren", catalog.SystemViewRel, vRenDef, inPlaceRelationID + 1, uint64(0)},
	}
	afterRename := func(version uint64) [][]interface{} {
		return [][]interface{}{
			{"t_rename", catalog.SystemOrdinaryRel, "", inPlaceRelationID, version},
			{"v_ren", catalog.SystemViewRel, vRenDef, inPlaceRelationID + 1, uint64(0)},
		}
	}
	require.Equal(t, beforeRename[0][3], afterRename(1)[0][3],
		"the fixtures keep the relation id unchanged: an in-place RENAME COLUMN preserves it")
	require.NotEqual(t, beforeRename[0][4], afterRename(1)[0][4],
		"the fixtures bump rel_version: an in-place RENAME COLUMN increments it")

	newRenameBh := func(t *testing.T, relations [][]interface{}) *backgroundExecTest {
		t.Helper()
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[relationsSQL] = newMrsForPublicationRelations(relations)
		return bh
	}

	t.Run("a stable schema set validates once", func(t *testing.T) {
		bh := newRenameBh(t, beforeRename)
		err := validatePublicationTableScope(ctx, bh, db, []string{"t_rename", "v_ren"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.Equal(t, 2, countPublicationExecutedSQLs(bh, relationsSQL),
			"a stable scope reads the relations exactly twice")
		require.Equal(t, 1, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_ren")))
	})

	t.Run("an interleaved rename cannot be silently admitted", func(t *testing.T) {
		bh := newRenameBh(t, beforeRename)
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads == 2 {
				// The rename lands after v_ren's successful probe and before
				// the second relation read: the relation id stays, rel_version
				// bumps, and the column the view selects is gone. An identity
				// without rel_version compares the two reads equal and admits
				// a scope whose probe now fails on every subscriber query.
				bh.sql2result[relationsSQL] = newMrsForPublicationRelations(afterRename(1))
				bh.sql2err[publicationViewBindingSQL(db, "v_ren")] =
					errors.New("invalid input: column a does not exist")
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"t_rename", "v_ren"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"table-level publication for database 'view_pub_db' cannot publish view 'v_ren': its stored definition no longer binds the published relations: invalid input: column a does not exist")
	})

	t.Run("a rename the view survives revalidates and admits", func(t *testing.T) {
		bh := newRenameBh(t, beforeRename)
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads == 2 {
				// The rename of a column the view does not select still bumps
				// rel_version: the scope revalidates, the re-probe keeps
				// binding, and the repeat read stays coherent.
				bh.sql2result[relationsSQL] = newMrsForPublicationRelations(afterRename(1))
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"t_rename", "v_ren"}, generalSubscriberAccounts, int64(1))
		require.NoError(t, err)
		require.Equal(t, 4, countPublicationExecutedSQLs(bh, relationsSQL),
			"the bump forced a full second validation pass")
		require.Equal(t, 2, countPublicationExecutedSQLs(bh, publicationViewBindingSQL(db, "v_ren")),
			"the revalidated pass bound the view against the renamed schema")
	})

	t.Run("a relation that renames under every read fails closed", func(t *testing.T) {
		bh := newRenameBh(t, beforeRename)
		relationsReads := 0
		bh.beforeExec = func(sql string) {
			if sql != relationsSQL {
				return
			}
			relationsReads++
			if relationsReads%2 == 0 {
				bh.sql2result[relationsSQL] =
					newMrsForPublicationRelations(afterRename(uint64(relationsReads / 2)))
			}
		}
		err := validatePublicationTableScope(ctx, bh, db, []string{"t_rename", "v_ren"}, generalSubscriberAccounts, int64(1))
		require.Error(t, err)
		require.ErrorContains(t, err,
			"cannot publish relation 't_rename': its definition changed during validation, retry the statement")
	})
}

// TestPublicationBackgroundLowerCaseTableNamesResolution pins the executor
// contract of publicationBackgroundLowerCaseTableNames: the background
// executor an account-certifying replay actually runs on names the mode of
// its own back session (the registered default for a session without an
// upstream), while an executor that cannot name one is refused explicitly.
// Both properties are what makes resolving that mode only for
// table-list publications safe for the other replay shapes.
func TestPublicationBackgroundLowerCaseTableNamesResolution(t *testing.T) {
	mode, err := publicationBackgroundLowerCaseTableNames(
		context.Background(), &backExec{backSes: &backSession{}})
	require.NoError(t, err)
	require.Equal(t, int64(1), mode,
		"a back session without an upstream names the registered default mode")

	_, err = publicationBackgroundLowerCaseTableNames(
		context.Background(), &backgroundExecTest{})
	require.Error(t, err)
	require.ErrorContains(t, err,
		"session variable lower_case_table_names is unavailable")
}

// TestIsPublicationProbeInfrastructureErrorClassifiesFamilies pins the DDL
// publication-probe's error identity contract after the lock-wait-timeout
// family became statement-scoped in the shared whole-transaction rollback
// policy: a timeout (local or owner-side), deadlock, retry/write-conflict,
// backend loss and cancellation conditions keep their own infrastructure
// identity instead of receiving the stable broken-view verdict, while
// semantic definition failures (unknown table, bad input) and plain errors
// stay probe-semantic.
func TestIsPublicationProbeInfrastructureErrorClassifiesFamilies(t *testing.T) {
	for name, err := range map[string]error{
		"lock wait timeout":     moerr.NewLockWaitTimeoutNoCtx(),
		"remote owner timeout":  moerr.NewRemoteLockWaitTimeoutNoCtx(),
		"deadlock":              moerr.NewDeadLockDetectedNoCtx(),
		"write conflict":        moerr.NewTxnWriteConflictNoCtx("concurrent change"),
		"txn need retry":        moerr.NewTxnNeedRetryNoCtx(),
		"retry with def change": moerr.NewTxnNeedRetryWithDefChangedNoCtx(),
		"backend closed":        moerr.NewBackendClosedNoCtx(),
		"cancellation":          context.Canceled,
		"deadline exceeded":     context.DeadlineExceeded,
		"wrapped lock timeout":  fmt.Errorf("probe depth: %w", moerr.NewLockWaitTimeoutNoCtx()),
		"joined remote timeout": errors.Join(moerr.NewInternalErrorNoCtx("x"), moerr.NewRemoteLockWaitTimeoutNoCtx()),
	} {
		require.True(t, isPublicationProbeInfrastructureError(err),
			"%s must keep its own infrastructure identity", name)
	}
	for name, err := range map[string]error{
		"unknown table": moerr.NewNoSuchTableNoCtx("db", "v"),
		"invalid input": moerr.NewInvalidInputNoCtx("stored definition does not bind"),
		"plain error":   errors.New("resolution failed"),
		"nil error":     nil,
	} {
		require.False(t, isPublicationProbeInfrastructureError(err),
			"%s is a semantic property of the stored definition", name)
	}
}
