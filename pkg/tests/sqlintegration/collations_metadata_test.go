// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

type collationsUpgradeExecutor struct {
	executor.SQLExecutor
	insertError error
	deletes     int
	inserts     int
}
type collationsUpgradeTxn struct {
	executor.TxnExecutor
	owner *collationsUpgradeExecutor
}

func (e *collationsUpgradeExecutor) ExecTxn(ctx context.Context, body func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.SQLExecutor.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		return body(&collationsUpgradeTxn{TxnExecutor: tx, owner: e})
	}, opts)
}
func (tx *collationsUpgradeTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	if sql == sysview.InformationSchemaCollationsData && tx.owner.insertError != nil {
		return executor.Result{}, tx.owner.insertError
	}
	res, err := tx.TxnExecutor.Exec(sql, opts)
	if err == nil {
		switch sql {
		case "DELETE FROM information_schema.COLLATIONS":
			tx.owner.deletes++
		case sysview.InformationSchemaCollationsData:
			tx.owner.inserts++
		}
	}
	return res, err
}

func TestCollationsMetadataUpgrade(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		svc := cn.RawService().(cnservice.Service)
		internal := svc.GetSQLExecutor()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		require.NoError(t, testutils.WaitSystemBootstrap(ctx, db))
		const account = "collations_metadata_upgrade"
		defer cleanupSQLIntegration(t, cn, "drop account if exists "+account)
		execSQLRequire(t, ctx, db, "create account "+account+" admin_name 'root' identified by '111'")
		var accountID int32
		require.NoError(t, db.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		tenantDB, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer tenantDB.Close()
		tenantDB.SetMaxOpenConns(1)

		type metadata struct {
			name, charset string
			id            int64
			def, compiled string
			sortlen       int64
			pad           string
		}
		// Independent released identities: do not derive the oracle from the INSERT
		// or SHOW registry, since both surfaces could otherwise be empty together.
		expected := []metadata{
			{"binary", "binary", 63, "YES", "Yes", 1, "NO PAD"},
			{"utf8_bin", "utf8", 83, "", "Yes", 1, "PAD SPACE"},
			{"utf8_general_ci", "utf8", 33, "YES", "Yes", 1, "PAD SPACE"},
			{"utf8_unicode_ci", "utf8", 192, "", "Yes", 8, "PAD SPACE"},
			{"utf8mb4_bin", "utf8mb4", 46, "", "Yes", 1, "PAD SPACE"},
			{"utf8mb4_general_ci", "utf8mb4", 45, "YES", "Yes", 1, "PAD SPACE"},
			{"utf8mb4_unicode_ci", "utf8mb4", 224, "", "Yes", 8, "PAD SPACE"},
			{"utf8mb4_0900_ai_ci", "utf8mb4", 255, "", "Yes", 1, "PAD SPACE"},
		}
		query := func(t *testing.T, connection *sql.DB, statement string) []metadata {
			t.Helper()
			rows, err := connection.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			var result []metadata
			for rows.Next() {
				var row metadata
				require.NoError(t, rows.Scan(&row.name, &row.charset, &row.id, &row.def, &row.compiled, &row.sortlen, &row.pad))
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		const selectCollations = "SELECT COLLATION_NAME,CHARACTER_SET_NAME,ID,IS_DEFAULT,IS_COMPILED,SORTLEN,PAD_ATTRIBUTE FROM information_schema.COLLATIONS"
		assertMetadata := func(t *testing.T, connection *sql.DB) {
			t.Helper()
			selected := query(t, connection, selectCollations)
			require.ElementsMatch(t, expected, selected)
			require.ElementsMatch(t, selected, query(t, connection, "SHOW COLLATION"))
		}
		t.Run("fresh system and tenant", func(t *testing.T) { assertMetadata(t, db); assertMetadata(t, tenantDB) })
		version := func() string {
			t.Helper()
			var value string
			require.NoError(t, db.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&value))
			return value
		}
		seed := func(t *testing.T, statements ...string) {
			t.Helper()
			require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				for _, statement := range statements {
					res, err := tx.Exec(statement, versions.UpgradeStatementOption(uint32(accountID)))
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()))
		}
		oldVersion := func(t *testing.T, value string) {
			t.Helper()
			res, err := internal.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_account set create_version='%s' where account_id=%d", value, accountID), executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
			require.NoError(t, err)
			res.Close()
		}
		newUpgrader := func(t *testing.T) (bootstrap.Service, *collationsUpgradeExecutor) {
			t.Helper()
			owner := &collationsUpgradeExecutor{SQLExecutor: internal}
			upgrader := bootstrap.NewService(cn.ServiceID(), nil, svc.GetClock(), svc.GetTxnClient(), owner)
			t.Cleanup(func() { require.NoError(t, upgrader.Close()) })
			return upgrader, owner
		}
		upgrade := func(upgrader bootstrap.Service) (bool, error) {
			return upgrader.MaybeUpgradeTenant(ctx, func() (int32, string, error) { return accountID, "", nil }, nil)
		}

		for _, tc := range []struct {
			name, version, seed string
			count               int
		}{
			{"historical six rows", "4.0.13", "DELETE FROM information_schema.COLLATIONS WHERE COLLATION_NAME IN ('utf8_unicode_ci','utf8mb4_unicode_ci')", 6},
			{"empty legacy table", "4.0.15", "DELETE FROM information_schema.COLLATIONS", 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				seed(t, tc.seed)
				require.Len(t, query(t, tenantDB, selectCollations), tc.count)
				require.Len(t, query(t, tenantDB, "SHOW COLLATION"), 8)
				oldVersion(t, tc.version)
				upgrader, owner := newUpgrader(t)
				upgraded, err := upgrade(upgrader)
				require.NoError(t, err)
				require.True(t, upgraded)
				require.Equal(t, upgrader.GetFinalVersion(), version())
				require.Equal(t, 1, owner.deletes)
				require.Equal(t, 1, owner.inserts)
				assertMetadata(t, tenantDB)
				upgraded, err = upgrade(upgrader)
				require.NoError(t, err)
				require.False(t, upgraded)
				require.Equal(t, 1, owner.deletes, "repeat must not replace current rows")
				assertMetadata(t, db)
			})
		}
		t.Run("delete rolls back on insert failure then retries", func(t *testing.T) {
			seed(t, "DELETE FROM information_schema.COLLATIONS WHERE COLLATION_NAME IN ('utf8_unicode_ci','utf8mb4_unicode_ci')")
			before := query(t, tenantDB, selectCollations)
			oldVersion(t, "4.0.15")
			upgrader, owner := newUpgrader(t)
			injected := errors.New("collations insert failure")
			owner.insertError = injected
			_, err := upgrade(upgrader)
			require.ErrorIs(t, err, injected)
			require.Equal(t, 1, owner.deletes, "the real DELETE completed inside the failed transaction")
			require.Equal(t, 0, owner.inserts)
			require.ElementsMatch(t, before, query(t, tenantDB, selectCollations))
			require.Equal(t, "4.0.15", version(), "failed transaction must not advance tenant version")
			owner.insertError = nil
			upgraded, err := upgrade(upgrader)
			require.NoError(t, err)
			require.True(t, upgraded)
			require.Equal(t, upgrader.GetFinalVersion(), version())
			assertMetadata(t, tenantDB)
		})
		// Preserve row count while falsifying each metadata field. The predicate
		// must compare full rows rather than accepting a count-only completion.
		for _, assignment := range []string{
			"COLLATION_NAME='stale_name'", "CHARACTER_SET_NAME='stale_charset'", "ID=999",
			"IS_DEFAULT=''", "IS_COMPILED='No'", "SORTLEN=99", "PAD_ATTRIBUTE='PAD SPACE'",
		} {
			t.Run(assignment, func(t *testing.T) {
				seed(t, "UPDATE information_schema.COLLATIONS SET "+assignment+" WHERE COLLATION_NAME='binary'")
				oldVersion(t, "4.0.15")
				upgrader, owner := newUpgrader(t)
				upgraded, err := upgrade(upgrader)
				require.NoError(t, err)
				require.True(t, upgraded)
				require.Equal(t, 1, owner.deletes)
				assertMetadata(t, tenantDB)
			})
		}
		t.Run("same count duplicate is repaired", func(t *testing.T) {
			seed(t, "DELETE FROM information_schema.COLLATIONS WHERE COLLATION_NAME='utf8_unicode_ci'",
				"INSERT INTO information_schema.COLLATIONS SELECT * FROM information_schema.COLLATIONS WHERE COLLATION_NAME='binary'")
			oldVersion(t, "4.0.15")
			upgrader, owner := newUpgrader(t)
			upgraded, err := upgrade(upgrader)
			require.NoError(t, err)
			require.True(t, upgraded)
			require.Equal(t, 1, owner.deletes)
			assertMetadata(t, tenantDB)
		})
		t.Run("exact rows require no replacement", func(t *testing.T) {
			oldVersion(t, "4.0.15")
			upgrader, owner := newUpgrader(t)
			upgraded, err := upgrade(upgrader)
			require.NoError(t, err)
			require.True(t, upgraded)
			require.Equal(t, upgrader.GetFinalVersion(), version())
			require.Zero(t, owner.deletes)
			require.Zero(t, owner.inserts)
			assertMetadata(t, tenantDB)
		})
	})
}
