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
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func m0ReadEvolution(t *testing.T, ctx context.Context, db *sql.DB, schema string) *api.M0EvolutionContract {
	t.Helper()
	var raw []byte
	var tableID, databaseID uint64
	require.NoError(t, db.QueryRowContext(ctx, "select rel_id,extra_info from mo_catalog.mo_tables where reldatabase=? and relname='customer'", schema).Scan(&tableID, &raw))
	require.NoError(t, db.QueryRowContext(ctx, "select dat_id from mo_catalog.mo_database where datname=?", schema).Scan(&databaseID))
	var extra api.SchemaExtra
	require.NoError(t, extra.Unmarshal(raw))
	require.NoError(t, m0read.ValidateEvolution(extra.M0Evolution, m0read.EvolutionBinding{Account: 0, Database: databaseID, Relation: tableID}))
	return extra.M0Evolution
}

func m0CheckpointManifest(t *testing.T, ctx context.Context, cn embed.ServiceOperator) []string {
	t.Helper()
	fs := cn.RawService().(cnservice.Service).GetEngine().(*disttae.Engine).FS()
	var names []string
	for entry, err := range fs.List(ctx, ioutil.GetCheckpointDir()) {
		if err != nil && (os.IsNotExist(err) || moerr.IsMoErrCode(err, moerr.ErrFileNotFound)) {
			break
		}
		require.NoError(t, err)
		if !entry.IsDir && strings.HasPrefix(entry.Name, "meta_") {
			names = append(names, fmt.Sprintf("%s:%d", entry.Name, entry.Size))
		}
	}
	sort.Strings(names)
	return names
}

func TestM0EvolutionContractTNRecoverySQL(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		name := "wal"
		if checkpoint {
			name = "checkpoint_wal"
		}
		t.Run(name, func(t *testing.T) {
			runSQLIntegration(t, func(cluster embed.Cluster) {
				ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
				defer cancel()
				cn, err := cluster.GetCNService(0)
				require.NoError(t, err)
				schema := "m0_contract_recovery_" + name
				baseline := m0CheckpointManifest(t, ctx, cn)
				db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
				running := true
				defer func() {
					if db != nil {
						_ = db.Close()
					}
					if !running {
						if err := cluster.Start(); err != nil {
							t.Errorf("cleanup restart: %v", err)
							return
						}
					}
					cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
				}()
				root := m0ReadEvolution(t, ctx, db, schema)
				require.Zero(t, root.LayoutGeneration)
				if checkpoint {
					m0Exec(t, ctx, db, "select mo_ctl('dn','globalcheckpoint','')")
					baseline = m0CheckpointManifest(t, ctx, cn)
					require.NotEmpty(t, baseline)
				}
				m0Exec(t, ctx, db, "alter table "+table+" add column recovered int")
				m0Exec(t, ctx, db, "update "+table+" set recovered=17 where c_id=1")
				m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
				before := m0ReadEvolution(t, ctx, db, schema)
				require.Equal(t, uint32(1), before.LayoutGeneration)
				require.Equal(t, root.RootId, before.RootId)
				require.Equal(t, baseline, m0CheckpointManifest(t, ctx, cn), "the post-baseline DDL must remain a WAL tail")
				require.NoError(t, db.Close())
				db = nil
				require.NoError(t, cluster.Stop())
				running = false
				require.NoError(t, cluster.Start())
				running = true
				cn, err = cluster.GetCNService(0)
				require.NoError(t, err)
				db = openM0SQL(t, cn)
				require.Equal(t, baseline, m0CheckpointManifest(t, ctx, cn), "restart must not replace WAL coverage with a newer checkpoint")
				require.Equal(t, before, m0ReadEvolution(t, ctx, db, schema))
				require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where recovered=17"))
				require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where recovered is null"))
				m0Exec(t, ctx, db, "alter table "+table+" add column after_restart int")
				require.Equal(t, uint32(2), m0ReadEvolution(t, ctx, db, schema).LayoutGeneration)
				require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where after_restart is null"))
			})
		})
	}
}

// Open real catalog relations at an earlier transaction snapshot after ADD.
// The authority is obtained from restored catalog state, not installed by a fixture.
func TestM0EvolutionContractHistoricalRelationSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_contract_historical"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		root := m0ReadEvolution(t, ctx, db, schema)
		svc := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		var before timestamp.Timestamp
		abort := errors.New("rollback catalog read control")
		err = svc.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			before = tx.Txn().SnapshotTS()
			return abort
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, abort)
		m0Exec(t, ctx, db, "alter table "+table+" add column later int")
		current := m0ReadEvolution(t, ctx, db, schema)
		require.Equal(t, uint32(1), current.LayoutGeneration)
		require.Equal(t, root.RootId, current.RootId)
		err = svc.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			historical := tx.Txn().CloneSnapshotOp(before)
			hdb, err := svc.GetEngine().Database(txnCtx, schema, historical)
			require.NoError(t, err)
			hrel, err := hdb.Relation(txnCtx, "customer", nil)
			require.NoError(t, err)
			hcontract := hrel.GetExtraInfo().M0Evolution
			require.Equal(t, root, hcontract)
			require.NoError(t, m0read.ValidateEvolution(hcontract, m0read.EvolutionBinding{Account: 0, Database: hrel.GetDBID(txnCtx), Relation: hrel.GetTableID(txnCtx)}))
			hdef := hrel.GetTableDef(txnCtx)
			for _, col := range hdef.Cols {
				require.NotEqual(t, "later", col.Name)
			}
			cdb, err := svc.GetEngine().Database(txnCtx, schema, tx.Txn())
			require.NoError(t, err)
			crel, err := cdb.Relation(txnCtx, "customer", nil)
			require.NoError(t, err)
			require.Equal(t, current, crel.GetExtraInfo().M0Evolution)
			require.Equal(t, root, hrel.GetExtraInfo().M0Evolution, "opening current schema cannot mutate historical authority")
			return abort
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, abort)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where later is null"))
	})
}

// Equal layout generations are not object ownership. Two branches introduce
// different columns at the same physical identity and retain independent data.
func TestM0EvolutionSiblingLayoutIsolationSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const parent, left, right = "m0_contract_sibling_parent", "m0_contract_sibling_left", "m0_contract_sibling_right"
		db, table := m0PrepareRollbackTable(t, ctx, cn, parent)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+right, "drop database if exists "+left, "drop database if exists "+parent)
		before := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, before)
		m0Exec(t, ctx, db, "create database "+left+" clone "+parent)
		m0Exec(t, ctx, db, "create database "+right+" clone "+parent)
		m0Exec(t, ctx, db, "alter table "+left+".customer add column left_only int")
		m0Exec(t, ctx, db, "alter table "+right+".customer add column right_only int")
		lc, rc := m0ReadEvolution(t, ctx, db, left), m0ReadEvolution(t, ctx, db, right)
		root := m0ReadEvolution(t, ctx, db, parent)
		require.Equal(t, root.RootId, lc.RootId)
		require.Equal(t, root.RootId, rc.RootId)
		require.Equal(t, uint32(1), lc.LayoutGeneration)
		require.Equal(t, lc.LayoutGeneration, rc.LayoutGeneration)
		require.NotEqual(t, lc.RelationId, rc.RelationId)
		require.NotEqual(t, lc.DatabaseId, rc.DatabaseId)
		require.NotEqual(t, lc.SchemaDigest, rc.SchemaDigest, "same generation must not erase branch-specific semantics")
		m0Exec(t, ctx, db, "update "+left+".customer set left_only=11 where c_id=1")
		m0Exec(t, ctx, db, "update "+right+".customer set right_only=22 where c_id=2")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+left+".customer')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+right+".customer')")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+left+".customer where left_only=11 and c_id=1"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+right+".customer where right_only=22 and c_id=2"))
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+left+".customer where left_only is null"))
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+right+".customer where right_only is null"))
		require.Equal(t, before, m0ObjectFingerprints(t, ctx, db, table))
		lobjects, robjects := m0ObjectFingerprints(t, ctx, db, left+".customer"), m0ObjectFingerprints(t, ctx, db, right+".customer")
		privateLeft, privateRight := 0, 0
		for _, obj := range lobjects {
			if !slices.Contains(before, obj) {
				privateLeft++
				require.NotContains(t, robjects, obj, "left output must not join right membership")
			}
		}
		for _, obj := range robjects {
			if !slices.Contains(before, obj) {
				privateRight++
				require.NotContains(t, lobjects, obj, "right output must not join left membership")
			}
		}
		require.Positive(t, privateLeft)
		require.Positive(t, privateRight)
	})
}
