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
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Corrupt only transaction-owned authority on a real persisted relation. No
// successful proof or object-membership fixture is injected into the reader.
func TestM0ContractControlsActualRelationReadSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_contract_read_authority"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column tail int")
		svc := cn.RawService().(cnservice.Service)
		for _, kind := range []string{"valid", "missing", "format", "account", "binding", "semantics"} {
			t.Run(kind, func(t *testing.T) {
				op, err := svc.GetTxnClient().New(ctx, timestamp.Timestamp{}, client.WithBeginAutoCommit(false, true), client.WithTxnMode(txn.TxnMode_Pessimistic), client.WithTxnIsolation(txn.TxnIsolation_RC))
				require.NoError(t, err)
				defer op.Rollback(ctx)
				require.NoError(t, svc.GetEngine().New(ctx, op))
				database, err := svc.GetEngine().Database(ctx, schema, op)
				require.NoError(t, err)
				rel, err := database.Relation(ctx, "customer", nil)
				require.NoError(t, err)
				extra := rel.GetExtraInfo()
				require.NotNil(t, extra.M0Evolution)
				switch kind {
				case "missing":
					extra.M0Evolution = nil
				case "format":
					extra.M0Evolution.FormatVersion++
				case "account":
					extra.M0Evolution.AccountId++
				case "binding":
					extra.M0Evolution.RelationId++
				case "semantics":
					extra.M0Evolution.Columns[0].Semantics[0] ^= 1
					extra.M0Evolution.SchemaDigest = m0read.EvolutionDigest(extra.M0Evolution.Columns)
				}
				var outputType types.Type
				for _, col := range rel.GetTableDef(ctx).Cols {
					if col.Name == "tail" {
						outputType = vector.ProtoTypeToType(col.Typ)
					}
				}
				require.Equal(t, types.T_int32, outputType.Oid)
				metadata, metadataErr := rel.GetColumMetadataScanInfo(ctx, "tail", false)
				if kind == "valid" {
					require.NoError(t, metadataErr)
					require.NotEmpty(t, metadata)
					var nulls int64
					for _, m := range metadata {
						nulls += m.NullCnt
					}
					require.Equal(t, int64(3), nulls)
				} else {
					require.Error(t, metadataErr, "metadata consumer must reject invalid authority")
				}
				minmax, _, minmaxErr := rel.MaxAndMinValues(ctx)
				if kind == "valid" {
					require.NoError(t, minmaxErr)
					require.Equal(t, [2]any{nil, nil}, minmax[len(minmax)-1])
				} else {
					require.Error(t, minmaxErr)
					_, statsErr := rel.Stats(ctx, false)
					require.Error(t, statsErr, "cached statistics cannot bypass owner validation")
				}
				data, readErr := rel.Ranges(ctx, engine.DefaultRangesParam)
				var rows int
				if readErr == nil {
					proc := rel.GetProcess().(*process.Process)
					var readers []engine.Reader
					readers, readErr = rel.BuildReaders(ctx, proc, nil, data, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
					for _, reader := range readers {
						defer reader.Close()
					}
					if readErr == nil {
						for _, reader := range readers {
							for {
								out := batch.NewWithSize(1)
								out.Vecs[0] = vector.NewVec(outputType)
								done, e := reader.Read(ctx, []string{"tail"}, nil, proc.Mp(), out)
								rows += out.RowCount()
								if kind == "valid" {
									for i := 0; i < out.RowCount(); i++ {
										require.True(t, out.Vecs[0].IsNull(uint64(i)))
									}
								}
								out.Clean(proc.Mp())
								if e != nil {
									readErr = e
									break
								}
								if done {
									break
								}
							}
							if readErr != nil {
								break
							}
						}
					}
				}
				if kind == "valid" {
					require.NoError(t, readErr)
					require.Equal(t, 3, rows)
				} else {
					require.Error(t, readErr, "shape cannot replace explicit authority")
					require.ErrorContains(t, readErr, "evolution")
				}
			})
		}
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where tail is null"))
	})
}
