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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// A generation built before the latest committed source row cannot answer top-k.
func TestHnswLaggingGenerationFallsBackToSource(t *testing.T) {
	for _, tc := range []struct {
		name        string
		build       int64
		wantIndex   bool
		forceSource bool
	}{
		{name: "behind_source", build: 99},
		{name: "covers_source", build: 100, wantIndex: true},
		{name: "coverage_retry_uses_source", build: 100, forceSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, project, sortID, _ := newExactVectorFallbackApplyIndicesCase(t, 0, nil)
			if tc.forceSource {
				b.compCtx.SetContext(context.WithValue(b.compCtx.GetContext(), defines.HnswSnapshotFallbackKey{}, true))
			}
			scan := b.qry.Nodes[b.qry.Nodes[sortID].Children[0]]
			scan.ObjRef = &plan.ObjectRef{SchemaName: "db", ObjName: scan.TableDef.Name}
			scan.TableDef.TblId = 1
			scan.Stats = &plan.Stats{TableCnt: 2, Outcnt: 2, Selectivity: 1, Cost: 2}
			idx := newVectorJoinHnswIndex()
			definitions := make([]*plan.IndexDef, 0, len(idx.IndexDefs))
			storage := t.Name() + "_storage"
			for role, def := range idx.IndexDefs {
				def.Parts = []string{"vec"}
				def.TableExist = true
				def.IndexTableName = t.Name() + "_meta"
				if role == catalog.Hnsw_TblType_Storage {
					def.IndexTableName = storage
				}
				definitions = append(definitions, def)
			}
			scan.TableDef.Indexes = definitions
			proc := b.compCtx.GetProcess()
			proc.Base.TxnOperator = hnswLagTxn{snapshot: timestamp.Timestamp{PhysicalTime: 101}}
			proc.Base.SessionInfo.StorageEngine = hnswLagEngine{}
			t.Cleanup(func() { veccache.Cache.RemoveAllGenerations(storage, "test_cleanup") })
			_, _, err := veccache.Cache.Search(nil, storage, &hnswLagGeneration{build: tc.build}, nil, vectorindex.RuntimeConfig{})
			require.NoError(t, err)
			result, err := b.applyIndices(project, nil, nil)
			require.NoError(t, err)
			search := findHnswTableFunctionNode(b, result)
			if tc.wantIndex {
				require.NotNil(t, search)
			} else {
				require.Nil(t, search, "a stale HNSW candidate set can omit the just-committed nearest row")
				require.Equal(t, []int32{sortID}, b.qry.Nodes[result].Children)
				require.Equal(t, []int32{scan.NodeId}, b.qry.Nodes[sortID].Children)
				require.Equal(t, plan.Node_SORT, b.qry.Nodes[sortID].NodeType)
				require.Equal(t, plan.Node_TABLE_SCAN, scan.NodeType)
			}
		})
	}
}

type hnswLagTxn struct {
	client.TxnOperator
	snapshot timestamp.Timestamp
}

func (t hnswLagTxn) SnapshotTS() timestamp.Timestamp { return t.snapshot }
func (t hnswLagTxn) Txn() txn.TxnMeta                { return txn.TxnMeta{SnapshotTS: t.snapshot} }

type hnswLagEngine struct{ engine.Engine }

func (hnswLagEngine) GetRelationById(context.Context, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
	return "db", "t", hnswLagRelation{}, nil
}

type hnswLagRelation struct{ engine.Relation }

func (hnswLagRelation) SourceCommitTS(context.Context, types.TS) (types.TS, error) {
	return types.BuildTS(100, 0), nil
}

type hnswLagGeneration struct {
	veccache.VectorIndexSearchIf
	build int64
}

func (*hnswLagGeneration) Preload(*sqlexec.SqlProcess) error { return nil }
func (*hnswLagGeneration) Load(*sqlexec.SqlProcess) error    { return nil }
func (*hnswLagGeneration) Destroy()                          {}
func (*hnswLagGeneration) GetIndexSize() (int64, int64)      { return 0, 0 }
func (g *hnswLagGeneration) BuildTS() int64                  { return g.build }
func (*hnswLagGeneration) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{0}, nil
}
