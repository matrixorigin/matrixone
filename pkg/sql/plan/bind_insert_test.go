// Copyright 2021 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertMainPrimaryKeyLockIdentity(t *testing.T) {
	for _, tc := range []struct {
		name           string
		sql            string
		autoIncrement  bool
		noUnique       bool
		resolvedTarget bool
	}{
		{
			name: "explicit primary key with unique conflict",
			sql: "insert into dept values (10001, 'Sales', 'NY') " +
				"on duplicate key update loc = 'LA'",
			resolvedTarget: true,
		},
		{
			name: "auto increment primary key with unique conflict",
			sql: "insert into dept(dname, loc) values ('Sales', 'NY') " +
				"on duplicate key update loc = 'LA'",
			autoIncrement: true, resolvedTarget: true,
		},
		{
			name: "nullable unique key without conflict",
			sql: "insert into dept values (10001, null, 'NY') " +
				"on duplicate key update loc = 'LA'",
			resolvedTarget: true,
		},
		{
			name: "ordinary insert",
			sql:  "insert into dept values (10001, 'Sales', 'NY')",
		},
		{
			name: "on duplicate without secondary unique key",
			sql: "insert into dept values (10001, 'Sales', 'NY') " +
				"on duplicate key update loc = 'LA'",
			noUnique: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			tableDef := mock.ctxt.tables["dept"]
			tableDef.Cols[0].Typ.AutoIncr = tc.autoIncrement
			if tc.noUnique {
				tableDef.Indexes = nil
			}
			// The real catalog supplies these positions for hidden index tables;
			// the general planner mock leaves them unset until a table is bound.
			for _, idx := range tableDef.Indexes {
				indexTable := mock.ctxt.tables[idx.IndexTableName]
				indexTable.Name2ColIndex = make(map[string]int32, len(indexTable.Cols))
				for pos, col := range indexTable.Cols {
					indexTable.Name2ColIndex[col.Name] = int32(pos)
				}
			}
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			var lockKey *plan.Expr
			var baseLock, actionOwner *plan.Node
			for _, node := range query.Nodes {
				if node.PreInsertUkCtx.GetOdkuActions() != nil {
					require.Nil(t, actionOwner, "expected one action owner")
					actionOwner = node
				}
				if node.NodeType != plan.Node_LOCK_OP {
					continue
				}
				for _, target := range node.LockTargets {
					if target.TableId != tableDef.TblId {
						continue
					}
					require.Nil(t, lockKey, "expected one base-table lock")
					require.Len(t, node.Children, 1)
					input := query.Nodes[node.Children[0]]
					require.Equal(t, plan.Node_PROJECT, input.NodeType)
					require.GreaterOrEqual(t, target.PrimaryColIdxInBat, int32(0))
					require.Less(t, int(target.PrimaryColIdxInBat), len(input.ProjectList))
					lockKey = input.ProjectList[target.PrimaryColIdxInBat]
					baseLock = node
					require.Equal(t, lockKey.Typ.Id, target.PrimaryColTyp.Id)
				}
			}
			require.NotNil(t, lockKey, "expected a base-table primary-key lock")
			if !tc.resolvedTarget {
				require.NotNil(t, lockKey.GetCol(), "ordinary insert paths must retain the incoming PK lock")
				return
			}

			// Evaluate the actual remapped lock expression against a tiny mixed
			// input: UK conflict, no existing target, and PK conflict. Its target
			// subexpression must stay NULL when no original row was prefetched.
			require.NotNil(t, lockKey.GetF())
			require.Equal(t, "coalesce", lockKey.GetF().Func.ObjName)
			require.Len(t, lockKey.GetF().Args, 2)
			resolved := lockKey.GetF().Args[0]
			incoming := lockKey.GetF().Args[1]
			require.NotNil(t, actionOwner)
			require.Same(t, baseLock, query.Nodes[actionOwner.Children[0]])
			actions := actionOwner.PreInsertUkCtx.OdkuActions
			require.Len(t, actionOwner.ProjectList, len(actions.SourceColumns)+len(actions.OldColumns))
			for i, old := range actions.OldColumns {
				output := actionOwner.ProjectList[len(actions.SourceColumns)+i]
				require.Equal(t, old.GetCol(), output.GetCol(), "the original image has a single output position per column")
			}
			// The actual ordinary ODKU plan exposes the immutable direct keys
			// used by the execution owner's delayed-original optimization.
			require.Nil(t, actions.PriorData)
			require.Empty(t, actions.ForeignKeyChecks)
			require.Negative(t, actions.ActionFinalColumn)
			for _, key := range actions.Constraints {
				column := key.CurrentKey.GetCol()
				require.NotNil(t, column)
				require.Equal(t, int32(0), column.RelPos)
				require.GreaterOrEqual(t, actions.InputColumns[column.ColPos], int32(0))
				for _, updated := range actions.UpdateColumns {
					require.NotEqual(t, updated, column.ColPos)
				}
			}
			originalPK := actions.OldColumns[actions.PkColumn].GetCol()
			require.NotNil(t, originalPK)
			require.Equal(t, int32(0), originalPK.RelPos)
			forwardedKey := baseLock.ProjectList[originalPK.ColPos].GetCol()
			require.NotNil(t, forwardedKey)
			originalDefinition := query.Nodes[baseLock.Children[0]].ProjectList[forwardedKey.ColPos].GetCol()
			require.NotNil(t, originalDefinition)
			require.Equal(t, resolved.GetCol().RelPos, originalDefinition.RelPos)
			require.Equal(t, resolved.GetCol().ColPos, originalDefinition.ColPos, "the owner and lock must refer to the same prefetched original PK")
			require.NotNil(t, resolved.GetCol())
			refs := []*plan.Expr{resolved, incoming}
			maxPos := int32(0)
			for _, ref := range refs {
				require.NotNil(t, ref.GetCol())
				require.Equal(t, int32(0), ref.GetCol().RelPos, "lock expressions must read their input, not their own output")
				maxPos = max(maxPos, ref.GetCol().ColPos)
			}
			proc := testutil.NewProc(t)
			defer proc.Free()
			input := batch.NewWithSize(int(maxPos) + 1)
			defer input.Clean(proc.Mp())
			for pos := range input.Vecs {
				input.Vecs[pos] = vector.NewVec(types.T_uint32.ToType())
				for row := 0; row < 3; row++ {
					value, isNull := uint32(0), true
					switch int32(pos) {
					case refs[0].GetCol().ColPos:
						value, isNull = 1, row == 1
						if row == 2 {
							value = 2
						}
					case refs[1].GetCol().ColPos:
						value, isNull = 10001, false
					}
					require.NoError(t, vector.AppendFixed(input.Vecs[pos], value, isNull, proc.Mp()))
				}
			}
			input.SetRowCount(3)
			for _, expr := range []*plan.Expr{resolved, lockKey} {
				func() {
					exec, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					defer exec.Free()
					result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					values := vector.MustFixedColWithTypeCheck[uint32](result)
					require.Equal(t, uint32(1), values[0])
					require.Equal(t, uint32(2), values[2])
					if expr == resolved {
						require.True(t, result.GetNulls().Contains(1), "no target must remain an INSERT")
					} else {
						require.False(t, result.HasNull(), "new rows must lock their incoming primary key")
						require.Equal(t, uint32(10001), values[1])
					}
				}()
			}
		})
	}
}

func TestGetValidIndexesKeepsRegularIndexThatCoversPrimaryKey(t *testing.T) {
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"user_id", "session_id", "id"},
			PkeyColName: catalog.CPrimaryKeyColName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:  "idx_ret",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: true,
				Unique:     false,
				Parts: []string{
					"status",
					"due",
					"user_id",
					"session_id",
					"id",
					catalog.CreateAlias(catalog.CPrimaryKeyColName),
				},
			},
			{
				IndexName:  "uq_token_pk",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: true,
				Unique:     true,
				Parts: []string{
					"token",
					"user_id",
					"session_id",
					"id",
				},
			},
			{
				IndexName:  "idx_pending",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: false,
				Parts:      []string{"status"},
			},
		},
	}

	got, hasIrregular := getValidIndexes(tableDef)

	assert.False(t, hasIrregular)
	if assert.Len(t, got, 2) {
		assert.Equal(t, "idx_ret", got[0].IndexName)
		assert.Equal(t, "uq_token_pk", got[1].IndexName)
	}
}

// TestGetIrregularIndexes verifies that existing IVF / fulltext / master indexes
// (synchronously maintained) are returned, while regular indexes, indexes whose
// hidden table has not been created, and HNSW/CAGRA/IVF-PQ indexes (cron-maintained)
// are filtered out.
func TestGetIrregularIndexes(t *testing.T) {
	t.Run("nil/empty table", func(t *testing.T) {
		assert.Nil(t, getIrregularIndexes(nil))
		assert.Nil(t, getIrregularIndexes(&plan.TableDef{}))
	})

	t.Run("mixed indexes", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				// regular unique index -> filtered out
				{IndexName: "uk", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true},
				// regular btree index -> filtered out
				{IndexName: "bt", IndexAlgo: catalog.MoIndexBTreeAlgo.ToString(), TableExist: true},
				// ivf index, table exists -> kept
				{IndexName: "ivf", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: true},
				// fulltext index, table exists -> kept
				{IndexName: "ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), TableExist: true},
				// master index -> kept (full synchronous modern maintenance)
				{IndexName: "mst", IndexAlgo: catalog.MOIndexMasterAlgo.ToString(), TableExist: true},
				// hnsw index -> filtered out (cron-maintained, no inline sub-plan)
				{IndexName: "hnsw", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), TableExist: true},
				// ivf index but hidden table not yet created -> filtered out
				{IndexName: "ivf_pending", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: false},
			},
		}

		got := getIrregularIndexes(tableDef)
		names := make([]string, 0, len(got))
		for _, idx := range got {
			names = append(names, idx.IndexName)
			assert.True(t, isModernMaintainedIrregularAlgo(idx.IndexAlgo))
			assert.True(t, idx.TableExist)
		}
		assert.ElementsMatch(t, []string{"ivf", "ft", "mst"}, names)
	})

	t.Run("only regular indexes returns nil", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{IndexName: "uk", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true},
			},
		}
		assert.Nil(t, getIrregularIndexes(tableDef))
	})
}
