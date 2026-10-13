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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPartitionPKCheckUsesLockedIdentity(t *testing.T) {
	col := func(pos int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	for _, tc := range []struct {
		name       string
		partition  *plan.Expr
		composite  bool
		explicit   int32
		wantColumn int32
		wantAll    bool
		rowIDFirst bool
	}{
		{name: "resolved PK differs from incoming schema position", partition: col(0), explicit: -1, wantColumn: 5},
		{name: "partition expression", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0)}}}}, explicit: -1, wantColumn: 5},
		{name: "repeated PK references", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(0)}}}}, explicit: -1, wantColumn: 5},
		{name: "explicit mixed columns", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(1)}}}}, explicit: 3, wantAll: true},
		{name: "explicit partition column", partition: col(1), explicit: 3, wantColumn: 3},
		{name: "partition on another column", partition: col(1), explicit: -1, wantAll: true},
		{name: "composite PK is not a scalar partition value", partition: col(0), composite: true, explicit: -1, wantAll: true},
		{name: "hidden index array", explicit: -1, wantColumn: -1},
		{name: "leading row ID", partition: col(0), explicit: -1, wantColumn: 4, rowIDFirst: true},
		{name: "multi column partition expression", partition: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(1)}}}}, explicit: -1, wantAll: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := &plan.TableDef{
				Name2ColIndex: map[string]int32{"id": 0, "uk": 1},
				Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			}
			if tc.partition != nil {
				def.Partition = &plan.Partition{PartitionDefs: []*plan.PartitionDef{{Def: tc.partition}}}
			}
			if tc.composite {
				def.Pkey = &plan.PrimaryKeyDef{PkeyColName: "__mo_cpkey_col", Names: []string{"id", "uk"}}
			}
			relation := &mockRelation{}
			pruned, all := 0, 0
			input := batch.NewWithSize(6)
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			if tc.rowIDFirst {
				input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			}
			table := &combinedTxnTable{
				primary: &txnTable{tableDef: def},
				prunePKFunc: func(got *batch.Batch, index int32) ([]engine.Relation, error) {
					pruned++
					require.Same(t, input, got)
					require.Equal(t, tc.wantColumn, index)
					return []engine.Relation{relation}, nil
				},
				tablesFunc: func() ([]engine.Relation, error) {
					all++
					return []engine.Relation{relation}, nil
				},
			}
			relations, err := table.relationsForPKCheck(context.Background(), input, 5, tc.explicit)
			require.NoError(t, err)
			require.Equal(t, []engine.Relation{relation}, relations)
			if tc.wantAll {
				require.Equal(t, 1, all)
				require.Zero(t, pruned)
			} else {
				require.Equal(t, 1, pruned)
				require.Zero(t, all)
			}
		})
	}
}

func TestPartitionPKCheckChecksEveryPartitionExpression(t *testing.T) {
	def := &plan.TableDef{
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Partition: &plan.Partition{PartitionDefs: []*plan.PartitionDef{
			{Def: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
			{Def: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}},
		}},
	}
	require.False(t, partitionUsesOnlyPrimaryKey(def))
	def.Partition.PartitionDefs = def.Partition.PartitionDefs[:1]
	require.True(t, partitionUsesOnlyPrimaryKey(def))
	delete(def.Name2ColIndex, "id")
	require.False(t, partitionUsesOnlyPrimaryKey(def))
}
