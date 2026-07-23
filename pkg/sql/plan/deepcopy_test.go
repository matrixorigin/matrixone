// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestCloneTableDefForPlan(t *testing.T) {
	require.Nil(t, CloneTableDefForPlan(nil, true))

	colA := &planpb.ColDef{Name: "a"}
	colB := &planpb.ColDef{Name: "b"}
	index := &planpb.IndexDef{IndexName: "idx_a"}
	pkey := &planpb.PrimaryKeyDef{PkeyColName: "a"}
	source := &planpb.TableDef{
		Name:          "source",
		Cols:          []*planpb.ColDef{colA, colB},
		Indexes:       []*planpb.IndexDef{index},
		Pkey:          pkey,
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
	}

	cloned := CloneTableDefForPlan(source, true)
	require.NotSame(t, source, cloned)
	require.Equal(t, source, cloned)
	require.Same(t, colA, cloned.Cols[0])
	require.Same(t, index, cloned.Indexes[0])
	require.Same(t, pkey, cloned.Pkey)

	cloned.Name = "clone"
	cloned.Cols[0] = &planpb.ColDef{Name: "replacement"}
	cloned.Cols = append(cloned.Cols, &planpb.ColDef{Name: "c"})
	require.Equal(t, "source", source.Name)
	require.Same(t, colA, source.Cols[0])
	require.Len(t, source.Cols, 2)

	withoutCols := CloneTableDefForPlan(source, false)
	require.Nil(t, withoutCols.Cols)
	require.Same(t, index, withoutCols.Indexes[0])
	require.Same(t, pkey, withoutCols.Pkey)
}

func TestDeepCopyExprPreservesAuxID(t *testing.T) {
	expr := &planpb.Expr{
		AuxId:          planpb.CheckConstraintWarningFilterAuxID,
		WarningMessage: "Check constraint 't_chk_1' is violated",
	}

	copied := DeepCopyExpr(expr)

	require.NotSame(t, expr, copied)
	require.Equal(t, planpb.CheckConstraintWarningFilterAuxID, copied.AuxId)
	require.Equal(t, expr.WarningMessage, copied.WarningMessage)
}

var clonedTableDef *planpb.TableDef

func BenchmarkCloneTableDefForPlan(b *testing.B) {
	cols := make([]*planpb.ColDef, 64)
	name2ColIndex := make(map[string]int32, len(cols))
	for i := range cols {
		name := fmt.Sprintf("col_%d", i)
		cols[i] = &planpb.ColDef{
			Name:    name,
			Default: &planpb.Default{OriginString: "0"},
		}
		name2ColIndex[name] = int32(i)
	}
	indexes := make([]*planpb.IndexDef, 8)
	for i := range indexes {
		indexes[i] = &planpb.IndexDef{
			IndexName: fmt.Sprintf("idx_%d", i),
			Parts:     []string{cols[i].Name, cols[i+1].Name},
		}
	}
	tableDef := &planpb.TableDef{
		Cols:          cols,
		Indexes:       indexes,
		Name2ColIndex: name2ColIndex,
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: cols[0].Name,
			Names:       []string{cols[0].Name},
		},
	}

	b.Run("deep", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			clonedTableDef = DeepCopyTableDef(tableDef, true)
		}
	})
	b.Run("planner", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			clonedTableDef = CloneTableDefForPlan(tableDef, true)
		}
	})
}
