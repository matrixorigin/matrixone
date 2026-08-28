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

package readutil

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func varcharFilterPayload(t *testing.T, mp *mpool.MPool, values ...string) []byte {
	t.Helper()
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	return data
}

func varcharPKTableDef() *plan.TableDef {
	colTyp := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{{
			Name:    "k",
			ColId:   0,
			Seqnum:  0,
			Typ:     colTyp,
			Primary: true,
		}},
		Name2ColIndex: map[string]int32{"k": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "k", Names: []string{"k"}},
	}
}

func prefixInExpr(data []byte, rows int) *plan.Expr {
	colTyp := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "prefix_in"},
			Args: []*plan.Expr{
				{Typ: colTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}},
				{Typ: colTyp, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
					Len: int32(rows), Data: data,
				}}},
			},
		}},
	}
}

func varcharBlockMeta(t *testing.T, values ...string) objectio.BlockObject {
	t.Helper()
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zm := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(zm, []byte(value))
	}
	meta.MustGetColumn(0).SetZoneMap(zm)
	return meta
}

// ZM.PrefixIn binary-searches the value list, so an unsorted payload makes it
// probe the wrong element and prune a block that genuinely holds a match. The
// producer is not required to sort: the pk_filter path normalizes for exactly
// this reason, and this path must too.
func TestCompileFilterExprPrunesConsistentlyForUnsortedPrefixIn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableDef := varcharPKTableDef()
	// The block holds keys in ["a", "b"]; "a" is in both payloads, so the block
	// must be selected no matter what order the payload arrives in.
	meta := varcharBlockMeta(t, "a", "b")

	for _, test := range []struct {
		name   string
		values []string
	}{
		{"sorted", []string{"a", "c"}},
		{"unsorted", []string{"c", "a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := varcharFilterPayload(t, mp, test.values...)
			expr := prefixInExpr(data, len(test.values))

			_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.True(t, canCompile)
			require.NotNil(t, blockFilter)

			stop, selected, err := blockFilter(0, meta, nil)
			require.NoError(t, err)
			require.False(t, stop, "block scan must not stop early: 'a' is in the payload")
			require.True(t, selected, "block covering [a,b] must be selected: 'a' is in the payload")
		})
	}
}

// UnmarshalBinary aliases the payload, which belongs to the plan expression and
// may be reused across blocks or shipped to another CN. Sorting must not write
// through to it.
func TestUnmarshalSortedFilterVectorLeavesPayloadIntact(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := varcharFilterPayload(t, mp, "c", "a", "b")
	original := bytes.Clone(data)

	vec, ok := unmarshalSortedFilterVector(data)
	require.True(t, ok)
	require.Equal(t, original, data, "payload was mutated in place")

	col, area := vector.MustVarlenaRawData(vec)
	require.Equal(t, 3, len(col))
	for i := 1; i < len(col); i++ {
		require.Negative(t, bytes.Compare(
			col[i-1].GetByteSlice(area), col[i].GetByteSlice(area)))
	}

	// An already-sorted payload is returned as-is, with no defensive copy.
	sortedData := varcharFilterPayload(t, mp, "a", "b", "c")
	sortedVec := vector.NewVec(types.T_varchar.ToType())
	defer sortedVec.Free(mp)
	require.NoError(t, sortedVec.UnmarshalBinary(sortedData))
	sortedVec.SetSorted(true)
	marked, err := sortedVec.MarshalBinary()
	require.NoError(t, err)

	got, ok := unmarshalSortedFilterVector(marked)
	require.True(t, ok)
	require.True(t, got.GetSorted())
	require.Equal(t, 3, got.Length())
}

func inExpr(fnName string, data []byte, rows int) *plan.Expr {
	colTyp := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: fnName},
			Args: []*plan.Expr{
				{Typ: colTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}},
				{Typ: colTyp, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
					Len: int32(rows), Data: data,
				}}},
			},
		}},
	}
}

// An undecodable payload must make the filter refuse to compile rather than
// prune with garbage.
func TestCompileFilterExprRejectsCorruptInPayload(t *testing.T) {
	tableDef := varcharPKTableDef()
	for _, fnName := range []string{"in", "prefix_in"} {
		t.Run(fnName, func(t *testing.T) {
			expr := inExpr(fnName, []byte("not a marshalled vector"), 1)
			_, _, _, _, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.False(t, canCompile)
		})
	}
}
