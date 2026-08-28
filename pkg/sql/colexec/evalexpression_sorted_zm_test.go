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

package colexec

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func zmVarcharPayload(t *testing.T, mp *mpool.MPool, values ...string) []byte {
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

func zmVarcharBlockMeta(t *testing.T, values ...string) objectio.BlockObject {
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

func zmInExpr(fnName string, data []byte, rows int) *plan.Expr {
	colTyp := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	return &plan.Expr{
		Typ:   plan.Type{Id: int32(types.T_bool)},
		AuxId: 0,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: fnName},
			Args: []*plan.Expr{
				{Typ: colTyp, AuxId: 1, Expr: &plan.Expr_Col{
					Col: &plan.ColRef{Name: "k", ColPos: 0}}},
				{Typ: colTyp, AuxId: 2, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
					Len: int32(rows), Data: data,
				}}},
			},
		}},
	}
}

func evalZoneMap(t *testing.T, proc *process.Process, expr *plan.Expr, meta objectio.BlockObject) bool {
	t.Helper()
	zms := make([]objectio.ZoneMap, 3)
	vecs := make([]*vector.Vector, 3)
	t.Cleanup(func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	})
	return EvaluateFilterByZoneMap(
		context.Background(), proc, expr, meta, map[int]int{0: 0}, zms, vecs)
}

// Both IN and prefix_in reach ZM through a binary search, so neither may depend
// on the order the payload happens to arrive in.
func TestEvaluateFilterByZoneMapInIgnoresPayloadOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcess(t)

	// The block covers ["b","d"]; "c" is in every payload, so it must be selected.
	meta := zmVarcharBlockMeta(t, "b", "d")
	for _, test := range []struct {
		name   string
		values []string
	}{
		{"sorted", []string{"a", "c"}},
		{"unsorted", []string{"c", "a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr := zmInExpr("in", zmVarcharPayload(t, mp, test.values...), len(test.values))
			require.True(t, evalZoneMap(t, proc, expr, meta),
				"block covering [b,d] must be selected: 'c' is in the payload")
		})
	}
}

// A payload that cannot be decoded must reset the zone map rather than be used.
func TestEvaluateFilterByZoneMapRejectsCorruptPayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	meta := zmVarcharBlockMeta(t, "a", "b")

	for _, fnName := range []string{"in", "prefix_in"} {
		t.Run(fnName, func(t *testing.T) {
			expr := zmInExpr(fnName, []byte("not a marshalled vector"), 1)
			// Undecodable means "cannot prune with this", so the block is kept.
			require.True(t, evalZoneMap(t, proc, expr, meta))
		})
	}
}

// ZM.PrefixIn binary-searches the payload, so an unsorted list makes zone-map
// pruning skip a block that holds matching rows. This is the live pruning path:
// BlockFilters reach it through EvaluateFilterByZoneMap.
func TestEvaluateFilterByZoneMapPrefixInIgnoresPayloadOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcess(t)

	colTyp := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	// The block covers keys in ["a","b"], and "a" is in every payload below, so
	// the block must be selected regardless of the order the payload arrives in.
	meta := zmVarcharBlockMeta(t, "a", "b")

	for _, test := range []struct {
		name   string
		values []string
	}{
		{"sorted", []string{"a", "c"}},
		{"unsorted", []string{"c", "a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr := &plan.Expr{
				Typ:   plan.Type{Id: int32(types.T_bool)},
				AuxId: 0,
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "prefix_in"},
					Args: []*plan.Expr{
						{Typ: colTyp, AuxId: 1, Expr: &plan.Expr_Col{
							Col: &plan.ColRef{Name: "k", ColPos: 0}}},
						{Typ: colTyp, AuxId: 2, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
							Len:  int32(len(test.values)),
							Data: zmVarcharPayload(t, mp, test.values...),
						}}},
					},
				}},
			}

			zms := make([]objectio.ZoneMap, 3)
			vecs := make([]*vector.Vector, 3)
			defer func() {
				for i := range vecs {
					if vecs[i] != nil {
						vecs[i].Free(proc.Mp())
					}
				}
			}()

			selected := EvaluateFilterByZoneMap(
				context.Background(), proc, expr, meta, map[int]int{0: 0}, zms, vecs)
			require.True(t, selected,
				"block covering [a,b] must be selected: 'a' is in the payload")
		})
	}
}

func TestZoneMapInVectorSortsWithoutTouchingThePayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := zmVarcharPayload(t, mp, "c", "a", "b")
	original := bytes.Clone(data)

	vec, ok := zoneMapInVector(data)
	require.True(t, ok)
	require.Equal(t, original, data, "payload was mutated in place")

	col, area := vector.MustVarlenaRawData(vec)
	require.Equal(t, 3, len(col))
	for i := 1; i < len(col); i++ {
		require.Negative(t, bytes.Compare(
			col[i-1].GetByteSlice(area), col[i].GetByteSlice(area)))
	}

	_, ok = zoneMapInVector([]byte("not a marshalled vector"))
	require.False(t, ok, "a corrupt payload must be reported, not silently used")
}
