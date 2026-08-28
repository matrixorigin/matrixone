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

// A NULL slot carries an empty payload, which PrefixCompare treats as a prefix
// of everything, so a leading NULL matches rather than prunes. What actually
// breaks the search is physical order, so the rule is about order and each
// consumer's search strategy -- not about NULL by itself.
func TestEvaluateFilterByZoneMapNullAndOrderRules(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcess(t)
	meta := zmVarcharBlockMeta(t, "a", "b")

	payload := func(t *testing.T, items ...string) []byte {
		t.Helper()
		vec := vector.NewVec(types.T_varchar.ToType())
		defer vec.Free(mp)
		for _, it := range items {
			if it == "NULL" {
				require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			} else {
				require.NoError(t, vector.AppendBytes(vec, []byte(it), false, mp))
			}
		}
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		return data
	}

	// "a" is in every payload, so the block covering ["a","b"] must survive all
	// of them however the needles are ordered or interleaved with NULLs.
	for _, test := range []struct {
		name  string
		fn    string
		items []string
	}{
		{"prefix_in sorted", "prefix_in", []string{"a", "c"}},
		{"prefix_in unsorted", "prefix_in", []string{"c", "a"}},
		{"prefix_in null first", "prefix_in", []string{"NULL", "c", "a"}},
		{"prefix_in null interleaved", "prefix_in", []string{"c", "NULL", "a"}},
		{"in sorted", "in", []string{"a", "c"}},
		{"in unsorted", "in", []string{"c", "a"}},
		{"in null interleaved", "in", []string{"c", "NULL", "a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr := zmInExpr(test.fn, payload(t, test.items...), len(test.items))
			require.True(t, evalZoneMap(t, proc, expr, meta),
				"block covering [a,b] must be kept: 'a' is in the payload")
		})
	}
}

// A nullable IN list keeps its pruning: AnyIn scans those linearly, so order
// never mattered for it and failing open would lose valid pruning.
func TestZoneMapInVectorKeepsNullableInPruning(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendBytes(vec, []byte("c"), false, mp))
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, mp))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)

	_, ok := zoneMapInVector(data, false)
	require.True(t, ok, "nullable IN uses a linear scan and must still prune")

	_, ok = zoneMapInVector(data, true)
	require.False(t, ok, "prefix_in binary-searches and must refuse this order")
}

// The evaluator runs once per object and again per block, and frees its vector
// cache each call, so a payload that already holds the invariant must decode
// without copying. Proven by aliasing: mutating the source bytes is visible
// through the decoded vector.
func TestZoneMapInVectorDecodesSortedPayloadZeroCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_varchar.ToType())
	defer src.Free(mp)
	for _, v := range []string{"aaa", "bbb", "ccc"} {
		require.NoError(t, vector.AppendBytes(src, []byte(v), false, mp))
	}
	src.SetSorted(true)
	data, err := src.MarshalBinary()
	require.NoError(t, err)

	vec, ok := zoneMapInVector(data, true)
	require.True(t, ok)
	require.Equal(t, 3, vec.Length())

	col, area := vector.MustVarlenaRawData(vec)
	before := string(col[0].GetByteSlice(area))
	require.Equal(t, "aaa", before)

	// Flip a byte of the first value inside the source buffer. A zero-copy decode
	// observes it; a defensive clone would not.
	idx := bytes.Index(data, []byte("aaa"))
	require.GreaterOrEqual(t, idx, 0)
	data[idx] = 'z'

	col, area = vector.MustVarlenaRawData(vec)
	require.Equal(t, "zaa", string(col[0].GetByteSlice(area)),
		"sorted payload was copied instead of decoded in place")
}

// An unsorted payload cannot be searched soundly, and normalizing it here would
// cost a copy and a sort for every block. It must fail open instead.
func TestZoneMapInVectorFailsOpenForUnsortedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	_, ok := zoneMapInVector(zmVarcharPayload(t, mp, "c", "a"), true)
	require.False(t, ok, "unsorted payload must not be used for pruning")

	// Sorted but unflagged still prunes: the O(n) check confirms the order.
	unflagged := vector.NewVec(types.T_varchar.ToType())
	defer unflagged.Free(mp)
	for _, v := range []string{"a", "b", "c"} {
		require.NoError(t, vector.AppendBytes(unflagged, []byte(v), false, mp))
	}
	require.False(t, unflagged.GetSorted())
	data, err := unflagged.MarshalBinary()
	require.NoError(t, err)
	vec, ok := zoneMapInVector(data, true)
	require.True(t, ok, "a de-facto sorted payload should keep its pruning")
	require.Equal(t, 3, vec.Length())

	_, ok = zoneMapInVector([]byte("not a marshalled vector"), true)
	require.False(t, ok, "a corrupt payload must be reported, not silently used")
}

// ZM.AnyIn binary-searches fixed-width values, so an unflagged payload whose
// order cannot be verified here must not prune. Needles [30,10] against a block
// zonemap [5,15] make the search probe 30, answer false, and drop a block holding
// the matching needle 10.
//
// Reachable beyond constant folding: readutil.ConstructInExpr serialises a
// caller-supplied vector verbatim (transfer and snapshot filtering), so the
// serialised sorted flag is not a universal invariant.
func TestEvaluateFilterByZoneMapKeepsBlockForUnflaggedFixedWidthIn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcess(t)

	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	for _, n := range []int64{30, 10} {
		require.NoError(t, vector.AppendFixed(vec, n, false, mp))
	}
	require.False(t, vec.GetSorted(), "constant folding marshals such a payload without sorting or flagging")
	data, err := vec.MarshalBinary()
	require.NoError(t, err)

	lo, hi := int64(5), int64(15)
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zm := index.NewZM(types.T_int64, 0)
	index.UpdateZM(zm, types.EncodeInt64(&lo))
	index.UpdateZM(zm, types.EncodeInt64(&hi))
	meta.MustGetColumn(0).SetZoneMap(zm)

	require.True(t, evalZoneMap(t, proc, zmInExpr("in", data, 2), meta),
		"block [5,15] must be kept: needle 10 is inside it")

	// A flagged, genuinely ordered payload still prunes normally.
	sorted := vector.NewVec(types.T_int64.ToType())
	defer sorted.Free(mp)
	for _, n := range []int64{10, 30} {
		require.NoError(t, vector.AppendFixed(sorted, n, false, mp))
	}
	sorted.SetSorted(true)
	sdata, err := sorted.MarshalBinary()
	require.NoError(t, err)
	got, ok := zoneMapInVector(sdata, false)
	require.True(t, ok, "a flagged ordered payload keeps its pruning")
	require.Equal(t, 2, got.Length())
}

// A constant vector represents one repeated value, so its order is not in
// question and it is passed through without inspection.
func TestZoneMapInVectorAcceptsConstPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 5, mp)
	require.NoError(t, err)
	defer vec.Free(mp)
	require.True(t, vec.IsConst())
	data, err := vec.MarshalBinary()
	require.NoError(t, err)

	for _, prefixSearch := range []bool{false, true} {
		got, ok := zoneMapInVector(data, prefixSearch)
		require.True(t, ok, "const payload is usable (prefixSearch=%v)", prefixSearch)
		require.True(t, got.IsConst())
	}
}
