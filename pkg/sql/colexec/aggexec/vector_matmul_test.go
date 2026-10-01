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

package aggexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const vmDim = 4

func vmVecType() types.Type { return types.New(types.T_array_float8, vmDim, 0) }

func vmConfig(limit int, queries [][]float32) []byte {
	q, _ := json.Marshal(queries)
	return EncodeVectorMatmulConfig(fmt.Sprintf(`{"limit":%d}`, limit), string(q))
}

func vmExec(t *testing.T, mp *mpool.MPool, idType types.Type, groups int, cfg []byte) *vectorMatmulExec {
	t.Helper()
	exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{idType, vmVecType()})
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(groups))
	require.NoError(t, exec.SetExtraInformation(cfg, 0))
	return exec.(*vectorMatmulExec)
}

// vmVectors builds int64 ids and vecf8 cells; a nil row is NULL.
func vmVectors(t *testing.T, mp *mpool.MPool, ids []int64, rows [][]float32) []*vector.Vector {
	t.Helper()
	idv := vector.NewVec(types.T_int64.ToType())
	vv := vector.NewVec(vmVecType())
	for i, r := range rows {
		require.NoError(t, vector.AppendFixed(idv, ids[i], false, mp))
		if r == nil {
			require.NoError(t, vector.AppendBytes(vv, nil, true, mp))
			continue
		}
		cell, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, r)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vv, cell, false, mp))
	}
	return []*vector.Vector{idv, vv}
}

func vmFree(mp *mpool.MPool, vecs []*vector.Vector) {
	for _, v := range vecs {
		v.Free(mp)
	}
}

func vmFlush(t *testing.T, mp *mpool.MPool, exec AggFuncExec) []string {
	t.Helper()
	ret, err := exec.Flush()
	require.NoError(t, err)
	var out []string
	for _, v := range ret {
		for i := 0; i < v.Length(); i++ {
			var c bytes.Buffer
			require.NoError(t, json.Compact(&c, []byte(types.DecodeJson(v.GetBytesAt(i)).String())))
			out = append(out, c.String())
		}
		v.Free(mp)
	}
	return out
}

func TestVectorMatmulTopK(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(2, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}))
	defer exec.Free()
	vecs := vmVectors(t, mp,
		[]int64{10, 2, 30, 4, 5},
		[][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {1, 1, 0, 0}, {-1, 0, 0, 0}, nil})
	defer vmFree(mp, vecs)
	require.NoError(t, exec.BulkFill(0, vecs))
	// query 0: ids 10 and 30 tie at 1, ordered by id text ("10" < "30")
	// query 1: ids 2 and 30 tie at 1 ("2" < "30")
	require.Equal(t, []string{`[[["10",1],["30",1]],[["2",1],["30",1]]]`}, vmFlush(t, mp, exec))
}

func TestVectorMatmulEmptyGroupAndGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := vmExec(t, mp, types.T_int64.ToType(), 3, vmConfig(1, [][]float32{{1, 0, 0, 0}, {0, 0, 1, 0}}))
	defer exec.Free()
	vecs := vmVectors(t, mp, []int64{1, 2, 3}, [][]float32{{1, 0, 0, 0}, {0, 0, 1, 0}, {2, 0, 0, 0}})
	defer vmFree(mp, vecs)
	// groups are 1-based; group 2 receives no row; row 1 is not matched
	require.NoError(t, exec.BatchFill(0, []uint64{1, GroupNotMatched, 3}, vecs))
	require.NoError(t, exec.Fill(0, 1, vecs))
	require.Equal(t, []string{
		`[[["1",1]],[["2",1]]]`,
		`[[],[]]`,
		`[[["3",2]],[["3",0]]]`,
	}, vmFlush(t, mp, exec))
}

// vmReference returns the expected JSON from a brute-force sort.
func vmReference(ids []int64, rows [][]float32, queries [][]float32, k int) string {
	type hit struct {
		id    string
		score float64
	}
	var out []string
	for _, q := range queries {
		qc, _ := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, q)
		qv, _ := types.BlockScaledToFloat32(qc)
		var hits []hit
		for i, r := range rows {
			if r == nil {
				continue
			}
			rc, _ := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, r)
			rv, _ := types.BlockScaledToFloat32(rc)
			var dot float64
			for d := range rv {
				dot += float64(rv[d]) * float64(qv[d])
			}
			hits = append(hits, hit{strconv.FormatInt(ids[i], 10), float64(float32(dot))})
		}
		sort.Slice(hits, func(a, b int) bool {
			if hits[a].score != hits[b].score {
				return hits[a].score > hits[b].score
			}
			return hits[a].id < hits[b].id
		})
		hits = hits[:min(k, len(hits))]
		parts := make([]string, len(hits))
		for i, h := range hits {
			parts[i] = `["` + h.id + `",` + strconv.FormatFloat(h.score, 'g', -1, 32) + `]`
		}
		out = append(out, "["+strings.Join(parts, ",")+"]")
	}
	return "[" + strings.Join(out, ",") + "]"
}

func vmRandom(r *rand.Rand, n int) ([]int64, [][]float32) {
	ids := make([]int64, n)
	rows := make([][]float32, n)
	for i := range rows {
		ids[i] = int64(r.Intn(1000))
		if r.Intn(10) == 0 {
			continue
		}
		rows[i] = make([]float32, vmDim)
		for d := range rows[i] {
			rows[i][d] = float32(r.Intn(5) - 2)
		}
	}
	return ids, rows
}

func TestVectorMatmulMergeMatchesReference(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	r := rand.New(rand.NewSource(3))
	queries := [][]float32{{1, 2, 0, -1}, {0, 0, 1, 1}, {-1, 1, -1, 1}}
	const k = 5
	ids, rows := vmRandom(r, 300)
	want := vmReference(ids, rows, queries, k)

	whole := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(k, queries))
	defer whole.Free()
	all := vmVectors(t, mp, ids, rows)
	defer vmFree(mp, all)
	require.NoError(t, whole.BulkFill(0, all))
	require.Equal(t, []string{want}, vmFlush(t, mp, whole))

	// three partials merged in both orders
	cuts := []int{0, 70, 210, 300}
	parts := make([]*vectorMatmulExec, 3)
	for p := range parts {
		parts[p] = vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(k, queries))
		defer parts[p].Free()
		vecs := vmVectors(t, mp, ids[cuts[p]:cuts[p+1]], rows[cuts[p]:cuts[p+1]])
		require.NoError(t, parts[p].BulkFill(0, vecs))
		vmFree(mp, vecs)
	}
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}} {
		target := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(k, queries))
		for _, p := range order {
			require.NoError(t, target.BatchMerge(parts[p], 0, []uint64{1}))
		}
		require.Equal(t, []string{want}, vmFlush(t, mp, target))
		target.Free()
	}
}

func TestVectorMatmulIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	cfg := vmConfig(3, [][]float32{{1, 1, 0, 0}})
	src := vmExec(t, mp, types.T_int64.ToType(), 2, cfg)
	defer src.Free()
	vecs := vmVectors(t, mp, []int64{7, 8, 9}, [][]float32{{1, 0, 0, 0}, {1, 1, 0, 0}, {0, 0, 1, 0}})
	defer vmFree(mp, vecs)
	require.NoError(t, src.BulkFill(0, vecs))
	want := vmFlush(t, mp, src)

	var buf bytes.Buffer
	require.NoError(t, src.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &buf))
	dst, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vmVecType()})
	require.NoError(t, err)
	defer dst.Free()
	require.NoError(t, dst.SetExtraInformation(cfg, 0))
	require.NoError(t, dst.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
	require.Equal(t, want, vmFlush(t, mp, dst))
	require.Equal(t, []string{`[[["8",2],["7",1],["9",0]]]`, `[[]]`}, want)
}

func TestVectorMatmulAccountedFill(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
	})
	require.NoError(t, err)

	exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_varchar.ToType(), vmVecType()})
	require.NoError(t, err)
	require.NoError(t, exec.SetExtraInformation(vmConfig(2, [][]float32{{1, 0, 0, 0}}), 0))
	require.NoError(t, exec.(AllocationAccountOwner).SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1))

	// long, churning string ids: the arena compacts in place instead of growing
	r := rand.New(rand.NewSource(5))
	for batch := 0; batch < 50; batch++ {
		idv := vector.NewVec(types.T_varchar.ToType())
		vv := vector.NewVec(vmVecType())
		groups := make([]uint64, 64)
		for i := range groups {
			groups[i] = 1
			id := strings.Repeat(strconv.Itoa(batch*64+i), 20)
			require.NoError(t, vector.AppendBytes(idv, []byte(id), false, mp))
			cell, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, []float32{float32(r.Intn(100)), 0, 0, 0})
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(vv, cell, false, mp))
		}
		vecs := []*vector.Vector{idv, vv}
		require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, vecs))
		require.NoError(t, exec.BatchFill(0, groups, vecs))
		vmFree(mp, vecs)
	}
	s := exec.(*vectorMatmulExec).state[0].mobs[0].(*vectorMatmulState)
	require.LessOrEqual(t, len(s.arena), 8<<10)
	out := vmFlush(t, mp, exec)
	require.Len(t, out, 1)
	var got [][][]any
	require.NoError(t, json.Unmarshal([]byte(out[0]), &got))
	require.Len(t, got[0], 2)
	exec.Free()
}

func TestVectorMatmulIDText(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	u, err := types.ParseUuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	cases := []struct {
		typ  types.Type
		add  func(v *vector.Vector) error
		want string
	}{
		{types.T_int8.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, int8(-8), false, mp) }, "-8"},
		{types.T_int16.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, int16(-16), false, mp) }, "-16"},
		{types.T_int32.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, int32(-32), false, mp) }, "-32"},
		{types.T_int64.ToType(), func(v *vector.Vector) error {
			return vector.AppendFixed(v, int64(9223372036854775807), false, mp)
		}, "9223372036854775807"},
		{types.T_uint8.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, uint8(8), false, mp) }, "8"},
		{types.T_uint16.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, uint16(16), false, mp) }, "16"},
		{types.T_uint32.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, uint32(32), false, mp) }, "32"},
		{types.T_uint64.ToType(), func(v *vector.Vector) error {
			return vector.AppendFixed(v, uint64(18446744073709551615), false, mp)
		}, "18446744073709551615"},
		{types.T_uuid.ToType(), func(v *vector.Vector) error { return vector.AppendFixed(v, u, false, mp) }, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{types.T_varchar.ToType(), func(v *vector.Vector) error { return vector.AppendBytes(v, []byte(`a"b`), false, mp) }, `a"b`},
	}
	for _, c := range cases {
		require.True(t, VectorMatmulIDSupported(c.typ.Oid))
		v := vector.NewVec(c.typ)
		require.NoError(t, c.add(v))
		require.Equal(t, c.want, string(appendVectorMatmulID(nil, v, 0)), c.typ.String())
		require.GreaterOrEqual(t, vectorMatmulIDLenBound(v, 0), len(c.want), c.typ.String())
		v.Free(mp)
	}
	require.False(t, VectorMatmulIDSupported(types.T_float64))

	// a quote in a string id is JSON-escaped
	exec := vmExec(t, mp, types.T_varchar.ToType(), 1, vmConfig(1, [][]float32{{1, 0, 0, 0}}))
	defer exec.Free()
	idv := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(idv, []byte(`a"b`), false, mp))
	vv := vector.NewVec(vmVecType())
	cell, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, []float32{1, 0, 0, 0})
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(vv, cell, false, mp))
	defer vmFree(mp, []*vector.Vector{idv, vv})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{idv, vv}))
	require.Equal(t, []string{`[[["a\"b",1]]]`}, vmFlush(t, mp, exec))
}

func TestVectorMatmulConfigErrors(t *testing.T) {
	vt := vmVecType()
	for _, tc := range []struct {
		params, queries, want string
	}{
		{`{}`, `[[1,0,0,0]]`, `requires "limit"`},
		{`{"limit":0}`, `[[1,0,0,0]]`, `out of range`},
		{`{"limit":2,"bogus":1}`, `[[1,0,0,0]]`, `invalid params`},
		{`{"limit":2,"mode":"fast"}`, `[[1,0,0,0]]`, `invalid mode`},
		{`{"limit":2,"mode":"gpu"}`, `[[1,0,0,0]]`, `gpu mode`},
		{`{"limit":2,"tile_bytes":-1}`, `[[1,0,0,0]]`, `tile_bytes`},
		{`not json`, `[[1,0,0,0]]`, `invalid params`},
		{`{"limit":2}`, `[]`, `query count`},
		{`{"limit":2}`, `[1,2]`, `JSON array of vectors`},
		{`{"limit":2}`, `[[1,0,0]]`, `different dimensions`},
		{`{"limit":2}`, `[[1,0,0,"x"]]`, `JSON array of vectors`},
	} {
		_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(tc.params, tc.queries), vt)
		require.ErrorContains(t, err, tc.want, tc.params+" "+tc.queries)
	}
	for _, mode := range []string{`"auto"`, `"cpu"`} {
		_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(`{"limit":1,"mode":`+mode+`,"tile_bytes":1024}`, `[[1,0,0,0]]`), vt)
		require.NoError(t, err)
	}
	_, _, err := decodeVectorMatmulConfig([]byte{1, 2})
	require.Error(t, err)

	mp := mpool.MustNewZero()
	exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vt})
	require.NoError(t, err)
	require.Error(t, exec.SetExtraInformation("not bytes", 0))
	_, err = exec.Flush()
	require.ErrorContains(t, err, "configuration is not set")
	exec.Free()
	_, err = makeVectorMatmul(mp, AggIdOfVectorMatmul, true, []types.Type{types.T_int64.ToType(), vt})
	require.Error(t, err)
	_, err = makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_float64.ToType(), vt})
	require.Error(t, err)
}

func vmAllocation(t *testing.T) *AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
	})
	require.NoError(t, err)
	return allocation
}

func TestVectorMatmulAccountedMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	cfg := vmConfig(2, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}})
	allocation := vmAllocation(t)
	mk := func() *vectorMatmulExec {
		exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vmVecType()})
		require.NoError(t, err)
		require.NoError(t, exec.SetExtraInformation(cfg, 0))
		require.NoError(t, exec.(AllocationAccountOwner).SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(2))
		return exec.(*vectorMatmulExec)
	}
	src, dst := mk(), mk()
	defer src.Free()
	defer dst.Free()
	vecs := vmVectors(t, mp, []int64{1, 2, 3}, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {1, 1, 0, 0}})
	defer vmFree(mp, vecs)
	groups := []uint64{1, 1, 1}
	require.NoError(t, src.PreflightBatchFill(0, groups, vecs))
	require.NoError(t, src.BatchFill(0, groups, vecs))
	require.Positive(t, src.Size())

	// source group 0 has state, source group 1 is empty
	merge := []uint64{2, 1}
	require.NoError(t, dst.PreflightBatchMerge(src, 0, merge))
	require.NoError(t, dst.BatchMerge(src, 0, merge))
	require.Equal(t, []string{`[[],[]]`, `[[["1",1],["3",1]],[["2",1],["3",1]]]`}, vmFlush(t, mp, dst))

	require.ErrorIs(t, dst.PreflightBatchMerge(nil, 0, merge), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, dst.PreflightBatchMerge(src, 5, merge), mpool.ErrAllocationAccountInvalid)
}

func TestVectorMatmulMergeRejectsDifferentConfig(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	a := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(2, [][]float32{{1, 0, 0, 0}}))
	b := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(2, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}))
	defer a.Free()
	defer b.Free()
	vecs := vmVectors(t, mp, []int64{1}, [][]float32{{1, 0, 0, 0}})
	defer vmFree(mp, vecs)
	require.NoError(t, b.BulkFill(0, vecs))
	require.ErrorContains(t, a.Merge(b, 0, 0), "different query configurations")

	other, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_varchar.ToType(), vmVecType()})
	require.NoError(t, err)
	defer other.Free()
	require.ErrorIs(t, a.Merge(other, 0, 0), mpool.ErrAllocationAccountMismatch)
}

func TestVectorMatmulStateCodec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	s, err := newVectorMatmulState(mp, nil, 2, 3)
	require.NoError(t, err)
	defer s.Free()
	require.NoError(t, s.offer([]float64{1, 5}, []byte("a")))
	require.NoError(t, s.offer([]float64{2, 4}, []byte("bb")))
	data, err := s.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, data, s.MarshaledSize())

	// a state built for another configuration takes the encoded q and k
	r, err := newVectorMatmulState(mp, nil, 1, 1)
	require.NoError(t, err)
	defer r.Free()
	require.NoError(t, r.UnmarshalBinary(data))
	got, err := r.appendJSON(nil)
	require.NoError(t, err)
	want, err := s.appendJSON(nil)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
	require.Equal(t, `[[["bb",2],["a",1]],[["a",5],["bb",4]]]`, string(got))

	for _, bad := range [][]byte{
		{},
		{2, 0, 0, 0, 0, 0, 0, 0, 0},
		append([]byte{1}, binary32(1<<20, 1<<20)...),
		append(append([]byte{1}, binary32(2, 3)...), 9, 0, 0, 0),
		data[:len(data)-1],
	} {
		m, err := newVectorMatmulState(mp, nil, 2, 3)
		require.NoError(t, err)
		require.Error(t, m.UnmarshalBinary(bad), "%v", bad)
		m.Free()
	}
}

func binary32(a, b uint32) []byte {
	return []byte{byte(a), byte(a >> 8), byte(a >> 16), byte(a >> 24), byte(b), byte(b >> 8), byte(b >> 16), byte(b >> 24)}
}

func TestVectorMatmulNullAndConstInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := vmExec(t, mp, types.T_int64.ToType(), 1, vmConfig(3, [][]float32{{1, 0, 0, 0}}))
	defer exec.Free()
	idv := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(idv, int64(1), true, mp))
	require.NoError(t, vector.AppendFixed(idv, int64(2), false, mp))
	cell, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, []float32{2, 0, 0, 0})
	require.NoError(t, err)
	constVec, err := vector.NewConstBytes(vmVecType(), cell, 2, mp)
	require.NoError(t, err)
	defer vmFree(mp, []*vector.Vector{idv, constVec})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{idv, constVec}))
	require.Equal(t, []string{`[[["2",2]]]`}, vmFlush(t, mp, exec))

	// a malformed cell is an error
	bad := vector.NewVec(vmVecType())
	require.NoError(t, vector.AppendBytes(bad, []byte{0x7f, 1, 0, 0, 4, 0, 0, 0}, false, mp))
	defer bad.Free(mp)
	one := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(one, int64(9), false, mp))
	defer one.Free(mp)
	require.Error(t, exec.Fill(0, 0, []*vector.Vector{one, bad}))
}
