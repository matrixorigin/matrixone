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
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const vmDim = 4

func vmVecType() types.Type { return types.New(types.T_array_float8, vmDim, 0) }

func vmConfig(topk int, queries [][]float32) []byte {
	q, _ := json.Marshal(queries)
	return EncodeVectorMatmulConfig(int64(topk), string(q), "", false)
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
	out := make([]string, 0, len(queries))
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
		topk             int64
		queries, options string
		want             string
	}{
		{0, `[[1,0,0,0]]`, ``, `topk 0 out of range`},
		{vectorMatmulMaxTopK + 1, `[[1,0,0,0]]`, ``, `out of range`},
		{2, `[]`, ``, `query count`},
		{2, `[1,2]`, ``, `JSON array of vectors`},
		{2, `[[1,0,0]]`, ``, `different dimensions`},
		{2, `[[1,0,0,"x"]]`, ``, `JSON array of vectors`},
	} {
		_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(tc.topk, tc.queries, tc.options, false), vt)
		require.ErrorContains(t, err, tc.want, "%d %s %s", tc.topk, tc.queries, tc.options)
	}
	many := "[" + strings.TrimSuffix(strings.Repeat(`[1,0,0,0],`, vectorMatmulMaxEntries/vectorMatmulMaxTopK+1), ",") + "]"
	_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(vectorMatmulMaxTopK, many, "", false), vt)
	require.ErrorContains(t, err, "queries x topk exceeds")
	// the options argument is not interpreted: any text is accepted
	for _, options := range []string{``, `{"mode":"auto"}`, `{"mode":"gpu","tile_bytes":-1}`, `{"bogus":1}`, `not json`} {
		_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(1, `[[1,0,0,0]]`, options, false), vt)
		require.NoError(t, err, options)
	}
	badFlag := EncodeVectorMatmulConfig(1, `[[1]]`, "", true)
	badFlag[len(badFlag)-1] = 2
	for _, bad := range [][]byte{{1, 2}, EncodeVectorMatmulConfig(1, `[[1]]`, "", false)[:12], append(EncodeVectorMatmulConfig(1, `[[1]]`, "", false), 0), badFlag} {
		_, _, _, _, err := decodeVectorMatmulConfig(bad)
		require.Error(t, err)
	}
	for _, gpu := range []bool{false, true} {
		topk, queries, options, got, err := decodeVectorMatmulConfig(EncodeVectorMatmulConfig(7, `[[1]]`, `{}`, gpu))
		require.NoError(t, err)
		require.Equal(t, []any{int64(7), `[[1]]`, `{}`, gpu}, []any{topk, queries, options, got})
	}

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

// An overflowing row (its float32 lanes reach +Inf and -Inf, so the dot is NaN) ranks
// last; it is an error only when it reaches the result.
func TestVectorMatmulOverflowRanksLast(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	const m = 3e38
	big := make([]float32, 32)
	query := make([]float32, 32)
	for i := range big {
		big[i], query[i] = m, m
		if i%2 == 1 {
			query[i] = -m
		}
	}
	vt := types.New(types.T_array_float8, 32, 0)
	mk := func(topk int) *vectorMatmulExec {
		q, _ := json.Marshal([][]float32{query})
		exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vt})
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation(EncodeVectorMatmulConfig(int64(topk), string(q), "", false), 0))
		return exec.(*vectorMatmulExec)
	}
	idv := vector.NewVec(types.T_int64.ToType())
	vv := vector.NewVec(vt)
	small := make([]float32, 32)
	small[0] = 1
	for i, row := range [][]float32{big, small, small} {
		require.NoError(t, vector.AppendFixed(idv, int64(i), false, mp))
		cell, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, row)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vv, cell, false, mp))
	}
	defer vmFree(mp, []*vector.Vector{idv, vv})

	top2 := mk(2)
	defer top2.Free()
	require.NoError(t, top2.BulkFill(0, []*vector.Vector{idv, vv}))
	out := vmFlush(t, mp, top2)
	require.Len(t, out, 1)
	var got [][][]any
	require.NoError(t, json.Unmarshal([]byte(out[0]), &got))
	require.Equal(t, "1", got[0][0][0])
	require.Equal(t, "2", got[0][1][0])

	top3 := mk(3)
	defer top3.Free()
	require.NoError(t, top3.BulkFill(0, []*vector.Vector{idv, vv}))
	_, err := top3.Flush()
	require.ErrorContains(t, err, "overflows the float32 domain")
}

// Merging must keep each hit's id text when appending an id compacts the target arena:
// the target carries garbage from evicted ids, and source ids are shared across queries.
func TestVectorMatmulStateMergeAcrossCompaction(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	r := rand.New(rand.NewSource(11))
	const q, k = 3, 4
	type hit struct {
		id    string
		score float64
	}
	for iter := 0; iter < 200; iter++ {
		var all []hit
		perQuery := make([][]hit, q)
		mkState := func(rows int) *vectorMatmulState {
			s, err := newVectorMatmulState(mp, nil, q, k)
			require.NoError(t, err)
			for i := 0; i < rows; i++ {
				id := strings.Repeat(string(rune('a'+r.Intn(26))), 1+r.Intn(40)) + strconv.Itoa(len(all))
				scores := make([]float64, q)
				for j := range scores {
					scores[j] = float64(r.Intn(1000))
					perQuery[j] = append(perQuery[j], hit{id, scores[j]})
				}
				all = append(all, hit{id, 0})
				require.NoError(t, s.offer(scores, []byte(id)))
			}
			return s
		}
		target := mkState(30)
		for src := 0; src < 4; src++ {
			s := mkState(10)
			require.NoError(t, target.Merge(s))
			s.Free()
		}
		for j := 0; j < q; j++ {
			want := perQuery[j]
			sort.Slice(want, func(a, b int) bool {
				if want[a].score != want[b].score {
					return want[a].score > want[b].score
				}
				return want[a].id < want[b].id
			})
			got := target.sorted(j)
			require.Len(t, got, k)
			for i, e := range got {
				require.Equal(t, want[i].id, string(target.id(e)), "iter %d query %d rank %d", iter, j, i)
				require.Equal(t, want[i].score, e.score)
			}
		}
		target.Free()
	}
}

// vmPlainCase holds a plain vector type and how a float32 maps to it.
type vmPlainCase struct {
	oid    types.T
	toCell func(v []float32) []byte
	value  func(v float32) float64
}

func vmPlainCases() []vmPlainCase {
	return []vmPlainCase{
		{types.T_array_float32, func(v []float32) []byte { return types.ArrayToBytes(v) }, func(v float32) float64 { return float64(v) }},
		{types.T_array_float16, func(v []float32) []byte {
			out := make([]types.Float16, len(v))
			for i, x := range v {
				out[i] = types.Float16FromFloat32(x)
			}
			return types.ArrayToBytes(out)
		}, func(v float32) float64 { return float64(types.Float16FromFloat32(v).ToFloat32()) }},
		{types.T_array_bf16, func(v []float32) []byte {
			out := make([]types.BF16, len(v))
			for i, x := range v {
				out[i] = types.BF16FromFloat32(x)
			}
			return types.ArrayToBytes(out)
		}, func(v float32) float64 { return float64(types.BF16FromFloat32(v).ToFloat32()) }},
		{types.T_array_int8, func(v []float32) []byte {
			out := make([]int8, len(v))
			for i, x := range v {
				out[i] = int8(x)
			}
			return types.ArrayToBytes(out)
		}, func(v float32) float64 { return float64(v) }},
		{types.T_array_uint8, func(v []float32) []byte {
			out := make([]uint8, len(v))
			for i, x := range v {
				out[i] = uint8(x)
			}
			return types.ArrayToBytes(out)
		}, func(v float32) float64 { return float64(v) }},
	}
}

func TestVectorMatmulPlainTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	r := rand.New(rand.NewSource(9))
	const dim, nrows, k = 8, 200, 5
	for _, c := range vmPlainCases() {
		gen := func() []float32 {
			v := make([]float32, dim)
			for i := range v {
				switch c.oid {
				case types.T_array_int8:
					v[i] = float32(r.Intn(255) - 127)
				case types.T_array_uint8:
					v[i] = float32(r.Intn(256))
				default:
					v[i] = float32(r.NormFloat64())
				}
			}
			return v
		}
		queries := [][]float32{gen(), gen()}
		rows := make([][]float32, nrows)
		for i := range rows {
			rows[i] = gen()
		}
		vt := types.New(c.oid, dim, 0)
		q, _ := json.Marshal(queries)
		exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vt})
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation(EncodeVectorMatmulConfig(k, string(q), "", false), 0))
		idv := vector.NewVec(types.T_int64.ToType())
		vv := vector.NewVec(vt)
		for i, row := range rows {
			require.NoError(t, vector.AppendFixed(idv, int64(i), false, mp))
			require.NoError(t, vector.AppendBytes(vv, c.toCell(row), false, mp))
		}
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{idv, vv}))
		out := vmFlush(t, mp, exec)
		exec.Free()
		vmFree(mp, []*vector.Vector{idv, vv})

		var got [][][]any
		require.NoError(t, json.Unmarshal([]byte(out[0]), &got))
		for j, qv := range queries {
			type hit struct {
				id    int
				score float64
			}
			hits := make([]hit, nrows)
			for i, row := range rows {
				var dot float64
				for d := range row {
					dot += c.value(row[d]) * c.value(qv[d])
				}
				hits[i] = hit{i, float64(float32(dot))}
			}
			sort.Slice(hits, func(a, b int) bool {
				if hits[a].score != hits[b].score {
					return hits[a].score > hits[b].score
				}
				return strconv.Itoa(hits[a].id) < strconv.Itoa(hits[b].id)
			})
			require.Len(t, got[j], k, c.oid.String())
			for i := 0; i < k; i++ {
				require.Equal(t, strconv.Itoa(hits[i].id), got[j][i][0], "%s query %d rank %d", c.oid, j, i)
				require.InDelta(t, hits[i].score, got[j][i][1], 1e-4*math.Max(1, math.Abs(hits[i].score)), c.oid.String())
			}
		}
	}

	// integer columns take integer queries in range
	for _, tc := range []struct {
		oid     types.T
		queries string
	}{
		{types.T_array_int8, `[[1.5,0,0,0,0,0,0,0]]`},
		{types.T_array_int8, `[[200,0,0,0,0,0,0,0]]`},
		{types.T_array_uint8, `[[-1,0,0,0,0,0,0,0]]`},
	} {
		_, err := parseVectorMatmulConfig(EncodeVectorMatmulConfig(1, tc.queries, "", false), types.New(tc.oid, dim, 0))
		require.ErrorContains(t, err, "not representable", tc.queries)
	}
	_, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), types.New(types.T_array_float64, dim, 0)})
	require.Error(t, err)
}

// TestVectorMatmulConfigShared checks that executors holding the same configuration share
// one parsed configuration, that the entry is dropped when the last executor is freed, and
// that a configuration error is returned to every executor without being kept.
func TestVectorMatmulConfigShared(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := vmConfig(3, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}})
	vt := vmVecType()
	mk := func() *vectorMatmulExec {
		exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vt})
		require.NoError(t, err)
		return exec.(*vectorMatmulExec)
	}
	execs := make([]*vectorMatmulExec, 8)
	var wg sync.WaitGroup
	for i := range execs {
		execs[i] = mk()
		wg.Add(1)
		go func(e *vectorMatmulExec) {
			defer wg.Done()
			require.NoError(t, e.SetExtraInformation(cfg, 0))
		}(execs[i])
	}
	wg.Wait()
	for _, e := range execs[1:] {
		require.Same(t, execs[0].cfg, e.cfg)
	}
	// setting the configuration again keeps one hold per executor
	require.NoError(t, execs[0].SetExtraInformation(cfg, 0))
	vectorMatmulConfigs.Lock()
	require.Len(t, vectorMatmulConfigs.m, 1)
	vectorMatmulConfigs.Unlock()

	// another column type is another entry
	other, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), types.New(types.T_array_float4, vmDim, 0)})
	require.NoError(t, err)
	require.NoError(t, other.SetExtraInformation(cfg, 0))
	require.NotSame(t, execs[0].cfg, other.(*vectorMatmulExec).cfg)
	other.Free()

	for _, e := range execs {
		e.Free()
	}
	vectorMatmulConfigs.Lock()
	require.Empty(t, vectorMatmulConfigs.m)
	vectorMatmulConfigs.Unlock()

	bad := vmConfig(0, [][]float32{{1, 0, 0, 0}})
	for i := 0; i < 2; i++ {
		e := mk()
		require.Error(t, e.SetExtraInformation(bad, 0))
		e.Free()
	}
	vectorMatmulConfigs.Lock()
	require.Empty(t, vectorMatmulConfigs.m)
	vectorMatmulConfigs.Unlock()
	require.Zero(t, mp.CurrNB())
}

// vmFakeEngine is a shape-only GPU engine: it keeps no row and reports no hit.
type vmFakeEngine struct {
	maxRows, cellBytes, topk int
	closed                   *int
}

func (e *vmFakeEngine) MaxRows() int   { return e.maxRows }
func (e *vmFakeEngine) CellBytes() int { return e.cellBytes }
func (e *vmFakeEngine) TopK() int      { return e.topk }
func (e *vmFakeEngine) Close()         { *e.closed++ }
func (e *vmFakeEngine) Run(cells []byte, scores []float32) error {
	clear(scores)
	return nil
}
func (e *vmFakeEngine) RunTopK(cells []byte, top []float32, rows []int32, full []float32, tied []uint8) error {
	for i := range rows {
		rows[i] = -1
	}
	clear(tied)
	return nil
}

// TestVectorMatmulGPUMemoryAdmission checks that the engine's native host memory and the
// tile buffers are charged to the allocation account before they are allocated, counted in
// Size, released by Free, and that an account without room scores on the CPU instead.
func TestVectorMatmulGPUMemoryAdmission(t *testing.T) {
	saved := vectorMatmulGPU
	defer func() { vectorMatmulGPU = saved }()
	const hostBytes = 1 << 20
	created, closed := 0, 0
	vectorMatmulGPU = &vectorMatmulGPUHooks{
		available: func() bool { return true },
		hostBytes: func(format, dim, nq, maxRows int) uint64 { return hostBytes },
		create: func(format, dim, nq int, queryCells []byte, cellBytes, maxRows, topk int) (vectorMatmulEngine, error) {
			created++
			return &vmFakeEngine{maxRows: (maxRows + 127) / 128 * 128, cellBytes: cellBytes, topk: min(topk, maxRows), closed: &closed}, nil
		},
	}
	q, _ := json.Marshal([][]float32{{1, 0, 0, 0}})
	gpuCfg := EncodeVectorMatmulConfig(1, string(q), "", true)
	run := func(limit uint64) (*vectorMatmulExec, *mpool.AllocationAccount, []*vector.Vector, *mpool.MPool) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
		require.NoError(t, err)
		agg, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vmVecType()})
		require.NoError(t, err)
		exec := agg.(*vectorMatmulExec)
		require.NoError(t, exec.SetExtraInformation(gpuCfg, 0))
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		vecs := vmVectors(t, mp, []int64{1, 2}, [][]float32{{1, 0, 0, 0}, {2, 0, 0, 0}})
		groups := []uint64{1, 1}
		require.NoError(t, exec.PreflightBatchFill(0, groups, vecs))
		require.NoError(t, exec.BatchFill(0, groups, vecs))
		return exec, account, vecs, mp
	}

	// room for the engine: native memory and tile are charged and counted
	exec, account, vecs, mp := run(256 << 20)
	require.Equal(t, 1, created)
	require.NotNil(t, exec.engine)
	tile := exec.tileSize()
	require.Greater(t, tile, int64(hostBytes))
	require.GreaterOrEqual(t, account.Snapshot().Used, uint64(tile))
	require.GreaterOrEqual(t, exec.Size(), tile)
	exec.Free()
	vmFree(mp, vecs)
	require.Equal(t, 1, closed)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())

	// no room for the native memory: no engine, rows scored on the CPU
	exec, account, vecs, mp = run(hostBytes / 2)
	require.Equal(t, 1, created)
	require.Nil(t, exec.engine)
	require.Equal(t, []string{`[[["2",2]]]`}, vmFlush(t, mp, exec))
	exec.Free()
	vmFree(mp, vecs)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())

	// room for the native memory but not the tile: the engine is closed, its charge released
	exec, account, vecs, mp = run(hostBytes + 64<<10)
	require.Equal(t, 2, created)
	require.Equal(t, 2, closed)
	require.Nil(t, exec.engine)
	require.Equal(t, []string{`[[["2",2]]]`}, vmFlush(t, mp, exec))
	exec.Free()
	vmFree(mp, vecs)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())
}
