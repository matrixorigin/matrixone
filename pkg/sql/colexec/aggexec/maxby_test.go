// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package aggexec

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func assertMaxByFixedCompare[T any](t *testing.T, mp *mpool.MPool, typ types.Type, low, high T) {
	t.Helper()
	a := vector.NewVec(typ)
	b := vector.NewVec(typ)
	t.Cleanup(func() {
		a.Free(mp)
		b.Free(mp)
	})
	require.NoError(t, vector.AppendFixed(a, low, false, mp))
	require.NoError(t, vector.AppendFixed(b, high, false, mp))
	require.Less(t, compareVectorValue(a, 0, b, 0, typ), 0)
	require.Greater(t, compareVectorValue(b, 0, a, 0, typ), 0)
	require.Zero(t, compareVectorValue(a, 0, a, 0, typ))
}

func maxByInputs(t *testing.T, mp *mpool.MPool, values []string, nullValue map[int]bool, orders []int64, ties []string) []*vector.Vector {
	t.Helper()
	valueVec := vector.NewVec(types.T_varchar.ToType())
	orderVec := vector.NewVec(types.T_int64.ToType())
	tieVec := vector.NewVec(types.T_varchar.ToType())
	for i := range values {
		require.NoError(t, vector.AppendBytes(valueVec, []byte(values[i]), nullValue[i], mp))
		require.NoError(t, vector.AppendFixed(orderVec, orders[i], false, mp))
		require.NoError(t, vector.AppendBytes(tieVec, []byte(ties[i]), false, mp))
	}
	return []*vector.Vector{valueVec, orderVec, tieVec}
}

func TestMaxByCompactsReplacedVarlenaState(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}
	exec := makeMaxByExec(mp, 7004, false, params).(*maxByExec)
	require.NoError(t, exec.GroupGrow(1))
	valueVec := vector.NewVec(types.T_varchar.ToType())
	orderVec := vector.NewVec(types.T_int64.ToType())
	tieVec := vector.NewVec(types.T_int64.ToType())
	defer func() {
		valueVec.Free(mp)
		orderVec.Free(mp)
		tieVec.Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()
	for i := range 600 {
		value := []byte(fmt.Sprintf("%06d:%s", i, strings.Repeat("x", 4096)))
		require.NoError(t, vector.AppendBytes(valueVec, value, false, mp))
		require.NoError(t, vector.AppendFixed(orderVec, int64(i), false, mp))
		require.NoError(t, vector.AppendFixed(tieVec, int64(i), false, mp))
	}
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{valueVec, orderVec, tieVec}))
	state := exec.state[0].vecs[0]
	require.Less(t, state.Allocated(), 2<<20, "winner state must be bounded by live groups, not by replaced input rows")
}

func TestMaxByDefersCompactionUntilBatchPublicationCompletes(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	}
	exec := makeMaxByExec(mp, 7014, false, params).(*maxByExec)
	require.NoError(t, exec.GroupGrow(1))
	valueVec := vector.NewVec(types.T_varchar.ToType())
	orderVec := vector.NewVec(types.T_int64.ToType())
	tieVec := vector.NewVec(types.T_int64.ToType())
	defer func() {
		valueVec.Free(mp)
		orderVec.Free(mp)
		tieVec.Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()

	for i := range 400 {
		value := []byte(fmt.Sprintf("%06d:%s", i, strings.Repeat("x", 4096)))
		require.NoError(t, vector.AppendBytes(valueVec, value, false, mp))
		require.NoError(t, vector.AppendFixed(orderVec, int64(i), false, mp))
		require.NoError(t, vector.AppendFixed(tieVec, int64(i), false, mp))
	}
	source := []*vector.Vector{valueVec, orderVec, tieVec}
	for row := range 400 {
		require.NoError(t, exec.copyWinner(
			0, exec.state[0].vecs, 0, source, [3]int{row, row, row}))
	}

	before := exec.state[0].vecs[0].Allocated()
	require.Greater(t, before, maxByVarlenaCompactionSlack)
	require.Greater(t, exec.varlenaUsage[0][0].staleBytes,
		exec.varlenaUsage[0][0].liveBytes+maxByVarlenaCompactionSlack)

	exec.compactChunk(0)
	require.Less(t, exec.state[0].vecs[0].Allocated(), before)
	require.Zero(t, exec.varlenaUsage[0][0].staleBytes)
}

func TestMaxByPreservesWinningPrepareParamKind(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_text.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}
	exec := makeMaxByExec(mp, AggIdOfMaxBy, false, params)
	value := vector.NewVec(types.T_text.ToType())
	order := vector.NewVec(types.T_int64.ToType())
	tie := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendBytes(value, []byte("winner"), false, mp))
	require.NoError(t, vector.AppendBytes(value, []byte("loser"), false, mp))
	value.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	})
	require.NoError(t, vector.AppendFixed(order, int64(2), false, mp))
	require.NoError(t, vector.AppendFixed(order, int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(tie, int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(tie, int64(2), false, mp))
	defer func() {
		value.Free(mp)
		order.Free(mp)
		tie.Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()

	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{value, order, tie}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, vector.PrepareParamInteger, results[0].GetPrepareParamKindAt(0))
	for _, result := range results {
		result.Free(mp)
	}
}

func TestCompactMaxByStateVectorPreservesAllocationOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(8 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwner(1),
		mpool.AllocationSite(1),
		mpool.AllocationSite(2),
		mpool.AllocationSite(3),
		mpool.AllocationSite(4),
	)
	require.NoError(t, err)
	vec := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.SetAllocationAccount(selection))
	defer func() {
		vec.Free(mp)
		snapshot := account.Seal()
		require.Zero(t, snapshot.Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		_, err = registry.Finalize(account)
		require.NoError(t, err)
		require.Zero(t, mp.CurrNB())
	}()

	value := []byte(strings.Repeat("x", 4096))
	require.NoError(t, vector.AppendBytes(vec, value, false, mp))
	for i := 0; i < 400; i++ {
		value[0] = byte(i)
		require.NoError(t, vec.SetRawBytesAt(0, value, mp))
	}
	before := account.Snapshot()
	require.Greater(t, before.Used, uint64(maxByVarlenaCompactionSlack))

	usage := &maxByVarlenaUsage{
		liveBytes:  maxByAreaBytes(vec.GetRawBytesAt(0)),
		staleBytes: len(vec.GetArea()) - maxByAreaBytes(vec.GetRawBytesAt(0)),
	}
	require.NoError(t, compactMaxByStateVector(vec, usage, mp))
	require.Same(t, selection, vec.AllocationAccountSelection())
	require.Equal(t, value, vec.GetBytesAt(0))
	after := account.Snapshot()
	require.Less(t, after.Used, before.Used)
	require.GreaterOrEqual(t, after.Peak, before.Used)
}

func TestMaxByTracksManyGroupVarlenaUsageIncrementally(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}
	exec := makeMaxByExec(mp, 7012, false, params).(*maxByExec)
	require.NoError(t, exec.GroupGrow(AggBatchSize))

	valueVec := vector.NewVec(types.T_varchar.ToType())
	orderVec := vector.NewVec(types.T_int64.ToType())
	tieVec := vector.NewVec(types.T_int64.ToType())
	groups := make([]uint64, AggBatchSize)
	initialValue := []byte(strings.Repeat("i", 128))
	for i := range groups {
		groups[i] = uint64(i + 1)
		require.NoError(t, vector.AppendBytes(valueVec, initialValue, false, mp))
		require.NoError(t, vector.AppendFixed(orderVec, int64(0), false, mp))
		require.NoError(t, vector.AppendFixed(tieVec, int64(i), false, mp))
	}
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{valueVec, orderVec, tieVec}))
	require.Equal(t, AggBatchSize*len(initialValue), exec.varlenaUsage[0][0].liveBytes)

	candidateValue := vector.NewVec(types.T_varchar.ToType())
	candidateOrder := vector.NewVec(types.T_int64.ToType())
	candidateTie := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendBytes(candidateValue, []byte(strings.Repeat("w", 64<<10)), false, mp))
	require.NoError(t, vector.AppendFixed(candidateOrder, int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(candidateTie, int64(0), false, mp))
	for winner := int64(1); winner <= 64; winner++ {
		require.NoError(t, vector.SetFixedAtNoTypeCheck(candidateOrder, 0, winner))
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{candidateValue, candidateOrder, candidateTie}))
	}

	usage := exec.varlenaUsage[0][0]
	require.Equal(t, (AggBatchSize-1)*len(initialValue)+(64<<10), usage.liveBytes)
	require.LessOrEqual(t, usage.staleBytes, usage.liveBytes+maxByVarlenaCompactionSlack,
		"compaction should reset stale accounting without rescanning all groups per winner")
	require.Less(t, exec.state[0].vecs[0].Allocated(), 5<<20)

	for _, vec := range []*vector.Vector{valueVec, orderVec, tieVec, candidateValue, candidateOrder, candidateTie} {
		vec.Free(mp)
	}
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func BenchmarkMaxByManyGroupsRepeatedWinners(b *testing.B) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}
	exec := makeMaxByExec(mp, 7013, false, params).(*maxByExec)
	if err := exec.GroupGrow(AggBatchSize); err != nil {
		b.Fatal(err)
	}
	valueVec := vector.NewVec(types.T_varchar.ToType())
	orderVec := vector.NewVec(types.T_int64.ToType())
	tieVec := vector.NewVec(types.T_int64.ToType())
	groups := make([]uint64, AggBatchSize)
	initialValue := []byte(strings.Repeat("i", 128))
	for i := range groups {
		groups[i] = uint64(i + 1)
		if err := vector.AppendBytes(valueVec, initialValue, false, mp); err != nil {
			b.Fatal(err)
		}
		if err := vector.AppendFixed(orderVec, int64(0), false, mp); err != nil {
			b.Fatal(err)
		}
		if err := vector.AppendFixed(tieVec, int64(i), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	if err := exec.BatchFill(0, groups, []*vector.Vector{valueVec, orderVec, tieVec}); err != nil {
		b.Fatal(err)
	}
	candidateValue := vector.NewVec(types.T_varchar.ToType())
	candidateOrder := vector.NewVec(types.T_int64.ToType())
	candidateTie := vector.NewVec(types.T_int64.ToType())
	if err := vector.AppendBytes(candidateValue, []byte(strings.Repeat("w", 64<<10)), false, mp); err != nil {
		b.Fatal(err)
	}
	if err := vector.AppendFixed(candidateOrder, int64(1), false, mp); err != nil {
		b.Fatal(err)
	}
	if err := vector.AppendFixed(candidateTie, int64(0), false, mp); err != nil {
		b.Fatal(err)
	}
	candidate := []*vector.Vector{candidateValue, candidateOrder, candidateTie}
	defer func() {
		for _, vec := range append([]*vector.Vector{valueVec, orderVec, tieVec}, candidate...) {
			vec.Free(mp)
		}
		exec.Free()
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := vector.SetFixedAtNoTypeCheck(candidate[1], 0, int64(i+1)); err != nil {
			b.Fatal(err)
		}
		if err := exec.Fill(0, 0, candidate); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMaxByNullContractAndDeterministicMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	inputs := maxByInputs(t, mp, []string{"ignored", "older"}, map[int]bool{0: true}, []int64{10, 9}, []string{"z", "a"})
	defer func() {
		for _, input := range inputs {
			input.Free(mp)
		}
	}()

	keepNull := makeMaxByExec(mp, 7001, false, params).(*maxByExec)
	require.NoError(t, keepNull.GroupGrow(1))
	require.NoError(t, keepNull.BulkFill(0, inputs))
	result, err := keepNull.Flush()
	require.NoError(t, err)
	require.True(t, result[0].IsNull(0), "max_by must retain a winning NULL value")
	result[0].Free(mp)

	skipNull := makeMaxByExec(mp, 7002, true, params).(*maxByExec)
	require.NoError(t, skipNull.GroupGrow(1))
	require.NoError(t, skipNull.BulkFill(0, inputs))
	result, err = skipNull.Flush()
	require.NoError(t, err)
	require.Equal(t, "older", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)

	leftInput := maxByInputs(t, mp, []string{"alpha"}, nil, []int64{10}, []string{"same"})
	rightInput := maxByInputs(t, mp, []string{"beta"}, nil, []int64{10}, []string{"same"})
	defer func() {
		for _, list := range [][]*vector.Vector{leftInput, rightInput} {
			for _, input := range list {
				input.Free(mp)
			}
		}
	}()
	left := makeMaxByExec(mp, 7003, false, params).(*maxByExec)
	right := makeMaxByExec(mp, 7003, false, params).(*maxByExec)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.BulkFill(0, leftInput))
	require.NoError(t, right.BulkFill(0, rightInput))
	require.NoError(t, left.Merge(right, 0, 0))

	var serialized bytes.Buffer
	require.NoError(t, left.SaveIntermediateResultOfChunk(0, &serialized))
	restored := makeMaxByExec(mp, 7003, false, params).(*maxByExec)
	require.NoError(t, restored.UnmarshalFromReader(&serialized, mp))
	result, err = restored.Flush()
	require.NoError(t, err)
	require.Equal(t, "beta", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	left.Free()
	right.Free()
	restored.Free()
}

func TestMaxByEqualWinnerOrsBinaryStringProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	for _, binaryFirst := range []bool{false, true} {
		inputs := maxByInputs(t, mp, []string{"same", "same"}, nil, []int64{10, 10}, []string{"tie", "tie"})
		require.NoError(t, inputs[0].SetBinaryStringRowsWithMP([]bool{binaryFirst, !binaryFirst}, mp))
		exec := makeMaxByExec(mp, 7020, false, params).(*maxByExec)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, inputs))
		result, err := exec.Flush()
		require.NoError(t, err)
		require.True(t, result[0].GetBinaryStringMetadataAt(0))
		result[0].Free(mp)
		exec.Free()
		for _, input := range inputs {
			input.Free(mp)
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestMaxByPreservesBinaryStringProvenanceAcrossGroups(t *testing.T) {
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	for _, binaryFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("binary_first_%t", binaryFirst), func(t *testing.T) {
			mp := mpool.MustNewZero()
			inputs := maxByInputs(t, mp, []string{"first", "second"}, nil, []int64{1, 1}, []string{"a", "b"})
			require.NoError(t, inputs[0].SetBinaryStringRowsWithMP([]bool{binaryFirst, !binaryFirst}, mp))
			exec := makeMaxByExec(mp, AggIdOfMaxBy, false, params).(*maxByExec)
			require.NoError(t, exec.GroupGrow(2))
			require.NoError(t, exec.Fill(0, 0, inputs))
			require.NoError(t, exec.Fill(1, 1, inputs))

			result, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, binaryFirst, result[0].GetBinaryStringMetadataAt(0))
			require.Equal(t, !binaryFirst, result[0].GetBinaryStringMetadataAt(1))

			result[0].Free(mp)
			exec.Free()
			for _, input := range inputs {
				input.Free(mp)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestMaxBySpillPreservesBinaryStringProvenanceAcrossGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType(),
	}
	inputs := maxByInputs(t, mp,
		[]string{"binary", "text"}, nil, []int64{1, 1}, []string{"a", "b"})
	require.NoError(t,
		inputs[0].SetBinaryStringRowsWithMP([]bool{true, false}, mp))

	source := makeMaxByExec(mp, AggIdOfMaxBy, false, params).(*maxByExec)
	require.NoError(t, source.GroupGrow(2))
	require.NoError(t, source.Fill(0, 0, inputs))
	require.NoError(t, source.Fill(1, 1, inputs))
	require.True(t, source.state[0].vecs[0].GetBinaryStringMetadataAt(0))
	require.False(t, source.state[0].vecs[0].GetBinaryStringMetadataAt(1))
	var spill bytes.Buffer
	require.NoError(t, source.SaveSpillIntermediateResult(
		2, 0, []uint8{1, 1}, &spill))

	restored := makeMaxByExec(mp, AggIdOfMaxBy, false, params).(*maxByExec)
	require.NoError(t, restored.UnmarshalSpillFromReader(
		bytes.NewReader(spill.Bytes()), mp))
	result, err := restored.Flush()
	require.NoError(t, err)
	require.True(t, result[0].GetBinaryStringMetadataAt(0))
	require.False(t, result[0].GetBinaryStringMetadataAt(1))

	result[0].Free(mp)
	source.Free()
	restored.Free()
	for _, input := range inputs {
		input.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func TestMaxByMergeIsCommutativeAndAssociative(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	inputs := []*vector.Vector(nil)
	for _, candidate := range []string{"alpha", "gamma", "beta"} {
		inputs = append(inputs, maxByInputs(t, mp, []string{candidate}, nil, []int64{10}, []string{"same"})...)
	}
	defer func() {
		for _, input := range inputs {
			input.Free(mp)
		}
		require.Zero(t, mp.CurrNB())
	}()

	run := func(merge func([]*maxByExec) error) string {
		states := make([]*maxByExec, 3)
		for i := range states {
			states[i] = makeMaxByExec(mp, 7005, false, params).(*maxByExec)
			require.NoError(t, states[i].GroupGrow(1))
			require.NoError(t, states[i].BulkFill(0, inputs[i*3:i*3+3]))
		}
		require.NoError(t, merge(states))
		result, err := states[0].Flush()
		require.NoError(t, err)
		winner := string(result[0].GetBytesAt(0))
		result[0].Free(mp)
		for _, state := range states {
			state.Free()
		}
		return winner
	}

	leftAssociated := run(func(states []*maxByExec) error {
		if err := states[0].Merge(states[1], 0, 0); err != nil {
			return err
		}
		return states[0].Merge(states[2], 0, 0)
	})
	rightAssociated := run(func(states []*maxByExec) error {
		if err := states[1].Merge(states[2], 0, 0); err != nil {
			return err
		}
		return states[0].Merge(states[1], 0, 0)
	})
	reversed := run(func(states []*maxByExec) error {
		if err := states[0].Merge(states[2], 0, 0); err != nil {
			return err
		}
		return states[0].Merge(states[1], 0, 0)
	})
	require.Equal(t, "gamma", leftAssociated)
	require.Equal(t, leftAssociated, rightAssociated)
	require.Equal(t, leftAssociated, reversed)
}

func TestMaxByRejectsIncompatibleMergeAndArity(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	left := makeMaxByExec(mp, 7007, false, params).(*maxByExec)
	right := makeMaxByExec(mp, 7008, true, params).(*maxByExec)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.ErrorContains(t, left.Merge(right, 0, 0), "incompatible")
	require.ErrorContains(t, left.BulkFill(0, nil), "three input vectors")
	left.Free()
	right.Free()

	_, err := MakeAgg(mp, AggIdOfMaxBy, false, types.T_varchar.ToType())
	require.ErrorContains(t, err, "requires value, order, and tie")
	require.Zero(t, mp.CurrNB())
}

func TestMaxByOOMDoesNotPublishMixedWinner(t *testing.T) {
	mp, err := mpool.NewMPool("max-by-oom", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	exec := makeMaxByExec(mp, 7006, false, params).(*maxByExec)
	require.NoError(t, exec.GroupGrow(1))
	old := maxByInputs(t, mp, []string{"old"}, nil, []int64{1}, []string{"old"})
	candidate := maxByInputs(t, mp,
		[]string{strings.Repeat("v", 400<<10)}, nil, []int64{2}, []string{strings.Repeat("t", 400<<10)})
	defer func() {
		for _, list := range [][]*vector.Vector{old, candidate} {
			for _, input := range list {
				input.Free(mp)
			}
		}
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, exec.BulkFill(0, old))
	require.Error(t, exec.BulkFill(0, candidate))

	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "old", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
}

func TestMaxByComparisonCoversSupportedOrderTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	assertMaxByFixedCompare(t, mp, types.T_bool.ToType(), false, true)
	assertMaxByFixedCompare(t, mp, types.T_int8.ToType(), int8(1), int8(2))
	assertMaxByFixedCompare(t, mp, types.T_int16.ToType(), int16(1), int16(2))
	assertMaxByFixedCompare(t, mp, types.T_int32.ToType(), int32(1), int32(2))
	assertMaxByFixedCompare(t, mp, types.T_int64.ToType(), int64(1), int64(2))
	assertMaxByFixedCompare(t, mp, types.T_uint8.ToType(), uint8(1), uint8(2))
	assertMaxByFixedCompare(t, mp, types.T_uint16.ToType(), uint16(1), uint16(2))
	assertMaxByFixedCompare(t, mp, types.T_uint32.ToType(), uint32(1), uint32(2))
	assertMaxByFixedCompare(t, mp, types.T_uint64.ToType(), uint64(1), uint64(2))
	assertMaxByFixedCompare(t, mp, types.T_bit.ToType(), uint64(1), uint64(2))
	assertMaxByFixedCompare(t, mp, types.T_float32.ToType(), float32(1), float32(2))
	assertMaxByFixedCompare(t, mp, types.T_float64.ToType(), float64(1), float64(2))
	assertMaxByFixedCompare(t, mp, types.T_date.ToType(), types.Date(1), types.Date(2))
	assertMaxByFixedCompare(t, mp, types.T_datetime.ToType(), types.Datetime(1), types.Datetime(2))
	assertMaxByFixedCompare(t, mp, types.T_timestamp.ToType(), types.Timestamp(1), types.Timestamp(2))
	assertMaxByFixedCompare(t, mp, types.T_time.ToType(), types.Time(1), types.Time(2))
	assertMaxByFixedCompare(t, mp, types.T_year.ToType(), types.MoYear(1), types.MoYear(2))
	decimal64Low, err := types.ParseDecimal64("1", 18, 0)
	require.NoError(t, err)
	decimal64High, err := types.ParseDecimal64("2", 18, 0)
	require.NoError(t, err)
	assertMaxByFixedCompare(t, mp, types.T_decimal64.ToType(), decimal64Low, decimal64High)
	decimal128Low, err := types.ParseDecimal128("1", 38, 0)
	require.NoError(t, err)
	decimal128High, err := types.ParseDecimal128("2", 38, 0)
	require.NoError(t, err)
	assertMaxByFixedCompare(t, mp, types.T_decimal128.ToType(), decimal128Low, decimal128High)
	decimal256Low, err := types.ParseDecimal256("1", 76, 0)
	require.NoError(t, err)
	decimal256High, err := types.ParseDecimal256("2", 76, 0)
	require.NoError(t, err)
	assertMaxByFixedCompare(t, mp, types.T_decimal256.ToType(), decimal256Low, decimal256High)
	uuidLow, err := types.ParseUuid("00000000-0000-0000-0000-000000000001")
	require.NoError(t, err)
	uuidHigh, err := types.ParseUuid("00000000-0000-0000-0000-000000000002")
	require.NoError(t, err)
	assertMaxByFixedCompare(t, mp, types.T_uuid.ToType(), uuidLow, uuidHigh)

	a := vector.NewVec(types.T_varchar.ToType())
	b := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(a, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(b, []byte("b"), false, mp))
	require.Less(t, compareVectorValue(a, 0, b, 0, types.T_varchar.ToType()), 0)
	a.Free(mp)
	b.Free(mp)
}

func TestMaxByNullableAndNaNComparison(t *testing.T) {
	mp := mpool.MustNewZero()
	a := vector.NewVec(types.T_varchar.ToType())
	b := vector.NewVec(types.T_varchar.ToType())
	defer func() {
		a.Free(mp)
		b.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, vector.AppendBytes(a, nil, true, mp))
	require.NoError(t, vector.AppendBytes(a, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(b, nil, true, mp))
	require.NoError(t, vector.AppendBytes(b, []byte("b"), false, mp))

	require.Zero(t, compareNullableVectorValue(a, 0, b, 0, types.T_varchar.ToType()))
	require.Less(t, compareNullableVectorValue(a, 0, b, 1, types.T_varchar.ToType()), 0)
	require.Greater(t, compareNullableVectorValue(a, 1, b, 0, types.T_varchar.ToType()), 0)
	require.Less(t, compareNullableVectorValue(a, 1, b, 1, types.T_varchar.ToType()), 0)
	require.Zero(t, compareNullableRaw(a, 0, b, 0))
	require.Less(t, compareNullableRaw(a, 0, b, 1), 0)
	require.Greater(t, compareNullableRaw(a, 1, b, 0), 0)
	require.Less(t, compareNullableRaw(a, 1, b, 1), 0)

	nan := math.NaN()
	require.Zero(t, compareFloat64(nan, nan))
	require.Greater(t, compareFloat64(nan, 1), 0)
	require.Less(t, compareFloat64(1, nan), 0)
	require.Less(t, compareFloat64(1, 2), 0)
}

func TestMaxByFillAndMergeSkipBranches(t *testing.T) {
	mp := mpool.MustNewZero()
	params := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()}
	exec := makeMaxByExec(mp, 7010, false, params).(*maxByExec)
	other := makeMaxByExec(mp, 7010, false, params).(*maxByExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, other.GroupGrow(1))
	inputs := maxByInputs(t, mp, []string{"value"}, nil, []int64{1}, []string{"tie"})
	require.NoError(t, exec.BatchFill(0, []uint64{GroupNotMatched}, inputs))
	require.NoError(t, exec.Fill(0, 0, inputs))
	require.NoError(t, exec.BatchMerge(other, 0, []uint64{GroupNotMatched}))
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	require.ErrorContains(t, exec.Fill(0, 0, nil), "three input vectors")
	require.ErrorContains(t, exec.BatchFill(0, nil, nil), "three input vectors")
	require.Panics(t, func() { makeMaxByExec(mp, 7011, false, nil) })
	for _, input := range inputs {
		input.Free(mp)
	}
	exec.Free()
	other.Free()
	require.Zero(t, mp.CurrNB())
}
