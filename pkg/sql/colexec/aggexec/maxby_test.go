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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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
