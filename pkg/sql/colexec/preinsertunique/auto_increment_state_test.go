// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preinsertunique

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestInsertIgnoreFinalKeyIntegerBoundaries(t *testing.T) {
	t.Run("int8", func(t *testing.T) { testFinalKeyType(t, types.T_int8, int8(math.MaxInt8), -1) })
	t.Run("int16", func(t *testing.T) { testFinalKeyType(t, types.T_int16, int16(math.MaxInt16), math.MinInt16) })
	t.Run("int32", func(t *testing.T) { testFinalKeyType(t, types.T_int32, int32(math.MaxInt32), math.MinInt32) })
	t.Run("int64", func(t *testing.T) { testFinalKeyType(t, types.T_int64, int64(math.MaxInt64), math.MinInt64) })
	t.Run("uint8", func(t *testing.T) { testFinalKeyType(t, types.T_uint8, uint8(math.MaxUint8), 2) })
	t.Run("uint16", func(t *testing.T) { testFinalKeyType(t, types.T_uint16, uint16(math.MaxUint16), 2) })
	t.Run("uint32", func(t *testing.T) { testFinalKeyType(t, types.T_uint32, uint32(math.MaxUint32), 2) })
	t.Run("uint64", func(t *testing.T) { testFinalKeyType(t, types.T_uint64, uint64(math.MaxUint64), 2) })
}

func TestInsertIgnoreFinalKeySetMatchesExactIntegerIdentity(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	// Cover dense sequences, equal low bits, signed/unsigned extrema and a
	// reproducible wide domain. The oracle is an independent exact Go set.
	values := []uint64{0, 1, math.MaxUint64, math.MaxInt64, 1 << 63}
	rng := rand.New(rand.NewSource(28349))
	for i := range 2048 {
		values = append(values, uint64(i), uint64(i)<<32|7, rng.Uint64())
	}
	seen := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		_, exists := seen[value]
		require.Equal(t, exists, arg.hasAcceptedAutoIncrementValue(value), "key=%d", value)
		if !exists {
			require.NoError(t, arg.recordAcceptedAutoIncrementValue(value))
			seen[value] = struct{}{}
		}
	}
	for value := range seen {
		require.True(t, arg.hasAcceptedAutoIncrementValue(value), "key=%d", value)
	}
	require.Error(t, arg.recordAcceptedAutoIncrementValue(0))
	require.Error(t, arg.recordAcceptedAutoIncrementValue(math.MaxUint64))
	arg.Reset(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	for value := range seen {
		require.False(t, arg.hasAcceptedAutoIncrementValue(value), "reset key=%d", value)
	}
	arg.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}

func testFinalKeyType[T constraints.Integer](t *testing.T, oid types.T, upper, manual T) {
	t.Helper()
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{1, 2, 3, 4}, []int32{10, 20, 30, 40},
		make([]bool, 4), make([]bool, 4), []bool{true, true, false, false})
	defer input.Clean(proc.Mp())
	input.Vecs[0].Free(proc.Mp())
	input.Vecs[0] = vector.NewVec(oid.ToType())
	want := []T{1, upper, 0, manual}
	for _, value := range want {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
	}
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	require.Equal(t, want, vector.MustFixedColNoTypeCheck[T](result.Batch.Vecs[0]))
	require.Equal(t, oid.ToType(), *result.Batch.Vecs[0].GetType())
	require.Equal(t, uint64(1), proc.GetStatementLastInsertID())
	// A fresh UK does not make an already accepted final PK insertable.
	for i := range want {
		vector.MustFixedColNoTypeCheck[int32](input.Vecs[1])[i] += 100
	}
	result, err = arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty())
	require.Equal(t, want, vector.MustFixedColNoTypeCheck[T](input.Vecs[0]), "input is borrowed, never rewritten")
	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAutoIncrementCandidateCompactionKeepsOwnedBase(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	stream := &arg.ctr.autoIncrementCandidates
	// Cross the production compaction threshold with minimal scalar storage.
	// Seed the owner directly: no SQL volume or cluster is needed to prove it.
	storage, err := mpool.MakeSlice[autoIncrementCandidateRun](2048, proc.Mp(), true)
	require.NoError(t, err)
	for i := range storage {
		storage[i] = autoIncrementCandidateRun{start: uint64(i*10 + 1), step: 2, count: 3}
	}
	stream.runs = storage
	stream.typ = types.T_uint64.ToType()
	stream.initialized = true
	stream.runIndex = 1024
	stream.runOffset = 1
	before := proc.Mp().CurrNB()
	require.NoError(t, arg.compactAutoIncrementCandidates(proc))
	require.Equal(t, before, proc.Mp().CurrNB(), "compaction must not allocate a replacement")
	require.Same(t, &storage[0], &stream.runs[0])
	require.Len(t, stream.runs, 1024)
	value, ok, err := arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(10243), value.value)
	require.NoError(t, stream.discardThrough(10251))
	require.Same(t, &storage[0], &stream.runs[0], "discard also retains the owned allocation base")
	value, ok, err = arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(10253), value.value)
	arg.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAutoIncrementCandidateRejectsInvalidValueBeforePublication(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64} {
		source := vector.NewVec(oid.ToType())
		switch oid {
		case types.T_int8:
			require.NoError(t, vector.AppendFixed(source, int8(-1), false, proc.Mp()))
		case types.T_int16:
			require.NoError(t, vector.AppendFixed(source, int16(-1), false, proc.Mp()))
		case types.T_int32:
			require.NoError(t, vector.AppendFixed(source, int32(-1), false, proc.Mp()))
		case types.T_int64:
			require.NoError(t, vector.AppendFixed(source, int64(-1), false, proc.Mp()))
		}
		err := arg.appendAutoIncrementCandidate(proc, source, 0)
		source.Free(proc.Mp())
		require.ErrorContains(t, err, "negative auto-increment candidate")
	}
	require.Empty(t, arg.ctr.autoIncrementCandidates.runs)
	output := vector.NewVec(types.T_int8.ToType())
	defer output.Free(proc.Mp())
	require.Error(t, appendAutoIncrementValue(proc, output, types.T_int8.ToType(), 128))
	require.Zero(t, output.Length())
	_, err := arg.ctr.autoIncrementCandidates.valueAt(autoIncrementCandidateRun{count: 1}, 1)
	require.Error(t, err)
	_, err = arg.ctr.autoIncrementCandidates.valueAt(autoIncrementCandidateRun{start: math.MaxUint64, step: 1, count: 2}, 1)
	require.Error(t, err)
	arg.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}
