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

package vector

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderWindowMetadataIsAccounted(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	defer finalizeTestVectorAllocationAccount(t, state)
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, state.selection)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	before := state.account.Snapshot().Used
	view, err := reader.Window(0, 1)
	require.NoError(t, err)
	defer view.Close()
	require.Greater(t, state.account.Snapshot().Used, before, "live read-only view metadata must be charged")
}

func TestJSONReaderMetadataFollowsIndependentLifetimes(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	defer finalizeTestVectorAllocationAccount(t, state)
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, state.selection)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	before := state.account.Snapshot().Used
	selected, err := reader.Select([]int64{0}, mp)
	require.NoError(t, err)
	defer selected.Close()
	require.Greater(t, state.account.Snapshot().Used, before)
	view, err := selected.Window(0, 1)
	require.NoError(t, err)
	defer view.Close()
	beforeCursor := state.account.Snapshot().Used
	cursor, err := view.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	require.Greater(t, state.account.Snapshot().Used, beforeCursor)
	beforeResult := state.account.Snapshot().Used
	result, err := view.Query(0, jsonReaderPath(t, "$.keep"))
	require.NoError(t, err)
	defer result.Close()
	require.Greater(t, state.account.Snapshot().Used, beforeResult)
	reader.Close()
	selected.Close()
	view.Close()
	require.Positive(t, state.account.Snapshot().Used)
	text, err := cursor.QueryText(0, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
	cursor.Close()
	value, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "1", value.Text)
	value.Close()
	result.Close()
	require.Zero(t, state.account.Snapshot().Used)
}

func TestJSONReaderMetadataAdmissionFailureIsAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	want := errors.New("metadata admission rejected")
	var live uint64
	calls, failAt, partialReleases := 0, 0, 0
	admission := func(capacity uint64) (func(), error) {
		calls++
		if calls == failAt {
			return func() { partialReleases++ }, want
		}
		live += capacity
		return func() { live -= capacity }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	before := live
	failAt = calls + 1
	view, err := reader.Window(0, 1)
	require.ErrorIs(t, err, want)
	require.Nil(t, view)
	require.Equal(t, before, live)
	require.Equal(t, 1, partialReleases)
	failAt = calls + 1
	result, err := reader.Query(0, jsonReaderPath(t, "$.keep"))
	require.ErrorIs(t, err, want)
	require.Nil(t, result)
	require.Equal(t, before, live)
	failAt = 0
	result, err = reader.Query(0, jsonReaderPath(t, "$.keep"))
	require.NoError(t, err)
	result.Close()
	reader.Close()
	require.Zero(t, live)
}

func TestJSONReaderMetadataPublicationFailureReleasesOwner(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			want := errors.New("metadata admission rejected")
			var live uint64
			calls := 0
			admission := func(capacity uint64) (func(), error) {
				calls++
				if failAt != 0 && calls == failAt {
					return nil, want
				}
				live += capacity
				return func() { live -= capacity }, nil
			}
			builder, err := NewJSONReaderBuilder(mp, nil, admission)
			if failAt == 1 {
				require.ErrorIs(t, err, want)
				require.Nil(t, builder)
				require.Zero(t, live)
				return
			}
			require.NoError(t, err)
			defer builder.Close()
			failAt = 0
			require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
			failAt = calls + 1 // reject publication after backing has been admitted
			reader, err := builder.Finish()
			require.ErrorIs(t, err, want)
			require.Nil(t, reader)
			builder.Close()
			require.Zero(t, live)
		})
	}
}

func TestJSONReaderContainerMetadataCapacityBounds(t *testing.T) {
	capacity, err := JSONReaderContainerMetadataCapacity(0, 2)
	require.NoError(t, err)
	require.Equal(t, JSONReaderMetadataCapacity(0)+2048, capacity)
	_, err = JSONReaderContainerMetadataCapacity(0, -1)
	require.Error(t, err)
	if ^uint(0)>>63 == 1 {
		_, err = JSONReaderContainerMetadataCapacity(0, int(^uint(0)>>1))
		require.Error(t, err)
	}
}

func TestJSONReaderMetadataSelectionTakesPrecedence(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	defer finalizeTestVectorAllocationAccount(t, state)
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	calls := 0
	admission := func(uint64) (func(), error) {
		calls++
		return nil, errors.New("unexpected transient admission")
	}
	builder, err := NewJSONReaderBuilder(mp, state.selection, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	view, err := reader.Window(0, 1)
	require.NoError(t, err)
	cursor, err := view.OpenCursor()
	require.NoError(t, err)
	result, err := view.Query(0, jsonReaderPath(t, "$.keep"))
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)
	require.Zero(t, calls)
	result.Close()
	cursor.Close()
	view.Close()
	reader.Close()
	require.Zero(t, state.account.Snapshot().Used)
}

func TestJSONReaderSnapshotAdmissionFailureIsTerminal(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1}`), false, mp))
	want := errors.New("statement budget rejected")
	reader, invalid, err := TryNewJSONReaderSnapshotForRows(source, 1, mp, nil, func(uint64) (func(), error) {
		return nil, want
	})
	require.ErrorIs(t, err, want)
	require.Nil(t, reader)
	require.False(t, invalid, "a budget rejection must not become a deferred document error")
}
