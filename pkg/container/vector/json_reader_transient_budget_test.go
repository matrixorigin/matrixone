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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderTransientAdmissionCoversBacking(t *testing.T) {
	for _, operation := range []string{"snapshot", "builder", "select", "materialize"} {
		t.Run(operation, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			want := errors.New("transient budget exhausted")
			var live uint64
			limit := uint64(1 << 30)
			admission := func(size uint64) (func(), error) {
				if live > limit || size > limit-live {
					return nil, want
				}
				live += size
				return func() { live -= size }, nil
			}
			large := jsonAdmissionValue(t, `{"keep":"`+strings.Repeat("x", 32<<10)+`"}`)
			switch operation {
			case "snapshot":
				source := NewVec(types.T_json.ToType())
				defer source.Free(mp)
				require.NoError(t, AppendBytes(source, large, false, mp))
				before := mp.CurrNB()
				limit = 8192
				reader, invalid, err := TryNewJSONReaderSnapshotForRows(source, 1, mp, nil, admission)
				if reader != nil {
					reader.Close()
				}
				require.ErrorIs(t, err, want)
				require.False(t, invalid)
				require.Equal(t, before, mp.CurrNB())
			case "builder":
				limit = 8192
				builder, err := NewJSONReaderBuilder(mp, nil, admission)
				require.NoError(t, err)
				defer builder.Close()
				require.ErrorIs(t, builder.Append(large, false), want)
				builder.Close()
			case "select", "materialize":
				builder, err := NewJSONReaderBuilder(mp, nil, admission)
				require.NoError(t, err)
				defer builder.Close()
				value := large
				if operation == "select" {
					value = jsonAdmissionValue(t, `1`)
				}
				require.NoError(t, builder.Append(value, false))
				reader, err := builder.Finish()
				require.NoError(t, err)
				defer reader.Close()
				before := live
				limit = live + 8192
				if operation == "select" {
					selected, err := reader.Select(make([]int64, 4096), mp)
					if selected != nil {
						selected.Close()
					}
					require.ErrorIs(t, err, want)
				} else {
					materialized, err := reader.Materialize(mp)
					if materialized != nil {
						materialized.Free(mp)
					}
					require.ErrorIs(t, err, want)
				}
				require.Equal(t, before, live)
				reader.Close()
			}
			require.Zero(t, live)
		})
	}
}

func TestJSONReaderTransientDictionaryAndMutableLifetime(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	want := errors.New("budget exhausted")
	var live uint64
	limit := uint64(1 << 30)
	admission := func(size uint64) (func(), error) {
		if live > limit || size > limit-live {
			return nil, want
		}
		live += size
		return func() { live -= size }, nil
	}
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	large := jsonAdmissionValue(t, `{"keep":"`+strings.Repeat("x", 16<<10)+`"}`)
	require.NoError(t, AppendBytes(source, large, false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	rows, err := NewRowSelection([]int64{0, 1, 0}, 2, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	dict, err := source.SelectDictionary(rows, mp, nil)
	require.NoError(t, err)
	defer dict.Free(mp)
	limit = 8192
	reader, invalid, err := TryNewJSONReaderSnapshotForRows(dict, 3, mp, nil, admission)
	require.ErrorIs(t, err, want)
	require.False(t, invalid)
	require.Nil(t, reader)
	require.Zero(t, live)
	limit = 1 << 30
	reader, invalid, err = TryNewJSONReaderSnapshotForRows(dict, 3, mp, nil, admission)
	require.NoError(t, err)
	require.False(t, invalid)
	defer reader.Close()
	materialized, err := reader.Materialize(mp)
	require.NoError(t, err)
	defer materialized.Free(mp)
	reader.Close()
	require.GreaterOrEqual(t, live, uint64(materialized.Allocated()))
	require.Equal(t, large, materialized.GetBytesAt(0))
	require.True(t, materialized.IsNull(1))
	before := live
	limit = live
	tooLarge := jsonAdmissionValue(t, `{"keep":"`+strings.Repeat("y", 128<<10)+`"}`)
	require.ErrorIs(t, AppendBytes(materialized, tooLarge, false, mp), want)
	require.Equal(t, before, live)
	require.Equal(t, large, materialized.GetBytesAt(0))
	materialized.ResetWithSameType()
	require.Positive(t, live, "reset retains admitted backing")
	materialized.Free(mp)
	require.Zero(t, live)
}

func TestJSONReaderTransientViewsRetainBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	var live uint64
	admission := func(size uint64) (func(), error) {
		live += size
		return func() { live -= size }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"k":123}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	selected, err := reader.Select([]int64{0, 0}, mp)
	require.NoError(t, err)
	window, err := selected.Window(1, 2)
	require.NoError(t, err)
	reader.Close()
	selected.Close()
	require.Positive(t, live)
	result, err := window.QueryTextAtPath(0, *jsonReaderPath(t, "$.k"), false)
	require.NoError(t, err)
	require.Equal(t, "123", result.Text)
	window.Close()
	require.Positive(t, live, "owned result keeps its reservation")
	result.Close()
	require.Zero(t, live)
}

func TestJSONReaderTransientIndependentConcurrentReaders(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	var live atomic.Int64
	admission := func(size uint64) (func(), error) {
		live.Add(int64(size))
		return func() { live.Add(-int64(size)) }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"k":123}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	path := *jsonReaderPath(t, "$.k")
	views := make([]JSONReader, 8)
	for i := range views {
		views[i], err = reader.Window(0, 1)
		require.NoError(t, err)
	}
	reader.Close()
	var wg sync.WaitGroup
	errs := make(chan error, len(views))
	for _, view := range views {
		wg.Add(1)
		go func(view JSONReader) {
			defer wg.Done()
			defer view.Close()
			selected, err := view.Select([]int64{0}, mp)
			if err != nil {
				errs <- err
				return
			}
			defer selected.Close()
			value, err := selected.QueryTextAtPath(0, path, false)
			if err != nil {
				errs <- err
				return
			}
			if value.Text != "123" {
				errs <- errors.New("unexpected value")
			}
			value.Close()
		}(view)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, live.Load())
}
