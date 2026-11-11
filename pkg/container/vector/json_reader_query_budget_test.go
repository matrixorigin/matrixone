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
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderDirectTextCopiesToAccountedResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	want := errors.New("reader memory denied")
	var live uint64
	deny := false
	admission := func(n uint64) (func(), error) {
		if deny {
			return nil, want
		}
		live += n
		return func() { live -= n }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	long := strings.Repeat("x", 8192)
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":"text","number":123,"long":"`+long+`","arr":[1,2]}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	direct, ok := reader.(JSONDirectTextReader)
	require.True(t, ok)
	output := NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer output.Free()
	writer := MustFunctionResult[types.Varlena](output)
	baseline := live
	deny = true
	for _, test := range []struct{ path, text string }{{"$.keep", "text"}, {"$.number", "123"}, {"$.long", long}} {
		require.NoError(t, output.PreExtendAndReset(1))
		selected, appended, err := direct.QueryTextTo(0, []byte(test.path), false, -1, writer)
		selected.Close()
		require.NoError(t, err)
		require.True(t, appended)
		require.Equal(t, test.text, string(output.GetResultVector().GetBytesAt(0)))
		require.Equal(t, baseline, live)
	}
	require.NoError(t, output.PreExtendAndReset(1))
	selected, appended, err := direct.QueryTextAtPathTo(0, *jsonReaderPath(t, "$.keep"), false, 3, writer)
	selected.Close()
	require.False(t, appended)
	require.ErrorIs(t, err, want, "over-width text must use the admitted owned path")
	selected, appended, err = direct.QueryTextAtPathTo(0, *jsonReaderPath(t, "$.arr[*]"), false, 512, writer)
	selected.Close()
	require.False(t, appended)
	require.ErrorIs(t, err, want, "wildcard result backing must remain budgeted")
	require.NoError(t, output.PreExtendAndReset(1))
	selected, appended, err = direct.QueryTextAtPathTo(0, *jsonReaderPath(t, "$.long"), false, -1, writer)
	selected.Close()
	require.NoError(t, err)
	require.True(t, appended)
	reader.Close()
	require.Zero(t, live)
	require.Equal(t, long, string(output.GetResultVector().GetBytesAt(0)), "result must outlive its source reader")
}

// TestJSONReaderQueryRespectsBudget keeps the source and a cursor admitted,
// then leaves room for only the fixed result metadata, not the selected data.
func TestJSONReaderQueryRespectsBudget(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	want := errors.New("query budget exhausted")
	var live uint64
	limit := ^uint64(0)
	admission := func(n uint64) (func(), error) {
		if live > limit || n > limit-live {
			return nil, want
		}
		live += n
		return func() { live -= n }, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `["`+strings.Repeat("x", 8192)+`","y"]`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	path := jsonReaderPath(t, "$[*]")

	limit = live + JSONReaderMetadataCapacity(reflect.TypeFor[jsonQueryResult]().Size())
	result, err := reader.Query(0, path)
	if result != nil {
		result.Close()
	}
	require.ErrorIs(t, err, want, "result backing must be admitted before allocation")

	limit = live
	require.ErrorIs(t, cursor.ResetQuery(0, *path), want, "cursor reset must admit result backing")
	single := jsonReaderPath(t, "$[0]")
	_, err = reader.QueryTextAtPath(0, *single, false)
	require.ErrorIs(t, err, want, "owned output text must be admitted before copying")
	limit = live + JSONReaderMetadataCapacity(reflect.TypeFor[jsonQueryResult]().Size())
	result, err = reader.Query(0, single)
	require.NoError(t, err)
	defer result.Close()
	limit = live
	text, err := result.Text()
	text.Close()
	require.ErrorIs(t, err, want, "result text must be admitted before copying")
}

func TestJSONReaderQueryBudgetLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	want := errors.New("query budget exhausted")
	var live uint64
	failNext, partialReleases := false, 0
	admission := func(n uint64) (func(), error) {
		live += n
		if failNext {
			failNext = false
			return func() { live -= n; partialReleases++ }, want
		}
		return func() { live -= n }, nil
	}
	defer func() { require.Zero(t, live) }()
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	value := strings.Repeat("x", 128)
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `["`+value+`","y"]`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	path := jsonReaderPath(t, "$[*]")
	baseline := live
	first, err := reader.Query(0, path)
	require.NoError(t, err)
	defer first.Close()
	afterFirst := live
	require.Greater(t, afterFirst-baseline, JSONReaderMetadataCapacity(reflect.TypeFor[jsonQueryResult]().Size()))
	second, err := reader.Query(0, path)
	require.NoError(t, err)
	defer second.Close()
	require.Greater(t, live, afterFirst)
	first.Close()
	require.Equal(t, afterFirst, live, "each result owns its own backing charge")
	require.True(t, second.Exists())

	require.NoError(t, cursor.ResetQuery(0, *path))
	require.Greater(t, live, afterFirst)
	require.NoError(t, cursor.ResetQuery(0, *jsonReaderPath(t, "$[0]")))
	require.Equal(t, afterFirst, live, "reset releases the prior merged result")
	failNext = true
	require.ErrorIs(t, cursor.ResetQuery(0, *path), want)
	require.Equal(t, afterFirst, live)
	require.Equal(t, 1, partialReleases)

	selected, err := reader.QueryTextAtPath(0, *jsonReaderPath(t, "$[0]"), false)
	require.NoError(t, err)
	defer selected.Close()
	alias := selected
	text, err := second.Text()
	require.NoError(t, err)
	defer text.Close()
	second.Close()
	cursor.Close()
	reader.Close()
	builder.Close()
	require.Equal(t, value, selected.Text)
	require.Contains(t, text.Text, value)
	require.Positive(t, live, "independent output remains charged after reader close")
	selected.Close()
	alias.Close()
	text.Close()
	require.Zero(t, live)
}

func TestJSONReaderQueryUsesAllocationAccount(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	defer finalizeTestVectorAllocationAccount(t, state)
	builder, err := NewJSONReaderBuilder(mp, state.selection)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `["`+strings.Repeat("x", 8192)+`","y"]`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	path := jsonReaderPath(t, "$[*]")
	baseline := state.account.Snapshot().Used
	result, err := reader.Query(0, path)
	require.NoError(t, err)
	defer result.Close()
	require.Greater(t, state.account.Snapshot().Used-baseline, uint64(8192))
	result.Close()
	require.Equal(t, baseline, state.account.Snapshot().Used)

	output, err := NewFunctionResultWrapperWithAllocation(types.T_varchar.ToType(), mp, state.selection)
	require.NoError(t, err)
	defer output.Free()
	require.NoError(t, output.PreExtendAndReset(1))
	snapshot := state.account.Snapshot()
	metadata := JSONReaderMetadataCapacity(reflect.TypeFor[jsonQueryResult]().Size())
	require.Greater(t, snapshot.Limit-snapshot.Used, metadata)
	fillerSize := snapshot.Limit - snapshot.Used - metadata
	reservation, err := state.account.ReserveCapacity(fillerSize, testVectorAllocationOwner, testVectorDataAllocationSite)
	require.NoError(t, err)
	filler, err := reservation.Commit(fillerSize)
	require.NoError(t, err)
	defer filler.Release()
	result, err = reader.Query(0, path)
	if result != nil {
		result.Close()
	}
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	direct := reader.(JSONDirectTextReader)
	selected, appended, err := direct.QueryTextTo(0, []byte("$[0]"), false, -1, MustFunctionResult[types.Varlena](output))
	selected.Close()
	require.True(t, appended)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity, "direct output must use the statement account")
	filler.Release()
	require.NoError(t, output.PreExtendAndReset(1))
	selected, appended, err = direct.QueryTextTo(0, []byte("$[0]"), false, -1, MustFunctionResult[types.Varlena](output))
	selected.Close()
	require.NoError(t, err)
	require.True(t, appended)
	require.Len(t, output.GetResultVector().GetBytesAt(0), 8192)
}

func TestJSONReaderIndependentBudgetedReaders(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	var mu sync.Mutex
	var live uint64
	admission := func(n uint64) (func(), error) {
		mu.Lock()
		live += n
		mu.Unlock()
		return func() {
			mu.Lock()
			live -= n
			mu.Unlock()
		}, nil
	}
	builder, err := NewJSONReaderBuilder(mp, nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `["first","second",123]`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	path := jsonReaderPath(t, "$[*]")
	single := *jsonReaderPath(t, "$[0]")
	number := *jsonReaderPath(t, "$[2]")
	views := make([]JSONReader, 2)
	for i := range views {
		views[i], err = reader.Window(0, 1)
		require.NoError(t, err)
		defer views[i].Close()
	}
	reader.Close()
	start := make(chan struct{})
	errs := make(chan error, len(views))
	var done sync.WaitGroup
	for _, view := range views {
		done.Add(1)
		go func(view JSONReader) {
			defer done.Done()
			defer view.Close()
			<-start
			result, queryErr := view.Query(0, path)
			if queryErr != nil {
				errs <- queryErr
				return
			}
			text, textErr := result.Text()
			result.Close()
			if textErr == nil && !strings.Contains(text.Text, "first") {
				textErr = errors.New("missing query text")
			}
			text.Close()
			if textErr != nil {
				errs <- textErr
				return
			}
			selected, selectErr := view.QueryTextAtPath(0, single, false)
			if selectErr == nil && selected.Text != "first" {
				selectErr = errors.New("unexpected selected text")
			}
			selected.Close()
			if selectErr == nil {
				localMP := mpool.MustNewZero()
				output := NewFunctionResultWrapper(types.T_varchar.ToType(), localMP)
				selectErr = output.PreExtendAndReset(1)
				if selectErr == nil {
					var appended bool
					selected, appended, selectErr = view.(JSONDirectTextReader).QueryTextAtPathTo(0, number, false, 512, MustFunctionResult[types.Varlena](output))
					selected.Close()
					if selectErr == nil && (!appended || string(output.GetResultVector().GetBytesAt(0)) != "123") {
						selectErr = errors.New("unexpected direct text")
					}
				}
				output.Free()
				mpool.DeleteMPool(localMP)
			}
			errs <- selectErr
		}(view)
	}
	close(start)
	results := make([]error, 0, len(views))
	for range views {
		results = append(results, <-errs)
	}
	done.Wait()
	for _, result := range results {
		require.NoError(t, result)
	}
	builder.Close()
	mu.Lock()
	remaining := live
	mu.Unlock()
	require.Zero(t, remaining)
}
