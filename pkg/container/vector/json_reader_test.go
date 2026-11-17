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
	"encoding/binary"
	"io"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func jsonReaderPath(t testing.TB, text string) *bytejson.Path {
	t.Helper()
	path, err := types.ParseStringToPath(text)
	require.NoError(t, err)
	return &path
}

func TestJSONReaderSnapshotIsolation(t *testing.T) {
	for _, document := range []string{`[1]`, `{"keep":1,"large":[0,0]}`} {
		t.Run(document, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewVec(types.T_json.ToType())
			t.Cleanup(func() { source.Free(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, document), false, mp))
			path := jsonReaderPath(t, "$.keep")
			if document == `[1]` {
				path = jsonReaderPath(t, "$[0]")
			}
			reader, err := NewJSONReaderSnapshot(source, mp)
			require.NoError(t, err)
			defer reader.Close()
			root := types.DecodeJson(source.GetBytesAt(0))
			var value bytejson.ByteJson
			if document == `[1]` {
				value = root.GetArrayElem(0)
			} else {
				value = root.GetObjectVal(0)
			}
			binary.LittleEndian.PutUint64(value.Data, 2)
			result, err := reader.Query(0, path)
			require.NoError(t, err)
			reader.Close()
			text, err := result.Text()
			require.NoError(t, err)
			require.Equal(t, "1", text.Text)
			text.Close()
			result.Close()
			require.ErrorIs(t, func() error { _, err := result.Text(); return err }(), io.ErrClosedPipe)
			fresh, err := NewJSONReaderSnapshot(source, mp)
			require.NoError(t, err)
			defer fresh.Close()
			second, err := fresh.Query(0, path)
			require.NoError(t, err)
			defer second.Close()
			text, err = second.Text()
			require.NoError(t, err)
			require.Equal(t, "2", text.Text)
			text.Close()
		})
	}
}

func TestJSONReaderRejectsUnselectedCorruptionAndDescriptors(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_json.ToType())
	defer func() { source.Free(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1,"large":[0,0]}`), false, mp))
	root := types.DecodeJson(source.GetBytesAt(0))
	root.GetObjectVal(1).Data[8+5] = 0xfd
	reader, err := NewJSONReaderSnapshot(source, mp)
	require.Error(t, err)
	require.Nil(t, reader)
	root.GetObjectVal(1).Data[8+5] = bytejson.TpCodeInt64
	descriptors := MustFixedColWithTypeCheck[types.Varlena](source)
	descriptors[0].SetOffsetLen(^uint32(0), 100)
	reader, err = NewJSONReaderSnapshot(source, mp)
	require.Error(t, err)
	require.Nil(t, reader)
}

func TestJSONReaderBuilderTypedAppend(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	value, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	require.NoError(t, builder.AppendByteJson(value, false))
	binary.LittleEndian.PutUint64(value.Data[len(value.Data)-8:], 9)
	require.Error(t, builder.AppendByteJson(bytejson.ByteJson{Type: 0xff}, false))
	require.NoError(t, builder.AppendByteJson(bytejson.ByteJson{}, true))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, 2, reader.Length(), "failed admission must not publish a row")
	result, err := reader.Query(0, jsonReaderPath(t, "$.keep"))
	require.NoError(t, err)
	defer result.Close()
	text, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
	wasNull, err := reader.IsNull(1)
	require.NoError(t, err)
	require.True(t, wasNull)
}

func TestJSONReaderBuilderViewsAndOwnedOutput(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	raw := jsonAdmissionValue(t, `{"keep":"retained payload longer than inline"}`)
	require.NoError(t, builder.Append(raw, false))
	require.NoError(t, builder.Append(nil, true))
	require.Error(t, builder.Append([]byte{0xff}, false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	require.ErrorIs(t, builder.Append(raw, false), io.ErrClosedPipe)
	for i := range raw {
		raw[i] = 0xff
	}
	view, err := reader.Window(0, 2)
	require.NoError(t, err)
	defer view.Close()
	reader.Close()
	path := jsonReaderPath(t, "$.keep")
	result, err := view.Query(0, path)
	require.NoError(t, err)
	defer result.Close()
	destination := NewVec(types.T_json.ToType())
	defer destination.Free(mp)
	require.NoError(t, result.AppendJSON(destination, mp))
	destination.GetBytesAt(0)[1] = 0xff
	text, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "retained payload longer than inline", text.Text)
	text.Close()
	null, err := view.Query(1, path)
	require.NoError(t, err)
	require.True(t, null.SQLNull())
	require.False(t, null.Exists())
	null.Close()
	miss, err := view.Query(0, jsonReaderPath(t, "$.missing"))
	require.NoError(t, err)
	require.False(t, miss.SQLNull())
	require.False(t, miss.Exists())
	miss.Close()
	_, err = view.Window(-1, 1)
	require.Error(t, err)
	_, err = view.Query(2, path)
	require.Error(t, err)
}

func TestJSONReaderIndependentConcurrentReaders(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	path := jsonReaderPath(t, "$.keep")
	start := make(chan struct{})
	results := make(chan error, 2)
	var done sync.WaitGroup
	for range 2 {
		view, err := reader.Window(0, 1)
		require.NoError(t, err)
		done.Add(1)
		go func() {
			defer done.Done()
			defer view.Close()
			<-start
			result, err := view.Query(0, path)
			if err == nil {
				var text JSONOwnedText
				text, err = result.Text()
				if err == nil && text.Text != "1" {
					err = io.ErrUnexpectedEOF
				}
				text.Close()
				result.Close()
			}
			if err == nil {
				var selected JSONTextSelection
				selected, err = view.QueryTextAtPath(0, *path, false)
				if err == nil && selected.Text != "1" {
					err = io.ErrUnexpectedEOF
				}
				selected.Close()
			}
			results <- err
		}()
	}
	reader.Close()
	close(start)
	done.Wait()
	for range 2 {
		require.NoError(t, <-results)
	}
}

func TestJSONReaderSelectionAndConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	for _, text := range []string{`1`, `2`, `3`} {
		require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, text), false, mp))
	}
	reader, err := NewJSONReaderSnapshot(source, mp)
	require.NoError(t, err)
	defer reader.Close()
	indices := []int64{2, 0, 2}
	selected, err := reader.Select(indices, mp)
	require.NoError(t, err)
	defer selected.Close()
	indices[0] = 100
	window, err := selected.Window(1, 3)
	require.NoError(t, err)
	defer window.Close()
	nested, err := window.Select([]int64{1, 0}, mp)
	require.NoError(t, err)
	defer nested.Close()
	reader.Close()
	selected.Close()
	window.Close()
	owned, err := nested.Materialize(mp)
	require.NoError(t, err)
	require.Equal(t, 2, owned.Length())
	owned.GetBytesAt(0)[1] ^= 1
	owned.Free(mp)
	path := jsonReaderPath(t, "$")
	for i, want := range []string{"3", "1"} {
		result, err := nested.Query(i, path)
		require.NoError(t, err)
		text, err := result.Text()
		result.Close()
		require.NoError(t, err)
		require.Equal(t, want, text.Text)
		text.Close()
	}
	constant := NewConstNull(types.T_json.ToType(), 3, mp)
	defer constant.Free(mp)
	nulls, err := NewJSONReaderSnapshot(constant, mp)
	require.NoError(t, err)
	defer nulls.Close()
	null, err := nulls.Query(2, path)
	require.NoError(t, err)
	require.True(t, null.SQLNull())
	null.Close()
}

func TestJSONReaderSnapshotAllocationFailure(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, 1<<20, 128)
			defer finalizeTestVectorAllocationAccount(t, state)
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			source, err := NewOffHeapVecWithTypeAndAllocation(types.T_json.ToType(), state.selection)
			require.NoError(t, err)
			defer source.Free(mp)
			require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1,"large":[0,0]}`), false, mp))
			controller := &rejectNextVectorAllocation{failAt: failAt}
			class, err := state.account.RegisterCapacityController(controller)
			require.NoError(t, err)
			defer func() { require.NoError(t, state.account.UnregisterCapacityController(class, controller)) }()
			selection := *state.selection
			selection.capacityClass = class
			source.allocationAccount = &selection
			before := mp.CurrNB()
			reader, invalid, err := TryNewJSONReaderSnapshotForRows(source, source.Length(), mp, &selection)
			require.Error(t, err)
			require.Nil(t, reader)
			require.False(t, invalid, "allocation failure cannot be deferred as bad input")
			require.Equal(t, before, mp.CurrNB())
			require.Zero(t, controller.used)
		})
	}
}

func TestJSONReaderSnapshotReportsInvalidWithoutPublishing(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1}`), false, mp))
	source.GetBytesAt(0)[0] = 0xfd
	before := mp.CurrNB()
	reader, invalid, err := TryNewJSONReaderSnapshotForRows(source, 1, mp, nil)
	require.Nil(t, reader)
	require.True(t, invalid)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Equal(t, before, mp.CurrNB())
	reader, err = NewJSONReaderSnapshotForRows(source, 1, mp, nil)
	require.Nil(t, reader)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestJSONReaderCursorRetainsAndReuses(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	require.NoError(t, builder.Append(nil, true))
	reader, err := builder.Finish()
	require.NoError(t, err)
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	reader.Close()
	path := jsonReaderPath(t, "$.keep")
	require.NoError(t, cursor.ResetQuery(0, *path))
	text, err := cursor.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
	require.NoError(t, cursor.ResetQuery(1, *path))
	require.True(t, cursor.SQLNull())
	require.False(t, cursor.Exists())
	require.Error(t, cursor.ResetQuery(2, *path))
	require.False(t, cursor.SQLNull())
	require.False(t, cursor.Exists())
	require.NoError(t, cursor.ResetQuery(0, *path))
	cursor.Close()
	require.ErrorIs(t, cursor.ResetQuery(0, *path), io.ErrClosedPipe)
}

func TestJSONReaderCursorsRetainClosedParent(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	first, err := reader.OpenCursor()
	require.NoError(t, err)
	defer first.Close()
	second, err := reader.OpenCursor()
	require.NoError(t, err)
	defer second.Close()
	path := *jsonReaderPath(t, "$.keep")
	start := make(chan struct{})
	results := make(chan error, 2)
	var done sync.WaitGroup
	for _, cursor := range []JSONCursor{first, second} {
		done.Add(1)
		go func() {
			defer done.Done()
			<-start
			for range 32 {
				if err := cursor.ResetQuery(0, path); err != nil {
					results <- err
					return
				}
				text, err := cursor.Text()
				if err != nil {
					results <- err
					return
				}
				if text.Text != "1" {
					text.Close()
					results <- io.ErrUnexpectedEOF
					return
				}
				text.Close()
			}
			results <- nil
		}()
	}
	close(start)
	reader.Close()
	done.Wait()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	first.Close()
	require.ErrorIs(t, first.ResetQuery(0, path), io.ErrClosedPipe)
	require.NoError(t, second.ResetQuery(0, path))
}

func TestJSONReaderConstantBatchDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	raw := jsonAdmissionValue(t, `{"keep":1}`)
	source, err := NewConstBytes(types.T_json.ToType(), raw, 1, mp)
	require.NoError(t, err)
	defer source.Free(mp)
	reader, err := NewJSONReaderSnapshotForRows(source, 64, mp, nil)
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, 1, source.Length(), "publication must not resize mutable input")
	require.Equal(t, 64, reader.Length())
	source.GetBytesAt(0)[0] = 0xfd
	path := jsonReaderPath(t, "$.keep")
	for _, row := range []int{0, 63} {
		result, err := reader.Query(row, path)
		require.NoError(t, err)
		text, err := result.Text()
		result.Close()
		require.NoError(t, err)
		require.Equal(t, "1", text.Text)
		text.Close()
	}
	window, err := reader.Window(32, 34)
	require.NoError(t, err)
	defer window.Close()
	selected, err := window.Select([]int64{1, 0, 1}, mp)
	require.NoError(t, err)
	defer selected.Close()
	result, err := selected.Query(2, path)
	require.NoError(t, err)
	defer result.Close()
	text, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
	flat := NewVec(types.T_json.ToType())
	defer flat.Free(mp)
	require.NoError(t, AppendBytes(flat, raw, false, mp))
	_, err = NewJSONReaderSnapshotForRows(flat, 64, mp, nil)
	require.Error(t, err, "flat inputs cannot silently change row domain")
}

func TestJSONReaderSnapshotRejectsTypeBeforeCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1}`), false, mp))
	original := source.typ
	source.typ.Size *= 2
	before := mp.CurrNB()
	reader, err := NewJSONReaderSnapshot(source, mp)
	require.Error(t, err)
	require.Nil(t, reader)
	require.Equal(t, before, mp.CurrNB())
	source.typ = original
	reader, err = NewJSONReaderSnapshot(source, mp)
	require.NoError(t, err)
	reader.Close()
}

type wrappedJSONReader struct{ JSONReader }

func TestJSONReaderRepeatView(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":1}`), false))
	require.NoError(t, builder.Append(jsonAdmissionValue(t, `{"keep":2}`), false))
	require.NoError(t, builder.Append(nil, true))
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	require.True(t, IsPrivateJSONReader(reader))
	require.False(t, IsPrivateJSONReader(&wrappedJSONReader{JSONReader: reader}))
	require.False(t, IsPrivateJSONReader(nil))
	_, err = reader.Repeat(-1, 1)
	require.Error(t, err)
	_, err = reader.Repeat(3, 1)
	require.Error(t, err)
	_, err = reader.Repeat(0, -1)
	require.Error(t, err)

	before := mp.CurrNB()
	repeated, err := reader.Repeat(1, 100_000)
	require.NoError(t, err)
	defer repeated.Close()
	require.True(t, IsPrivateJSONReader(repeated))
	require.Equal(t, before, mp.CurrNB())
	require.Equal(t, 100_000, repeated.Length())
	window, err := repeated.Window(32_000, 32_003)
	require.NoError(t, err)
	defer window.Close()
	selected, err := window.Select([]int64{2, 0}, mp)
	require.NoError(t, err)
	defer selected.Close()
	again, err := selected.Repeat(1, 3)
	require.NoError(t, err)
	defer again.Close()
	nullView, err := reader.Repeat(2, 2)
	require.NoError(t, err)
	require.True(t, func() bool { null, e := nullView.IsNull(1); require.NoError(t, e); return null }())
	nullView.Close()
	empty, err := reader.Repeat(0, 0)
	require.NoError(t, err)
	require.Zero(t, empty.Length())
	empty.Close()

	materialized, err := again.Materialize(mp)
	require.NoError(t, err)
	defer materialized.Free(mp)
	require.Equal(t, 3, materialized.Length())
	cursor, err := again.OpenCursor()
	require.NoError(t, err)
	reader.Close()
	repeated.Close()
	window.Close()
	selected.Close()
	again.Close()
	require.False(t, IsPrivateJSONReader(again))
	result, err := cursor.QueryText(2, []byte("$.keep"), false)
	require.NoError(t, err)
	require.Equal(t, "2", result.Text)
	result.Close()
	cursor.Close()
	_, err = again.Repeat(0, 1)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	for row := 0; row < 3; row++ {
		value := types.DecodeJson(materialized.GetBytesAt(row))
		selectedValue, exists := value.QueryWithExists([]*bytejson.Path{jsonReaderPath(t, "$.keep")})
		require.True(t, exists)
		require.Equal(t, int64(2), selectedValue.GetInt64())
	}
}
