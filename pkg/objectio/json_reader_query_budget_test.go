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

package objectio

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestJSONCacheReaderDirectTextCopiesToAccountedResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	long := strings.Repeat("x", 8192)
	doc, err := types.ParseStringToByteJson(`{"keep":"text","number":123,"long":"` + long + `","arr":[1,2]}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
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
	reader, err := OpenJSONReader(data, mp, nil, admission)
	require.NoError(t, err)
	defer reader.Close()
	direct, ok := reader.(vector.JSONDirectTextReader)
	require.True(t, ok)
	output := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer output.Free()
	writer := vector.MustFunctionResult[types.Varlena](output)
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
	parse := func(path string) bytejson.Path {
		t.Helper()
		parsed, err := bytejson.ParseJsonPath(path)
		require.NoError(t, err)
		return parsed
	}
	require.NoError(t, output.PreExtendAndReset(1))
	selected, appended, err := direct.QueryTextAtPathTo(0, parse("$.keep"), false, 3, writer)
	selected.Close()
	require.False(t, appended)
	require.ErrorIs(t, err, want)
	selected, appended, err = direct.QueryTextAtPathTo(0, parse("$.arr[*]"), false, 512, writer)
	selected.Close()
	require.False(t, appended)
	require.ErrorIs(t, err, want)
	require.NoError(t, output.PreExtendAndReset(1))
	selected, appended, err = direct.QueryTextAtPathTo(0, parse("$.long"), false, -1, writer)
	selected.Close()
	require.NoError(t, err)
	require.True(t, appended)
	reader.Close()
	require.Zero(t, live)
	require.Equal(t, long, string(output.GetResultVector().GetBytesAt(0)))
}

func TestJSONCacheReaderQueryRespectsBudget(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	doc, err := types.ParseStringToByteJson(`["` + strings.Repeat("x", 8192) + `","y"]`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
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
	reader, err := OpenJSONReader(data, mp, nil, admission)
	require.NoError(t, err)
	defer reader.Close()
	_, ok := reader.(*cacheJSONReader)
	require.True(t, ok)
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	path, err := bytejson.ParseJsonPath("$[*]")
	require.NoError(t, err)

	limit = live + vector.JSONReaderMetadataCapacity(reflect.TypeFor[cacheJSONQueryResult]().Size())
	result, err := reader.Query(0, &path)
	if result != nil {
		result.Close()
	}
	require.ErrorIs(t, err, want, "result backing must be admitted before allocation")

	limit = live
	require.ErrorIs(t, cursor.ResetQuery(0, path), want, "cursor reset must admit result backing")
	single, err := bytejson.ParseJsonPath("$[0]")
	require.NoError(t, err)
	_, err = reader.QueryTextAtPath(0, single, false)
	require.ErrorIs(t, err, want, "owned output text must be admitted before copying")
	limit = live + vector.JSONReaderMetadataCapacity(reflect.TypeFor[cacheJSONQueryResult]().Size())
	result, err = reader.Query(0, &single)
	require.NoError(t, err)
	defer result.Close()
	limit = live
	text, err := result.Text()
	text.Close()
	require.ErrorIs(t, err, want, "result text must be admitted before copying")
}

func TestJSONCacheReaderQueryBudgetLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := vector.NewVec(types.T_json.ToType())
	defer source.Free(mp)
	value := strings.Repeat("x", 128)
	doc, err := types.ParseStringToByteJson(`["` + value + `","y"]`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
	data := privateJSONColumnForTest(t, source)
	defer data.Release()
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
	reader, err := OpenJSONReader(data, mp, nil, admission)
	require.NoError(t, err)
	defer reader.Close()
	cursor, err := reader.OpenCursor()
	require.NoError(t, err)
	defer cursor.Close()
	path, err := bytejson.ParseJsonPath("$[*]")
	require.NoError(t, err)
	baseline := live
	first, err := reader.Query(0, &path)
	require.NoError(t, err)
	defer first.Close()
	afterFirst := live
	require.Greater(t, afterFirst-baseline, vector.JSONReaderMetadataCapacity(reflect.TypeFor[cacheJSONQueryResult]().Size()))
	second, err := reader.Query(0, &path)
	require.NoError(t, err)
	defer second.Close()
	require.Greater(t, live, afterFirst)
	first.Close()
	require.Equal(t, afterFirst, live, "each result owns its own backing charge")
	require.True(t, second.Exists())

	require.NoError(t, cursor.ResetQuery(0, path))
	require.Greater(t, live, afterFirst)
	single, err := bytejson.ParseJsonPath("$[0]")
	require.NoError(t, err)
	require.NoError(t, cursor.ResetQuery(0, single))
	require.Equal(t, afterFirst, live, "reset releases the prior merged result")
	failNext = true
	require.ErrorIs(t, cursor.ResetQuery(0, path), want)
	require.Equal(t, afterFirst, live)
	require.Equal(t, 1, partialReleases)

	selected, err := reader.QueryTextAtPath(0, single, false)
	require.NoError(t, err)
	defer selected.Close()
	alias := selected
	text, err := second.Text()
	require.NoError(t, err)
	defer text.Close()
	second.Close()
	cursor.Close()
	reader.Close()
	require.Equal(t, value, selected.Text)
	require.Contains(t, text.Text, value)
	require.Positive(t, live, "independent output remains charged after reader close")
	selected.Close()
	alias.Close()
	text.Close()
	require.Zero(t, live)
}
