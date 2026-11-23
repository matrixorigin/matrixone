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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAsParamValue(t *testing.T) {
	stored := ParamValue{Value: "7", IsBinaryProtocol: true}
	for _, v := range []any{stored, &stored} {
		got, ok := AsParamValue(v)
		require.True(t, ok)
		require.Equal(t, stored, got)
		// The result is a copy: adjusting it leaves the storage alone.
		got.Value = "changed"
		require.Equal(t, "7", stored.Value)
	}
	for _, v := range []any{nil, (*ParamValue)(nil), "7", int64(7)} {
		_, ok := AsParamValue(v)
		require.False(t, ok, "%T", v)
	}
}

func TestSetParamValueAt(t *testing.T) {
	var store ParamValueStore
	values := store.Reset(2)
	SetParamValueAt(values, 0, ParamValue{Value: "a"})
	require.Equal(t, "a", store.Ref(0).Value, "a pointer element is written through")
	require.Same(t, store.Ref(0), values[0])

	plain := []any{ParamValue{Value: "x"}}
	SetParamValueAt(plain, 0, ParamValue{Value: "y"})
	require.Equal(t, ParamValue{Value: "y"}, plain[0])
}

func TestParamValueStoreReuse(t *testing.T) {
	var store ParamValueStore
	values := store.Reset(3)
	*store.Ref(1) = ParamValue{Value: "old", HasRuntimeType: true}

	// A new execution reuses the storage and starts from zero values.
	again := store.Reset(3)
	require.Same(t, &values[0], &again[0])
	got, ok := AsParamValue(again[1])
	require.True(t, ok)
	require.Equal(t, ParamValue{}, got)

	// Once large enough, readying the store for an execution allocates nothing.
	allocs := testing.AllocsPerRun(100, func() {
		values := store.Reset(3)
		for i := range values {
			*store.Ref(i) = ParamValue{IsBinaryProtocol: true}
		}
	})
	require.Zero(t, allocs)
	require.Len(t, store.Reset(1), 1)
	require.Len(t, store.Reset(5), 5)
}

func TestCopyParamValuesDetachesFromStorage(t *testing.T) {
	var store ParamValueStore
	values := store.Reset(2)
	*store.Ref(0) = ParamValue{Value: "a"}
	values[1] = "raw"
	snapshot := CopyParamValues(values)
	*store.Ref(0) = ParamValue{Value: "rewritten"}
	got, ok := AsParamValue(snapshot[0])
	require.True(t, ok)
	require.Equal(t, "a", got.Value, "a snapshot does not follow the storage")
	require.Equal(t, "raw", snapshot[1])
	require.Nil(t, CopyParamValues(nil))
}
