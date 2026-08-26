// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestResolveVariableFunc(t *testing.T) {
	jstr := `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}, 
	"kmeans_max_iteration":{"t":"I", "v":4}, 
	"ivf_threads_build":{"t":"I", "v":23},
	"action":{"t":"S", "v":"action string"},
	"float":{"t":"F", "v":23.3}
	}, "action": "xxx"}`

	m, err := NewMetadataFromJson(jstr)
	require.Nil(t, err)

	f := m.ResolveVariableFunc

	v1, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, v1, any(float64(10)))

	v2, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, v2, any(int64(4)))

	v3, err := f("ivf_threads_build", false, false)
	require.Nil(t, err)
	require.Equal(t, v3, any(int64(23)))

	v4, err := f("float", false, false)
	require.Nil(t, err)
	require.Equal(t, v4, any(float64(23.3)))

	v5, err := f("action", false, false)
	require.Nil(t, err)
	require.Equal(t, v5, any("action string"))
}

// TestResolveVariableWithSessionDefaults pins the idxcron reindex-hook fix: a
// captured Metadata holds only algo build knobs and NOT sql_mode, so the plain
// ResolveVariableFunc errors on it — which aborts every reindex once the rebuild
// INSERT resolves sql_mode (RejectZeroTemporalWritePolicy, #25438). The
// session-defaulting resolver must instead return the permissive background
// default ("") for sql_mode, while keeping algo vars answered exactly and an
// uncaptured algo var still ERRORING (so a real build-config bug surfaces).
func TestResolveVariableWithSessionDefaults(t *testing.T) {
	// A realistic captured blob: algo knobs only, deliberately no sql_mode.
	jstr := `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
	"kmeans_max_iteration":{"t":"I", "v":4}}}`

	m, err := NewMetadataFromJson(jstr)
	require.Nil(t, err)

	// Baseline: the plain resolver errors on the uncaptured sql_mode — this is
	// exactly the "key sql_mode not found" that broke idxcron reindex.
	_, err = m.ResolveVariableFunc("sql_mode", true, false)
	require.NotNil(t, err)

	// Fixed resolver: sql_mode falls back to the permissive background default "".
	v, err := m.ResolveVariableWithSessionDefaults("sql_mode", true, false)
	require.Nil(t, err)
	require.Equal(t, any(""), v)

	// Case-insensitive on the whitelisted var name.
	v, err = m.ResolveVariableWithSessionDefaults("SQL_MODE", true, false)
	require.Nil(t, err)
	require.Equal(t, any(""), v)

	// lock_wait_timeout is enumerated with a nil default: its callers fall back to
	// their own context-aware default on nil, so we must NOT invent a value.
	v, err = m.ResolveVariableWithSessionDefaults("lock_wait_timeout", true, false)
	require.Nil(t, err)
	require.Nil(t, v)

	// FAIL FAST on an un-enumerated session var: rather than silently defaulting to
	// nil, a newly-plumbed dependency surfaces loudly, named, so it gets a
	// deliberate default. (This is design B.)
	_, err = m.ResolveVariableWithSessionDefaults("some_new_session_var", true, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "some_new_session_var")

	// Captured algo vars are answered exactly as the strict resolver does.
	v, err = m.ResolveVariableWithSessionDefaults("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, any(float64(10)), v)

	// Strictness preserved for CAPTURED vars: a captured-but-malformed algo knob
	// still errors, so a genuine build blob bug surfaces loudly. (An uncaptured
	// name is treated as a session var above; only names present in cfg are strict.)
	bad, err := NewMetadataFromJson(`{"cfg":{"kmeans_train_percent":{"v":10}}}`) // missing type
	require.Nil(t, err)
	_, err = bad.ResolveVariableWithSessionDefaults("kmeans_train_percent", false, false)
	require.NotNil(t, err)
}

func TestMetadataWriter(t *testing.T) {

	writer := NewMetadataWriter()
	writer.AddFloat("kmeans_train_percent", 10)
	writer.AddInt("kmeans_max_iteration", 20)
	writer.AddString("string_param", "hello")
	writer.AddFloat("float_param", 44.56)

	js, err := writer.Marshal()
	require.Nil(t, err)

	bj, err := bytejson.ParseFromString(string(js))
	require.Nil(t, err)

	bytes, err := bj.Marshal()
	require.Nil(t, err)

	m, err := NewMetadata(bytes)
	require.Nil(t, err)

	f := m.ResolveVariableFunc

	v1, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, v1, any(float64(10)))

	v2, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, v2, any(int64(20)))

	v4, err := f("float_param", false, false)
	require.Nil(t, err)
	require.Equal(t, v4, any(float64(44.56)))

	v5, err := f("string_param", false, false)
	require.Nil(t, err)
	require.Equal(t, v5, any("hello"))

	err = m.Modify("kmeans_train_percent", 0.2)
	require.Nil(t, err)

	v6, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, any(float64(0.2)), v6)

	err = m.Modify("string_param", "world")
	require.Nil(t, err)

	v7, err := f("string_param", false, false)
	require.Nil(t, err)
	require.Equal(t, any("world"), v7)

	err = m.Modify("kmeans_max_iteration", 33)
	require.Nil(t, err)

	v8, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, any(int64(33)), v8)

}

func TestMetadataError(t *testing.T) {

	_, err := NewMetadata(nil)
	require.NotNil(t, err)

}
func TestMetadataFromJsonError(t *testing.T) {
	_, err := NewMetadataFromJson("")
	require.NotNil(t, err)

	_, err = NewMetadataFromJson("{\"a:3}")
	require.NotNil(t, err)

}

func TestMetadataResolveError(t *testing.T) {

	{
		// key not found
		var bj bytejson.ByteJson
		bytes, err := bj.Marshal()
		require.Nil(t, err)

		m, err := NewMetadata(bytes)
		require.Nil(t, err)

		_, err = m.ResolveVariableFunc("a", false, false)
		require.NotNil(t, err)
	}

	{
		// invalid json path
		var bj bytejson.ByteJson
		bytes, err := bj.Marshal()
		require.Nil(t, err)

		m, err := NewMetadata(bytes)
		require.Nil(t, err)

		_, err = m.ResolveVariableFunc("[", false, false)
		require.NotNil(t, err)
	}

	{
		// type is nill
		//jstr := `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`
		jstr := `{"cfg":{"kmeans_train_percent":{"v":10}}}`

		m, err := NewMetadataFromJson(jstr)
		require.Nil(t, err)

		_, err = m.ResolveVariableFunc("kmeans_train_percent", false, false)
		require.NotNil(t, err)

	}

	{
		// value is nill
		jstr := `{"cfg":{"kmeans_train_percent":{"t":"F"}}}`

		m, err := NewMetadataFromJson(jstr)
		require.Nil(t, err)

		_, err = m.ResolveVariableFunc("kmeans_train_percent", false, false)
		require.NotNil(t, err)

	}

	{
		// invalid type
		jstr := `{"cfg":{"kmeans_train_percent":{"t":"Y", "v": 9}}}`

		m, err := NewMetadataFromJson(jstr)
		require.Nil(t, err)

		_, err = m.ResolveVariableFunc("kmeans_train_percent", false, false)
		require.NotNil(t, err)

	}

}

func TestMetadataModifyError(t *testing.T) {

	{
		// key not found
		var bj bytejson.ByteJson
		bytes, err := bj.Marshal()
		require.Nil(t, err)

		m, err := NewMetadata(bytes)
		require.Nil(t, err)

		err = m.Modify("[", "v")
		require.NotNil(t, err)
	}

	{
		// invalid value type
		var bj bytejson.ByteJson
		bytes, err := bj.Marshal()
		require.Nil(t, err)

		m, err := NewMetadata(bytes)
		require.Nil(t, err)

		err = m.Modify("a", bj)
		require.NotNil(t, err)
	}

}
