// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"reflect"
	"testing"

	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/require"
)

func TestConfigDataCopyIsolation(t *testing.T) {
	source := &ConfigData{Content: map[string]*ConfigItem{
		"key": {Name: "name", CurrentValue: "current", DefaultValue: "default", Internal: "true",
			XXX_unrecognized: []byte{16, 1}},
		"nil": nil,
	}, XXX_unrecognized: []byte{16, 2}}
	// Exercise the real hook dispatch rather than calling only the new method.
	first := deepcopy.Copy(source).(*ConfigData)
	second := deepcopy.Copy(source).(*ConfigData)
	require.Equal(t, source, first)
	require.Equal(t, source, second)
	first.Content["key"].CurrentValue = "first"
	first.Content["key"].XXX_unrecognized[1] = 3
	first.XXX_unrecognized[1] = 4
	delete(first.Content, "nil")
	require.Equal(t, "current", source.Content["key"].CurrentValue)
	require.Equal(t, byte(1), source.Content["key"].XXX_unrecognized[1])
	require.Equal(t, byte(2), source.XXX_unrecognized[1])
	require.Contains(t, source.Content, "nil")
	require.Nil(t, source.Content["nil"])
	require.Equal(t, source, second)

	source.Content["key"].Name = "updated"
	source.Content["key"].DefaultValue = "updated-default"
	source.Content["key"].Internal = "false"
	source.Content["key"].XXX_unrecognized[1] = 5
	source.XXX_unrecognized[1] = 6
	source.Content["new"] = &ConfigItem{Name: "new"}
	require.Equal(t, "name", second.Content["key"].Name)
	require.Equal(t, "default", second.Content["key"].DefaultValue)
	require.Equal(t, "true", second.Content["key"].Internal)
	require.Equal(t, byte(1), second.Content["key"].XXX_unrecognized[1])
	require.Equal(t, byte(2), second.XXX_unrecognized[1])
	require.NotContains(t, second.Content, "new")
}

func TestConfigDataCopyEmptyContainers(t *testing.T) {
	var nilConfig *ConfigData
	require.Equal(t, nilConfig, deepcopy.Copy(nilConfig))
	for _, source := range []*ConfigData{
		{},
		{Content: map[string]*ConfigItem{}},
		{Content: map[string]*ConfigItem{"nil": nil}},
		{Content: map[string]*ConfigItem{"empty": {XXX_unrecognized: make([]byte, 0, 4)}},
			XXX_unrecognized: make([]byte, 0, 8)},
	} {
		result := deepcopy.Copy(source).(*ConfigData)
		require.Equal(t, source, result)
		require.Equal(t, cap(source.XXX_unrecognized), cap(result.XXX_unrecognized))
		if item := source.Content["empty"]; item != nil {
			require.Equal(t, cap(item.XXX_unrecognized), cap(result.Content["empty"].XXX_unrecognized))
		}
	}
}

// A new mutable protobuf field requires extending the typed copy. Scalar
// additions remain covered by the struct value copy without another field list.
func TestConfigDataCopyMutableFields(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[ConfigData](), reflect.TypeFor[ConfigItem]()} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			switch field.Type.Kind() {
			case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface, reflect.Array, reflect.Struct:
				if field.Type.Kind() == reflect.Struct && field.Type.NumField() == 0 {
					continue
				}
				if typ == reflect.TypeFor[ConfigData]() && field.Name == "Content" {
					require.Equal(t, reflect.TypeFor[map[string]*ConfigItem](), field.Type)
					continue
				}
				if field.Name == "XXX_unrecognized" {
					require.Equal(t, reflect.TypeFor[[]byte](), field.Type)
					continue
				}
				t.Errorf("%s.%s requires independent copy ownership", typ.Name(), field.Name)
			}
		}
	}
}
