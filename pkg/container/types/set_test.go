// Copyright 2021 Matrix Origin
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

package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSetValues(t *testing.T) {
	values, err := NormalizeSetValues([]string{"red", "blue   ", "GREEN"})
	require.NoError(t, err)
	require.Equal(t, []string{"red", "blue", "GREEN"}, values)

	_, err = NormalizeSetValues([]string{"red", "Red"})
	require.Error(t, err)

	_, err = NormalizeSetValues([]string{"red,blue"})
	require.Error(t, err)
}

func TestParseSet(t *testing.T) {
	tests := []struct {
		name    string
		def     string
		input   string
		want    uint64
		wantErr bool
	}{
		{name: "single member", def: "red,blue,green", input: "blue", want: 2},
		{name: "multiple members", def: "red,blue,green", input: "green,red", want: 5},
		{name: "deduplicate and normalize", def: "red,blue,green", input: "blue,RED,blue", want: 3},
		{name: "numeric mask", def: "red,blue,green", input: "3", want: 3},
		{name: "numeric string member wins", def: "1,2,4", input: "1", want: 1},
		{name: "empty set", def: "red,blue", input: "", want: 0},
		{name: "zero value", def: "red,blue", input: "0", want: 0},
		{name: "duplicate input members", def: "red,blue,green", input: "red,red", want: 1},
		{name: "invalid member", def: "red,blue", input: "green", wantErr: true},
		{name: "overflow mask", def: "red,blue", input: "8", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSet(tc.def, tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseSetIndex(t *testing.T) {
	got, err := ParseSetIndex("red,blue,green", 5)
	require.NoError(t, err)
	require.Equal(t, "red,green", got)

	got, err = ParseSetIndex("选项1,选项2", 0)
	require.NoError(t, err)
	require.Equal(t, "", got)

	_, err = ParseSetIndex("red,blue", 4)
	require.Error(t, err)
}

func TestGetSetDescriptorCache(t *testing.T) {
	first, err := getSetDescriptor("red,blue")
	require.NoError(t, err)

	second, err := getSetDescriptor("red,blue")
	require.NoError(t, err)

	require.Same(t, first, second)
}

func TestNormalizeSetValuesMaxMembers(t *testing.T) {
	values := make([]string, 0, MaxSetMembers+1)
	for i := 0; i < MaxSetMembers+1; i++ {
		values = append(values, strings.Repeat("a", i+1))
	}
	_, err := NormalizeSetValues(values)
	require.Error(t, err)
}
