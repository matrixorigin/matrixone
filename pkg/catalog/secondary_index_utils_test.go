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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsIndexAsync(t *testing.T) {

	var (
		json string
		err  error
		ok   bool
	)

	json = ""
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = "{}"
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = `{"async": "true"}`
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, true)

	json = `{"async": 1}`
	_, err = IsIndexAsync(json)
	require.NotNil(t, err)
}

func TestIndexPrefixLengthsFromParamsWithError(t *testing.T) {
	tests := []struct {
		name      string
		params    string
		want      map[string]int
		wantError bool
	}{
		{
			name:   "empty params",
			params: "",
		},
		{
			name:   "no prefix lengths",
			params: `{"lists":"1"}`,
		},
		{
			name:   "valid prefix lengths",
			params: `{"prefix_lengths":"b:20,t:10"}`,
			want: map[string]int{
				"b": 20,
				"t": 10,
			},
		},
		{
			name:      "invalid json",
			params:    "{bad json",
			wantError: true,
		},
		{
			name:      "missing separator",
			params:    `{"prefix_lengths":"t"}`,
			wantError: true,
		},
		{
			name:      "non numeric length",
			params:    `{"prefix_lengths":"t:abc"}`,
			wantError: true,
		},
		{
			name:      "non positive length",
			params:    `{"prefix_lengths":"t:0"}`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IndexPrefixLengthsFromParamsWithError(tt.params)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
