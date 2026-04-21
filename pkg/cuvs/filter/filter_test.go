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

package filter

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Data contract with the C++ filter.hpp parser. Values MUST match the enum in
// cgo/cuvs/filter.hpp; if a future commit changes these, the C++ side will
// silently mis-parse column metadata.
func TestColTypeValuesMatchCpp(t *testing.T) {
	require.Equal(t, ColType(0), ColTypeInt32)
	require.Equal(t, ColType(1), ColTypeInt64)
	require.Equal(t, ColType(2), ColTypeFloat32)
	require.Equal(t, ColType(3), ColTypeFloat64)
	require.Equal(t, ColType(4), ColTypeUint64)
}

func TestElemSize(t *testing.T) {
	cases := []struct {
		name string
		t    ColType
		want uint32
	}{
		{"int32", ColTypeInt32, 4},
		{"int64", ColTypeInt64, 8},
		{"float32", ColTypeFloat32, 4},
		{"float64", ColTypeFloat64, 8},
		{"uint64", ColTypeUint64, 8},
		{"unknown", ColType(99), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.t.ElemSize())
		})
	}
}

// The table function marshals []ColumnMeta via sonic/json.Marshal and hands
// the resulting string to gpu_<idx>_set_filter_columns, which then parses it
// via matrixone::parse_filter_col_meta (cgo/cuvs/filter.hpp). This test
// guards the JSON shape against accidental tag-rename / field-reorder.
func TestColumnMetaJSONShape(t *testing.T) {
	cols := []ColumnMeta{
		{Name: "price", TypeOid: ColTypeFloat32},
		{Name: "cat", TypeOid: ColTypeInt64},
	}
	buf, err := json.Marshal(cols)
	require.NoError(t, err)

	// The C++ parser reads key "name" (string) and key "type" (int).
	// Keep this string literal — drift here is silent at build time but
	// breaks parse_filter_col_meta at runtime.
	want := `[{"name":"price","type":2},{"name":"cat","type":1}]`
	require.JSONEq(t, want, string(buf))
}

// Round-trip through JSON preserves all fields.
func TestColumnMetaJSONRoundTrip(t *testing.T) {
	src := []ColumnMeta{
		{Name: "a", TypeOid: ColTypeInt32},
		{Name: "b", TypeOid: ColTypeUint64},
	}
	buf, err := json.Marshal(src)
	require.NoError(t, err)

	var dst []ColumnMeta
	require.NoError(t, json.Unmarshal(buf, &dst))
	require.Equal(t, src, dst)
}
