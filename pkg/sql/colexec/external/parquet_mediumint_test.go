// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetMediumIntEnforcesLogicalBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		node    parquet.Node
		values  []parquet.Value
		target  types.T
		want    any
		wantErr string
	}{
		{
			name:   "signed exact boundaries",
			node:   parquet.Int(32),
			values: []parquet.Value{parquet.Int32Value(-1 << 23), parquet.Int32Value((1 << 23) - 1)},
			target: types.T_int32,
			want:   []int32{-1 << 23, (1 << 23) - 1},
		},
		{
			name:    "signed overflow through plain integer page",
			node:    parquet.Int(32),
			values:  []parquet.Value{parquet.Int32Value(1 << 23)},
			target:  types.T_int32,
			wantErr: "overflows MEDIUMINT",
		},
		{
			name:   "unsigned exact upper boundary",
			node:   parquet.Uint(32),
			values: []parquet.Value{parquet.ValueOf(uint32(0)), parquet.ValueOf(uint32((1 << 24) - 1))},
			target: types.T_uint32,
			want:   []uint32{0, (1 << 24) - 1},
		},
		{
			name:    "unsigned overflow through plain integer page",
			node:    parquet.Uint(32),
			values:  []parquet.Value{parquet.ValueOf(uint32(1 << 24))},
			target:  types.T_uint32,
			wantErr: "overflows MEDIUMINT UNSIGNED",
		},
		{
			name:    "signed overflow through string page",
			node:    parquet.String(),
			values:  []parquet.Value{parquet.ByteArrayValue([]byte("8388608"))},
			target:  types.T_int32,
			wantErr: "overflows MEDIUMINT",
		},
		{
			name:    "unsigned overflow after rounded float conversion",
			node:    parquet.Leaf(parquet.DoubleType),
			values:  []parquet.Value{parquet.DoubleValue(16777215.5)},
			target:  types.T_uint32,
			wantErr: "overflows MEDIUMINT UNSIGNED",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			file, page := writeDictAndGetPage(t, tc.node, tc.values)
			typ := types.New(tc.target, 24, -1)
			vec := vector.NewVec(typ)
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var handler ParquetHandler
			mapper := handler.getMapper(file.Root().Column("c"), plan.Type{
				Id:          int32(tc.target),
				Width:       24,
				NotNullable: true,
			})
			require.NotNil(t, mapper)
			err := mapper.mapping(page, proc, vec)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Zero(t, vec.Length(), "an invalid Parquet value must not append output")
				return
			}
			require.NoError(t, err)
			requireParquetScalarResult(t, vec, tc.want, 0)
		})
	}
}
