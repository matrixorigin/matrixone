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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestGenCreateColumnTuplesPreservesUnsignedFlag(t *testing.T) {
	tests := []struct {
		name string
		typ  types.Type
		want int8
	}{
		{name: "signed_tinyint", typ: types.T_int8.ToType()},
		{name: "unsigned_tinyint", typ: types.T_uint8.ToType(), want: 1},
		{name: "signed_smallint", typ: types.T_int16.ToType()},
		{name: "unsigned_smallint", typ: types.T_uint16.ToType(), want: 1},
		{name: "signed_int", typ: types.T_int32.ToType()},
		{name: "unsigned_int", typ: types.T_uint32.ToType(), want: 1},
		{name: "signed_bigint", typ: types.T_int64.ToType()},
		{name: "unsigned_bigint", typ: types.T_uint64.ToType(), want: 1},
		{name: "bit", typ: types.T_bit.ToType()},
		{name: "decimal", typ: types.T_decimal128.ToType()},
	}

	defs := make([]engine.TableDef, 0, len(tests))
	for _, test := range tests {
		defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{
			Name: test.name,
			Type: test.typ,
		}})
	}

	cols, err := GenColumnsFromDefs(7, "unsigned_flags", "issue_27661", 9, 8, defs)
	require.NoError(t, err)
	require.Len(t, cols, len(tests)+1)

	mp := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	bat, err := GenCreateColumnTuples(cols, mp, packer)
	require.NoError(t, err)
	defer bat.Clean(mp)

	flags := vector.MustFixedColWithTypeCheck[int8](bat.Vecs[MO_COLUMNS_ATT_IS_UNSIGNED_IDX])
	for i, test := range tests {
		require.Equal(t, test.want, cols[i].IsUnsigned, test.name)
		require.Equal(t, test.want, flags[i], test.name)
	}
	require.Equal(t, Row_ID, cols[len(tests)].Name)
	require.Zero(t, cols[len(tests)].IsUnsigned)
	require.Zero(t, flags[len(tests)])
}
