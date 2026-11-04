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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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

func TestGenColumnsFromDefsSequenceLimit(t *testing.T) {
	defs := make([]engine.TableDef, 0, objectio.SEQNUM_UPPER+2)
	// The last ordinary slot is hidden; a hidden column still consumes capacity.
	for i := 0; i < objectio.SEQNUM_UPPER; i++ {
		defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{Name: fmt.Sprintf("c%d", i), Type: types.T_int32.ToType(), IsHidden: i == objectio.SEQNUM_UPPER-1}})
	}
	defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{Name: Row_ID, Type: types.T_Rowid.ToType()}}, &engine.CommentDef{Comment: "not a column"})
	cols, err := GenColumnsFromDefs(0, "wide", "test", 1, 1, defs)
	require.NoError(t, err)
	require.Len(t, cols, objectio.SEQNUM_UPPER+1)
	require.Equal(t, uint16(objectio.SEQNUM_UPPER-1), cols[objectio.SEQNUM_UPPER-1].Seqnum)
	// Existing rowid treatment is preserved; it does not occupy an ordinary slot.
	require.Equal(t, Row_ID, cols[len(cols)-1].Name)
	extra := &engine.AttributeDef{Attr: engine.Attribute{Name: "extra", Type: types.T_int32.ToType(), ID: 42, Seqnum: 17}}
	defs = append(defs, extra)
	first := defs[0].(*engine.AttributeDef)
	first.Attr.ID = 99
	first.Attr.Seqnum = 77
	_, err = GenColumnsFromDefs(0, "wide", "test", 1, 1, defs)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyFields), "%v", err)
	require.Equal(t, uint64(99), first.Attr.ID)
	require.Equal(t, uint16(77), first.Attr.Seqnum)
	require.Equal(t, uint64(42), extra.Attr.ID)
	require.Equal(t, uint16(17), extra.Attr.Seqnum)
}
