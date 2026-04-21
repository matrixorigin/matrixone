//go:build gpu

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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	"github.com/stretchr/testify/require"
)

// mockFilterBuilder captures SetFilterColumns / AddFilterChunk calls so tests
// can assert the bytes the table function hands to CGo without actually
// spinning up a GPU index.
type mockFilterBuilder struct {
	metaJSON string
	chunks   []mockChunk
}

type mockChunk struct {
	colIdx uint32
	data   []byte
	nrows  uint64
}

func (m *mockFilterBuilder) SetFilterColumns(colMetaJSON string) {
	m.metaJSON = colMetaJSON
}

func (m *mockFilterBuilder) AddFilterChunk(colIdx uint32, data []byte, nrows uint64) error {
	// Copy: the helper hands us slices aliased over the original value which
	// goes out of scope after the loop iteration.
	cp := make([]byte, len(data))
	copy(cp, data)
	m.chunks = append(m.chunks, mockChunk{colIdx: colIdx, data: cp, nrows: nrows})
	return nil
}

func TestInitFilterColumnsSerialisesJSON(t *testing.T) {
	mb := &mockFilterBuilder{}
	cols := []cuvsfilter.ColumnMeta{
		{Name: "price", TypeOid: cuvsfilter.ColTypeFloat32},
		{Name: "cat", TypeOid: cuvsfilter.ColTypeInt64},
	}
	require.NoError(t, initFilterColumns(mb, cols))
	require.JSONEq(t,
		`[{"name":"price","type":2},{"name":"cat","type":1}]`,
		mb.metaJSON,
	)
}

func TestInitFilterColumnsEmptyIsNoop(t *testing.T) {
	mb := &mockFilterBuilder{}
	require.NoError(t, initFilterColumns(mb, nil))
	require.Equal(t, "", mb.metaJSON)
}

// Builds a one-row *vector.Vector holding `val` of the given types.T, for use
// as a stand-in table-function argVec in appendFilterRow tests.
func singleRowVec[T any](t *testing.T, mp *mpool.MPool, oid types.T, val T) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_int64.ToType()) // placeholder, overwritten below
	v.ResetWithSameType()
	var width int32 = 8
	switch oid {
	case types.T_int32, types.T_float32:
		width = 4
	}
	v.ResetWithNewType(&types.Type{Oid: oid, Size: width, Width: width})
	require.NoError(t, vector.AppendFixed(v, val, false, mp))
	return v
}

// Exercises every supported FilterColType: verify the byte payload written to
// the builder matches the native in-memory representation of the value.
func TestAppendFilterRowAllTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	baseOffset := 3 // matches cagra_create / ivfpq_create arg layout

	cols := []cuvsfilter.ColumnMeta{
		{Name: "a", TypeOid: cuvsfilter.ColTypeInt32},
		{Name: "b", TypeOid: cuvsfilter.ColTypeInt64},
		{Name: "c", TypeOid: cuvsfilter.ColTypeFloat32},
		{Name: "d", TypeOid: cuvsfilter.ColTypeFloat64},
		{Name: "e", TypeOid: cuvsfilter.ColTypeUint64},
	}

	// Fill argVecs with 3 dummy "base" slots + 5 filter slots.
	argVecs := make([]*vector.Vector, baseOffset+len(cols))
	for i := 0; i < baseOffset; i++ {
		argVecs[i] = singleRowVec(t, mp, types.T_int64, int64(0))
	}
	argVecs[baseOffset+0] = singleRowVec(t, mp, types.T_int32, int32(-42))
	argVecs[baseOffset+1] = singleRowVec(t, mp, types.T_int64, int64(0x1122334455667788))
	argVecs[baseOffset+2] = singleRowVec(t, mp, types.T_float32, float32(3.14))
	argVecs[baseOffset+3] = singleRowVec(t, mp, types.T_float64, float64(2.718281828))
	argVecs[baseOffset+4] = singleRowVec(t, mp, types.T_uint64, uint64(0xDEADBEEFCAFEBABE))

	mb := &mockFilterBuilder{}
	require.NoError(t, appendFilterRow(mb, cols, argVecs, baseOffset, 0))

	require.Len(t, mb.chunks, 5)

	// Native little-endian byte patterns on x86_64.
	require.Equal(t, uint32(0), mb.chunks[0].colIdx)
	require.Equal(t, []byte{0xD6, 0xFF, 0xFF, 0xFF}, mb.chunks[0].data) // -42 as int32 LE
	require.Equal(t, uint64(1), mb.chunks[0].nrows)

	require.Equal(t, uint32(1), mb.chunks[1].colIdx)
	require.Equal(t,
		[]byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11},
		mb.chunks[1].data)

	require.Equal(t, uint32(2), mb.chunks[2].colIdx)
	require.Len(t, mb.chunks[2].data, 4)

	require.Equal(t, uint32(3), mb.chunks[3].colIdx)
	require.Len(t, mb.chunks[3].data, 8)

	require.Equal(t, uint32(4), mb.chunks[4].colIdx)
	require.Equal(t,
		[]byte{0xBE, 0xBA, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE},
		mb.chunks[4].data)
}

// Row with a NULL filter-column value must reject the whole row — otherwise
// the filter buffer would drift out of step with the vector insertions.
func TestAppendFilterRowRejectsNull(t *testing.T) {
	mp := mpool.MustNewZero()

	v := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(v, int64(0), true, mp)) // null
	argVecs := []*vector.Vector{nil, nil, nil, v}

	mb := &mockFilterBuilder{}
	cols := []cuvsfilter.ColumnMeta{
		{Name: "a", TypeOid: cuvsfilter.ColTypeInt64},
	}
	err := appendFilterRow(mb, cols, argVecs, 3, 0)
	require.Error(t, err)
	require.Empty(t, mb.chunks)
}

func TestValidateFilterArgCount(t *testing.T) {
	cols := []cuvsfilter.ColumnMeta{{Name: "a", TypeOid: cuvsfilter.ColTypeInt64}}

	// Enough args.
	argVecs := make([]*vector.Vector, 4) // 3 base + 1 filter
	require.NoError(t, validateFilterArgCount(argVecs, 3, cols))

	// Too few.
	argVecs = make([]*vector.Vector, 3)
	require.Error(t, validateFilterArgCount(argVecs, 3, cols))
}
