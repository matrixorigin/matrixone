// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_aes(t *testing.T) {
	AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { AesKey = "" }()

	data := "test ase"
	encData, err := AesCFBEncode([]byte(data))
	assert.NoError(t, err)
	decData, err := AesCFBDecode(context.Background(), encData)
	assert.NoError(t, err)
	assert.Equal(t, data, decData)
}

func Test_appendByte(t *testing.T) {
	type args struct {
		buf []byte
		d   byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, d: 'a'},
			want: []byte{'a'},
		},
		{
			args: args{buf: []byte{'a'}, d: 'b'},
			want: []byte{'a', 'b'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendByte(tt.args.buf, tt.args.d), "appendByte(%v, %v)", tt.args.buf, tt.args.d)
		})
	}
}

func Test_appendBytes(t *testing.T) {
	type args struct {
		buf  []byte
		data []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, data: []byte{}},
			want: []byte{},
		},
		{
			args: args{buf: []byte{}, data: []byte{'a', 'b', 'c'}},
			want: []byte{'a', 'b', 'c'},
		},
		{
			args: args{buf: []byte{'a', 'b'}, data: []byte{'c', 'd'}},
			want: []byte{'a', 'b', 'c', 'd'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendBytes(tt.args.buf, tt.args.data), "appendBytes(%v, %v)", tt.args.buf, tt.args.data)
		})
	}
}

func Test_appendFloat64(t *testing.T) {
	type args struct {
		buf     []byte
		value   float64
		bitSize int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1.1, bitSize: 64},
			want: []byte("1.1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendFloat64(tt.args.buf, tt.args.value, tt.args.bitSize), "appendFloat64(%v, %v, %v)", tt.args.buf, tt.args.value, tt.args.bitSize)
		})
	}
}

func Test_appendInt64(t *testing.T) {
	type args struct {
		buf   []byte
		value int64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1},
			want: []byte{'1'},
		},
		{
			args: args{buf: []byte{1}, value: math.MaxInt64},
			want: []byte{1, '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '7'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendInt64(tt.args.buf, tt.args.value), "appendInt64(%v, %v)", tt.args.buf, tt.args.value)
		})
	}
}

func Test_appendString(t *testing.T) {
	type args struct {
		buf []byte
		s   string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, s: "test"},
			want: []byte{116, 101, 115, 116},
		},
		{
			args: args{buf: []byte{116, 101, 115, 116}, s: "test"},
			want: []byte{116, 101, 115, 116, 116, 101, 115, 116},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendString(tt.args.buf, tt.args.s), "appendString(%v, %v)", tt.args.buf, tt.args.s)
		})
	}
}

func Test_appendUint64(t *testing.T) {
	type args struct {
		buf   []byte
		value uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1},
			want: []byte("1"),
		},
		{
			args: args{buf: []byte{}, value: math.MaxUint64},
			want: []byte("18446744073709551615"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendUint64(tt.args.buf, tt.args.value), "appendUint64(%v, %v)", tt.args.buf, tt.args.value)
		})
	}
}

func Test_convertColIntoSql(t *testing.T) {
	bj, err := bytejson.ParseFromString("{\"a\": 1}")
	require.Nil(t, err)

	date, err := types.ParseDateCast("2023-02-03")
	require.Nil(t, err)

	rowid := types.BuildTestRowid(1234, 5678)
	blockid := types.BuildTestBlockid(1234, 5678)

	ts := types.BuildTS(1234, 5678)

	type args struct {
		ctx     context.Context
		data    any
		typ     *types.Type
		sqlBuff []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args:    args{ctx: context.Background(), data: nil, typ: &types.Type{Oid: types.T_int8}, sqlBuff: []byte{}},
			want:    []byte("NULL"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: bj, typ: &types.Type{Oid: types.T_json}, sqlBuff: []byte{}},
			want:    []byte("'{\"a\": 1}'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: false, typ: &types.Type{Oid: types.T_bool}, sqlBuff: []byte{}},
			want:    []byte("false"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: true, typ: &types.Type{Oid: types.T_bool}, sqlBuff: []byte{}},
			want:    []byte("true"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint64(1), typ: &types.Type{Oid: types.T_bit, Width: 5}, sqlBuff: []byte{}},
			want:    []byte{0x27, 0x1, 0x27},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int8(1), typ: &types.Type{Oid: types.T_int8}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint8(1), typ: &types.Type{Oid: types.T_uint8}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int16(1), typ: &types.Type{Oid: types.T_int16}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint16(1), typ: &types.Type{Oid: types.T_uint16}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int32(1), typ: &types.Type{Oid: types.T_int32}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint32(1), typ: &types.Type{Oid: types.T_uint32}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int64(1), typ: &types.Type{Oid: types.T_int64}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint64(1), typ: &types.Type{Oid: types.T_uint64}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: float32(1.1), typ: &types.Type{Oid: types.T_float32}, sqlBuff: []byte{}},
			want:    []byte("1.1"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: 1.1, typ: &types.Type{Oid: types.T_float64}, sqlBuff: []byte{}},
			want:    []byte("1.1"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_char}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_varchar}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_blob}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_text}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_binary}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_varbinary}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_datalink}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float32{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float32}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float64{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float64}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float64{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float64}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: date, typ: &types.Type{Oid: types.T_date}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "2023-02-03 01:23:45", typ: &types.Type{Oid: types.T_datetime}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03 01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "01:23:45", typ: &types.Type{Oid: types.T_time}, sqlBuff: []byte{}},
			want:    []byte("'01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "2023-02-03 01:23:45", typ: &types.Type{Oid: types.T_timestamp}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03 01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_decimal64}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_decimal128}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_uuid}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: rowid, typ: &types.Type{Oid: types.T_Rowid}, sqlBuff: []byte{}},
			want:    []byte("'d2040000-0000-0000-2e16-000000000000-0-0-5678'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: blockid, typ: &types.Type{Oid: types.T_Blockid}, sqlBuff: []byte{}},
			want:    []byte("'d2040000-0000-0000-2e16-000000000000-0-0'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: ts, typ: &types.Type{Oid: types.T_TS}, sqlBuff: []byte{}},
			want:    []byte("'1234-5678'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: types.Enum(1), typ: &types.Type{Oid: types.T_enum}, sqlBuff: []byte{}},
			want:    []byte("'1'"),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertColIntoSql(tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)
			if !tt.wantErr(t, err, fmt.Sprintf("convertColIntoSql(%v, %v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)) {
				return
			}
			assert.Equalf(t, tt.want, got, "convertColIntoSql(%v, %v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)
		})
	}
}

func Test_copyBytes(t *testing.T) {
	type args struct {
		src []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{src: []byte{}},
			want: []byte{},
		},
		{
			args: args{src: []byte{'a', 'b', 'c'}},
			want: []byte{'a', 'b', 'c'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, copyBytes(tt.args.src), "copyBytes(%v)", tt.args.src)
		})
	}
}

func Test_extractRowFromEveryVector(t *testing.T) {
	var err error
	bat := batch.New(true, []string{"const_null", "const", "normal"})
	bat.Vecs[0] = testutil.MakeScalarNull(types.T_int32, 3)
	bat.Vecs[1] = testutil.MakeScalarInt64(1, 3)
	bat.Vecs[2] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)

	row := make([]any, 3)
	err = extractRowFromEveryVector(context.Background(), bat, 0, row)
	require.NoError(t, err)
	assert.Equal(t, []any{nil, int64(1), int32(1)}, row)
}

func Test_extractRowFromVector(t *testing.T) {
	bj, err := bytejson.ParseFromString("{\"a\": 1}")
	require.Nil(t, err)

	date, err := types.ParseDateCast("2023-02-03")
	require.Nil(t, err)

	rowid := types.BuildTestRowid(1234, 5678)
	blockid := types.BuildTestBlockid(1234, 5678)

	ts := types.BuildTS(1234, 5678)

	type args struct {
		ctx      context.Context
		vec      *vector.Vector
		i        int
		row      []any
		rowIndex int
	}
	tests := []struct {
		name    string
		args    args
		wantRow []any
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeScalarNull(types.T_int32, 3),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{nil},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeJsonVector([]string{"{\"a\": 1}"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{bj},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeBoolVector([]bool{true}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{true},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeBitVector([]uint64{1}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{uint64(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeInt8Vector([]int8{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{int8(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeInt16Vector([]int16{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{int16(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeInt32Vector([]int32{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{int32(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeInt64Vector([]int64{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{int64(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeUint8Vector([]uint8{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{uint8(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeUint16Vector([]uint16{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{uint16(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeUint32Vector([]uint32{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{uint32(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeUint64Vector([]uint64{1, 2, 3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{uint64(1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeFloat32Vector([]float32{1.1, 2.2, 3.3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{float32(1.1)},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeFloat64Vector([]float64{1.1, 2.2, 3.3}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{1.1},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeVarcharVector([]string{"abc"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{[]byte("abc")},
			wantErr: assert.NoError,
		},
		// TODO vector
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeDateVector([]string{"2023-02-03 01:23:45"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{date},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeTimeVector([]string{"2023-02-03 01:23:45"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{"01:23:45"},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeDatetimeVector([]string{"2023-02-03 01:23:45"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{"2023-02-03 01:23:45"},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeTimestampVector([]string{"2023-02-03 01:23:45"}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{"2023-02-03 01:23:45"},
			wantErr: assert.NoError,
		},
		// TODO decimal
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeUUIDVector([]types.Uuid{types.Uuid([]byte("1234567890123456"))}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{"31323334-3536-3738-3930-313233343536"},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeRowIdVector([]types.Rowid{rowid}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{rowid},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeBlockIdVector([]types.Blockid{blockid}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{blockid},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      testutil.MakeTSVector([]types.TS{ts}, nil),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{ts},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx:      context.Background(),
				vec:      vector.NewVec(types.T_decimal256.ToType()),
				i:        0,
				row:      make([]any, 1),
				rowIndex: 0,
			},
			wantRow: []any{},
			wantErr: assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = extractRowFromVector(tt.args.ctx, tt.args.vec, tt.args.i, tt.args.row, tt.args.rowIndex)
			tt.wantErr(t, err, fmt.Sprintf("extractRowFromVector(%v, %v, %v, %v, %v)", tt.args.ctx, tt.args.vec, tt.args.i, tt.args.row, tt.args.rowIndex))
			if err == nil {
				assert.Equal(t, tt.wantRow, tt.args.row)
			}
		})
	}
}

func Test_floatArrayToString(t *testing.T) {
	type args[T interface{ float32 | float64 }] struct {
		arr []T
	}
	type testCase[T interface{ float32 | float64 }] struct {
		name string
		args args[T]
		want string
	}
	tests := []testCase[float32]{
		{
			args: args[float32]{arr: []float32{}},
			want: "'[]'",
		},
		{
			args: args[float32]{arr: []float32{1.1, 2.2, 3.3}},
			want: "'[1.100000,2.200000,3.300000]'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, floatArrayToString(tt.args.arr), "floatArrayToString(%v)", tt.args.arr)
		})
	}
}

func Test_openDbConn(t *testing.T) {
	stub := gostub.Stub(&tryConn, func(_ string) (*sql.DB, error) {
		return nil, nil
	})
	defer stub.Reset()

	conn, err := openDbConn("user", "password", "host", 1234)
	assert.Nil(t, err)
	assert.Nil(t, conn)
}

func Test_tryConn(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectPing()

	stub := gostub.Stub(&openDb, func(_, _ string) (*sql.DB, error) {
		return db, nil
	})
	defer stub.Reset()
	got, err := tryConn("dsn")
	assert.NoError(t, err)
	assert.Equal(t, db, got)
}

func TestGetTxnOp(t *testing.T) {
	type args struct {
		ctx         context.Context
		cnEngine    engine.Engine
		cnTxnClient client.TxnClient
		info        string
	}
	tests := []struct {
		name    string
		args    args
		want    client.TxnOperator
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTxnOp(tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)
			if !tt.wantErr(t, err, fmt.Sprintf("GetTxnOp(%v, %v, %v, %v)", tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetTxnOp(%v, %v, %v, %v)", tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)
		})
	}
}

func TestFinishTxnOp(t *testing.T) {
	type args struct {
		ctx      context.Context
		inputErr error
		txnOp    client.TxnOperator
		cnEngine engine.Engine
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FinishTxnOp(tt.args.ctx, tt.args.inputErr, tt.args.txnOp, tt.args.cnEngine)
		})
	}
}

func TestGetTableDef(t *testing.T) {
	type args struct {
		ctx      context.Context
		txnOp    client.TxnOperator
		cnEngine engine.Engine
		tblId    uint64
	}
	tests := []struct {
		name    string
		args    args
		want    *plan.TableDef
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTableDef(tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)
			if !tt.wantErr(t, err, fmt.Sprintf("GetTableDef(%v, %v, %v, %v)", tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetTableDef(%v, %v, %v, %v)", tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)
		})
	}
}

func TestGetInitDataKeySql(t *testing.T) {
	{
		stub := gostub.Stub(&cryptoRandRead, func([]byte) (int, error) {
			return 0, moerr.NewInternalErrorNoCtx("")
		})
		defer stub.Reset()

		_, err := GetInitDataKeySql("01234567890123456789012345678901")
		assert.Error(t, err)
	}
	{
		stub := gostub.Stub(&encrypt, func(data []byte, aesKey []byte) (string, error) {
			return "encrypted", nil
		})
		defer stub.Reset()

		s, err := GetInitDataKeySql("01234567890123456789012345678901")
		assert.NoError(t, err)
		assert.Equal(t, "insert into mo_catalog.mo_data_key (account_id, key_id, encrypted_key) values (0, '4e3da275-5003-4ca0-8667-5d3cdbecdd35', 'encrypted')", s)
	}
}

func TestAesCFBEncodeWithKey_EmptyKey(t *testing.T) {
	_, err := aesCFBEncodeWithKey([]byte("01234567890123456789012345678901"), []byte{})
	assert.Error(t, err)
}

func TestAesCFBDecodeWithKey_EmptyKey(t *testing.T) {
	_, err := AesCFBDecodeWithKey(context.Background(), "01234567890123456789012345678901", []byte{})
	assert.Error(t, err)
}
