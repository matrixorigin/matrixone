// Copyright 2026 Matrix Origin
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

package external

import (
	"bytes"
	"context"
	"github.com/parquet-go/parquet-go"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestEnumExternalDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := plan.Type{Id: int32(types.T_enum), Enumvalues: "4#3:a,b0:3: c 1:d", EnumEncoding: 1}
	// Exact byte lengths include both spaces around the third member.
	col := &plan.ColDef{Name: "e", Typ: typ, Default: &plan.Default{NullAbility: true}}
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background(), Cols: []*plan.ColDef{col}, Extern: &tree.ExternParam{}}}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_enum.ToType())
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	for _, value := range []string{"a,b", "", " c ", "d"} {
		require.True(t, isLegalLine(param.Extern, param.Cols, []csvparser.Field{{Val: value}}), value)
		require.NoError(t, getColData(bat, []csvparser.Field{{Val: value}}, 0, param, proc.Mp(), plan.ExternAttr{ColIndex: 0, ColName: "e", ColFieldIndex: 0}, proc))
	}
	require.Equal(t, []types.Enum{1, 2, 3, 4}, vector.MustFixedColWithTypeCheck[types.Enum](bat.Vecs[0]))
	// Hive's fill owner decodes once and repeats the resulting ordinal.
	hive := vector.NewVec(types.T_enum.ToType())
	defer hive.Free(proc.Mp())
	require.NoError(t, fillConstantVector(hive, "a,b", col, 7, proc, "e=a,b/part.parquet"))
	require.True(t, hive.IsConst())
	require.Equal(t, types.Enum(1), vector.GetFixedAtNoTypeCheck[types.Enum](hive, 0))
	bad := *col
	bad.Typ.EnumEncoding = 257
	require.Error(t, validatePartitionValue("a,b", "e", tree.HivePartColType{Id: typ.Id, Enumvalues: typ.Enumvalues, EnumEncoding: 257}, proc, "bad"))
	_, err := decodeExternalEnumMembers([]*plan.ColDef{&bad})
	require.Error(t, err)
	require.False(t, isLegalLine(param.Extern, []*plan.ColDef{&bad}, []csvparser.Field{{Val: "a,b"}}))
	op := &External{Es: param}
	op.Reset(proc, false, nil)
	require.Nil(t, param.enumMembers)
	require.NoError(t, param.initEnumMembers())
	require.NotNil(t, param.enumMembers)
	op.Free(proc, true, nil)
	require.Nil(t, param.enumMembers)

}

func TestEnumParquetDomain(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		t.Run(map[bool]string{false: "labels", true: "ordinals"}[numeric], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			st := parquet.ByteArrayType
			rows := []parquet.Row{{parquet.ByteArrayValue([]byte("a,b"))}, {parquet.ByteArrayValue(nil)}, {parquet.ByteArrayValue([]byte("c"))}}
			if numeric {
				st = parquet.Int32Type
				rows = []parquet.Row{{parquet.Int32Value(1)}, {parquet.Int32Value(2)}, {parquet.Int32Value(3)}}
			}
			var data bytes.Buffer
			schema := parquet.NewSchema("enum", parquet.Group{"e": parquet.Leaf(st)})
			writer := parquet.NewWriter(&data, schema)
			_, err := writer.WriteRows(rows)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			file, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
			require.NoError(t, err)
			pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
			defer pages.Close()
			page, err := pages.ReadPage()
			require.NoError(t, err)
			var handler ParquetHandler
			target := plan.Type{Id: int32(types.T_enum), Enumvalues: "3#3:a,b0:1:c", EnumEncoding: 1}
			mapper := handler.getMapper(file.Root().Column("e"), target)
			require.NotNil(t, mapper)
			out := vector.NewVec(types.T_enum.ToType())
			defer out.Free(proc.Mp())
			require.NoError(t, mapper.mapping(page, proc, out))
			require.Equal(t, []types.Enum{1, 2, 3}, vector.MustFixedColWithTypeCheck[types.Enum](out))
			target.EnumEncoding = 257
			mapper = handler.getMapper(file.Root().Column("e"), target)
			require.NotNil(t, mapper)
			require.Error(t, mapper.mapping(page, proc, out))
		})
	}
}
