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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// attributedDecimalParquetParam builds an in-memory parquet ExternalParam for
// a single optional DECIMAL(10,2) column named "c" whose MO type carries the
// unsigned attribute mask. externType selects the scan kind (LOAD or
// EXTERNAL_TB), mirroring how the operator hands the same file to LOAD DATA
// and to an external table.
func attributedDecimalParquetParam(externType plan.ExternType) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:   context.Background(),
			Attrs: []plan.ExternAttr{{ColName: "c", ColIndex: 0}},
			Cols: []*plan.ColDef{{
				Name: "c",
				Typ: plan.Type{
					Id:                int32(types.T_decimal64),
					Width:             10,
					Scale:             2,
					NumericAttributes: types.NumericAttributeUnsigned,
				},
			}},
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Format: tree.PARQUET,
				},
				ExParam: tree.ExParam{
					ExternType: int32(externType),
				},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{
			FileIndex: 1,
			FileCnt:   1,
			Filepath:  "s3://bucket/path/attributed.parquet",
		}},
	}
}

// openAttributedDecimalParquetFile writes one optional FLBA DECIMAL(10,2)
// parquet file with a positive value and a NULL control row.
func openAttributedDecimalParquetFile(t *testing.T) *parquet.File {
	t.Helper()

	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Optional(parquet.Decimal(2, 10, parquet.FixedLenByteArrayType(5))),
	})
	w := parquet.NewWriter(&buf, schema)
	rows := []parquet.Row{
		{parquet.FixedLenByteArrayValue(encodeDecimalToBytes(12345, 5)).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

// TestParquetAttributedDecimalStagingPredicateCoupling pins the contract the
// review finding requires: mapper selection and output vector allocation must
// share the same LOAD staging predicate. A LOAD scan of an attributed DECIMAL
// parquet file stages rendered text into a VARCHAR vector; an external table
// scan of the same file projects the physical DECIMAL vector and must take the
// typed mapper. Before the fix, mapper selection ignored the LOAD gate, so an
// external table scan received the varlena text staging mapper while external.go
// allocated a physical DECIMAL vector - staging wrote varlena descriptors into
// fixed-width storage.
func TestParquetAttributedDecimalStagingPredicateCoupling(t *testing.T) {
	proc := testutil.NewProc(t)
	f := openAttributedDecimalParquetFile(t)

	loadParam := attributedDecimalParquetParam(plan.ExternType_LOAD)
	externalParam := attributedDecimalParquetParam(plan.ExternType_EXTERNAL_TB)

	// The single predicate that prepare() now shares with allocation.
	require.True(t, loadAttributedDecimalTextInput(loadParam, &loadParam.Cols[0].Typ))
	require.False(t, loadAttributedDecimalTextInput(externalParam, &externalParam.Cols[0].Typ))
	require.True(t, isAttributedDecimalStagingTarget(&loadParam.Cols[0].Typ))

	t.Run("load_stages_rendered_text", func(t *testing.T) {
		h := &ParquetHandler{file: f, batchCnt: 10}
		require.NoError(t, h.prepare(loadParam))

		// Allocation mirror: external.go allocates VARCHAR for the LOAD
		// staging predicate.
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 65535, 0))
		require.NoError(t, h.getData(bat, loadParam, proc))
		require.Equal(t, 2, bat.RowCount())
		require.False(t, bat.Vecs[0].GetNulls().Contains(0))
		require.True(t, bat.Vecs[0].GetNulls().Contains(1), "NULL must be preserved, never rendered")
		require.Equal(t, "123.45", bat.Vecs[0].GetStringAt(0))
	})

	t.Run("external_table_reads_physical_decimal", func(t *testing.T) {
		h := &ParquetHandler{file: f, batchCnt: 10}
		require.NoError(t, h.prepare(externalParam))

		// Allocation mirror: external.go keeps the physical DECIMAL vector for
		// a non-LOAD scan of the same attributed column, so the mapper must be
		// the typed decimal mapper.
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		require.NoError(t, h.getData(bat, externalParam, proc))
		require.Equal(t, 2, bat.RowCount())
		require.False(t, bat.Vecs[0].GetNulls().Contains(0))
		require.True(t, bat.Vecs[0].GetNulls().Contains(1))
		got := vector.MustFixedColWithTypeCheck[types.Decimal64](bat.Vecs[0])
		require.Equal(t, types.Decimal64(12345), got[0])
	})
}

// TestParquetAttributedDecimalStagingMapperGate unit-pins the mapper gate
// itself: the staging mapper is selected exactly when the shared LOAD staging
// predicate holds, independent of the column's attribute mask.
func TestParquetAttributedDecimalStagingMapperGate(t *testing.T) {
	proc := testutil.NewProc(t)
	f := openAttributedDecimalParquetFile(t)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	typ := plan.Type{
		Id:                int32(types.T_decimal64),
		Width:             10,
		Scale:             2,
		NumericAttributes: types.NumericAttributeUnsigned,
	}

	var h ParquetHandler

	// LOAD staging bool: rendered text stages into a VARCHAR vector.
	loadParam := attributedDecimalParquetParam(plan.ExternType_LOAD)
	mp := h.getMapperWithStaging(col, typ, loadAttributedDecimalTextInput(loadParam, &typ))
	require.NotNil(t, mp)
	vec := vector.NewVec(types.New(types.T_varchar, 65535, 0))
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, 2, vec.Length())
	require.Equal(t, "123.45", vec.GetStringAt(0))
	require.True(t, vec.GetNulls().Contains(1))

	// Non-LOAD staging bool (external table): typed decimal mapper reads the
	// physical value, including the NULL control.
	externalParam := attributedDecimalParquetParam(plan.ExternType_EXTERNAL_TB)
	mp = h.getMapperWithStaging(col, typ, loadAttributedDecimalTextInput(externalParam, &typ))
	require.NotNil(t, mp)
	vec2 := vector.NewVec(types.New(types.T_decimal64, 10, 2))
	require.NoError(t, mp.mapping(page, proc, vec2))
	require.Equal(t, 2, vec2.Length())
	require.False(t, vec2.GetNulls().Contains(0))
	require.True(t, vec2.GetNulls().Contains(1))
	require.Equal(t, types.Decimal64(12345), vector.MustFixedColWithTypeCheck[types.Decimal64](vec2)[0])
}

// openAttributedDecimalBooleanParquetFile writes one optional BOOLEAN parquet
// column with a true row, a false row and a NULL control row.
func openAttributedDecimalBooleanParquetFile(t *testing.T) *parquet.File {
	t.Helper()

	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"c": parquet.Optional(parquet.Leaf(parquet.BooleanType)),
	})
	w := parquet.NewWriter(&buf, schema)
	rows := []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 1, 0)},
		{parquet.BooleanValue(false).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f
}

// TestParquetAttributedDecimalStagingBooleanSource pins the BOOLEAN cast
// source the attributed staging mapper must preserve: the typed decimal
// mapper has always accepted parquet BOOLEAN (isParquetDecimalCastSource)
// and renders it 1/0, so a LOAD into a DECIMAL UNSIGNED or ZEROFILL target
// from a boolean physical must stage exactly the same text - true -> "1",
// false -> "0", NULL preserved and never rendered. Before the fix the
// attributed text mapper's switch omitted parquet.Boolean and returned a nil
// mapper, failing the whole LOAD at mapper preparation.
func TestParquetAttributedDecimalStagingBooleanSource(t *testing.T) {
	proc := testutil.NewProc(t)
	f := openAttributedDecimalBooleanParquetFile(t)

	t.Run("load_stages_shared_scalar_rendering", func(t *testing.T) {
		loadParam := attributedDecimalParquetParam(plan.ExternType_LOAD)
		require.True(t, loadAttributedDecimalTextInput(loadParam, &loadParam.Cols[0].Typ))

		h := &ParquetHandler{file: f, batchCnt: 10}
		require.NoError(t, h.prepare(loadParam),
			"the mapper switch must accept parquet.Boolean for attributed LOAD")
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 65535, 0))
		require.NoError(t, h.getData(bat, loadParam, proc))
		require.Equal(t, 3, bat.RowCount())
		require.Equal(t, "1", bat.Vecs[0].GetStringAt(0), "--- renders true with the shared scalar logic")
		require.Equal(t, "0", bat.Vecs[0].GetStringAt(1))
		require.True(t, bat.Vecs[0].GetNulls().Contains(2), "NULL booleans record as nulls, never rendered")
		require.False(t, bat.Vecs[0].GetNulls().Contains(0))
		require.False(t, bat.Vecs[0].GetNulls().Contains(1))
	})

	t.Run("external_table_typed_path_control", func(t *testing.T) {
		// Control: the pre-existing typed decimal mapper already accepted
		// parquet.Boolean; the attributed LOAD staging must mirror exactly
		// that conversion contract.
		externalParam := attributedDecimalParquetParam(plan.ExternType_EXTERNAL_TB)
		h := &ParquetHandler{file: f, batchCnt: 10}
		require.NoError(t, h.prepare(externalParam))
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		require.NoError(t, h.getData(bat, externalParam, proc))
		require.Equal(t, 3, bat.RowCount())
		got := vector.MustFixedColWithTypeCheck[types.Decimal64](bat.Vecs[0])
		require.Equal(t, types.Decimal64(100), got[0], "true -> decimal 1.00")
		require.Equal(t, types.Decimal64(0), got[1], "false -> decimal 0.00")
		require.True(t, bat.Vecs[0].GetNulls().Contains(2))
	})
}
