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
	"io"
	"iter"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestIcebergDeleteApplyPositionAndEqualityMasksBatch(t *testing.T) {
	ctx := context.Background()
	deleteFS := &icebergDeleteTestFS{files: map[string][]byte{
		"delete-pos.parquet":       writeIcebergPositionDeleteParquet(t, []icebergPositionDeleteRow{{FilePath: "data.parquet", Pos: 1}}),
		"delete-eq-global.parquet": writeIcebergEqualityDeleteParquet(t, []int64{1}),
		"delete-eq.parquet":        writeIcebergEqualityDeleteParquet(t, []int64{3}),
		"delete-eq-other.parquet":  writeIcebergEqualityDeleteParquet(t, []int64{4}),
	}}
	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergDeleteTestProvider{fs: deleteFS}, nil, time.Minute)
	require.NoError(t, err)
	defer icebergio.ReleaseObjectIORef(ref)

	proc := testutil.NewProc(t)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                ctx,
			IcebergObjectIORef: ref,
			IcebergDeleteTasks: []*pipeline.IcebergDeleteFileTask{
				{DeleteType: "position", DeleteFilePath: "delete-pos.parquet", ReferencedDataFile: "data.parquet"},
				{DeleteType: "equality", DeleteFilePath: "delete-eq-global.parquet", EqualityFieldIds: []int32{1}},
				{DeleteType: "equality", DeleteFilePath: "delete-eq.parquet", ReferencedDataFile: "data.parquet", EqualityFieldIds: []int32{1}},
				{DeleteType: "equality", DeleteFilePath: "delete-eq-other.parquet", ReferencedDataFile: "other.parquet", EqualityFieldIds: []int32{1}},
			},
			IcebergColumns: []*pipeline.IcebergColumnMapping{
				{MoColIndex: 0, IcebergFieldId: 1, CurrentFieldName: "id", MoType: &plan.Type{Id: int32(types.T_int64)}},
			},
			NeedRowOrdinal: true,
			Extern:         &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}},
		},
		ExParam: ExParam{
			IcebergBatchDataFile:        "data.parquet",
			IcebergBatchStartRowOrdinal: 0,
		},
	}
	ext := &External{Es: param}
	bat := icebergInt64Batch(t, proc, []int64{1, 2, 3, 4})
	defer bat.Clean(proc.Mp())

	require.NoError(t, ext.applyIcebergDeletes(ctx, bat, proc))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, []int64{4}, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...))
	profile := param.takeParquetProfile()
	require.Equal(t, int64(4), profile.IcebergDeleteFilesRead)
	require.Equal(t, int64(3), profile.IcebergDeleteRowsFiltered)
	require.Equal(t, int64(1), profile.IcebergPositionDeleteRowsFiltered)
	require.Equal(t, int64(2), profile.IcebergEqualityDeleteRowsFiltered)
	require.Positive(t, profile.IcebergDeleteApplyPeakMemoryBytes)
}

func TestIcebergDeleteApplyMasksHiddenReadColumns(t *testing.T) {
	proc := testutil.NewProc(t)
	// id and value are projected, hidden_key is force-read for an equality
	// delete only. hidden_key sits before value, so physically dropping it would
	// shift value's ordinal and corrupt downstream projection. It must instead be
	// NULL-filled in place, preserving the batch shape a delete-free scan emits.
	bat := batch.NewWithSchema(false,
		[]string{"id", "hidden_key", "value"},
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(100), false, proc.Mp()))
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	ext := &External{Es: &ExternalParam{ExParamConst: ExParamConst{IcebergHiddenReadCols: []int32{1}}}}
	out, err := ext.ExecProjection(proc, bat)
	require.NoError(t, err)
	// Shape is preserved: same batch, same columns and ordinals.
	require.Same(t, bat, out)
	require.Equal(t, []string{"id", "hidden_key", "value"}, out.Attrs)
	require.Len(t, out.Vecs, 3)
	require.Equal(t, []int64{1}, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](out.Vecs[0])...))
	require.Equal(t, []int64{100}, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](out.Vecs[2])...))
	// The hidden equality key is reset to NULL so its force-read value cannot leak.
	require.True(t, out.Vecs[1].IsNull(0))
}

func TestIcebergDeleteApplyEqualityTemporalValuesUseMORepresentation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = time.UTC

	dateValue, err := parquetValueEqualityValue(ctx, proc, equalityDeleteColumn{
		mapping:     &pipeline.IcebergColumnMapping{MoType: &plan.Type{Id: int32(types.T_date)}},
		parquetType: parquet.Date().Type(),
	}, parquet.Int32Value(0))
	require.NoError(t, err)
	require.Equal(t, int32(types.DaysFromUnixEpochToDate(0)), dateValue)

	datetimeFromDate, err := parquetValueEqualityValue(ctx, proc, equalityDeleteColumn{
		mapping:     &pipeline.IcebergColumnMapping{MoType: &plan.Type{Id: int32(types.T_datetime)}},
		parquetType: parquet.Date().Type(),
	}, parquet.Int32Value(1))
	require.NoError(t, err)
	require.Equal(t, int64(types.DaysFromUnixEpochToDate(1).ToDatetime()), datetimeFromDate)

	datetimeFromTimestamp, err := parquetValueEqualityValue(ctx, proc, equalityDeleteColumn{
		mapping:     &pipeline.IcebergColumnMapping{MoType: &plan.Type{Id: int32(types.T_datetime)}},
		parquetType: parquet.Timestamp(parquet.Millisecond).Type(),
	}, parquet.Int64Value(1500))
	require.NoError(t, err)
	require.Equal(t, int64(types.Datetime(types.UnixMicroToTimestamp(1_500_000))), datetimeFromTimestamp)

	timestampValue, err := parquetValueEqualityValue(ctx, proc, equalityDeleteColumn{
		mapping:     &pipeline.IcebergColumnMapping{MoType: &plan.Type{Id: int32(types.T_timestamp)}},
		parquetType: parquet.Timestamp(parquet.Microsecond).Type(),
	}, parquet.Int64Value(1_000_000))
	require.NoError(t, err)
	require.Equal(t, int64(types.UnixMicroToTimestamp(1_000_000)), timestampValue)

	badDate, err := parquetValueEqualityValue(ctx, proc, equalityDeleteColumn{
		mapping:     &pipeline.IcebergColumnMapping{MoType: &plan.Type{Id: int32(types.T_date)}},
		parquetType: parquet.Leaf(parquet.Int32Type).Type(),
	}, parquet.Int32Value(0))
	require.Error(t, err)
	require.Nil(t, badDate)
	require.Contains(t, err.Error(), "ICEBERG_UNSUPPORTED_FEATURE")
}

func TestIcebergDeleteApplyDateEqualityMatchesScanVector(t *testing.T) {
	ctx := context.Background()
	deleteFS := &icebergDeleteTestFS{files: map[string][]byte{
		"delete-date.parquet": writeIcebergDateEqualityDeleteParquet(t, []int32{0}),
	}}
	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergDeleteTestProvider{fs: deleteFS}, nil, time.Minute)
	require.NoError(t, err)
	defer icebergio.ReleaseObjectIORef(ref)

	proc := testutil.NewProc(t)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                ctx,
			IcebergObjectIORef: ref,
			IcebergDeleteTasks: []*pipeline.IcebergDeleteFileTask{{
				DeleteType:       "equality",
				DeleteFilePath:   "delete-date.parquet",
				EqualityFieldIds: []int32{2},
			}},
			IcebergColumns: []*pipeline.IcebergColumnMapping{{
				MoColIndex:       0,
				IcebergFieldId:   2,
				CurrentFieldName: "d",
				MoType:           &plan.Type{Id: int32(types.T_date)},
			}},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}},
		},
	}
	ext := &External{Es: param}
	bat := batch.NewWithSchema(false, []string{"d"}, []types.Type{types.T_date.ToType()})
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.DaysFromUnixEpochToDate(0), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.DaysFromUnixEpochToDate(1), false, proc.Mp()))
	bat.SetRowCount(2)
	defer bat.Clean(proc.Mp())

	require.NoError(t, ext.applyIcebergDeletes(ctx, bat, proc))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, []types.Date{types.DaysFromUnixEpochToDate(1)}, append([]types.Date(nil), vector.MustFixedColWithTypeCheck[types.Date](bat.Vecs[0])...))
}

func TestIcebergDeleteApplyUsesConfiguredMemoryLimit(t *testing.T) {
	ctx := context.Background()
	deleteFS := &icebergDeleteTestFS{files: map[string][]byte{
		"delete-pos.parquet": writeIcebergPositionDeleteParquet(t, []icebergPositionDeleteRow{{FilePath: "data.parquet", Pos: 1}}),
	}}
	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergDeleteTestProvider{fs: deleteFS}, nil, time.Minute)
	require.NoError(t, err)
	defer icebergio.ReleaseObjectIORef(ref)

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:                         ctx,
			IcebergObjectIORef:          ref,
			IcebergDeleteMaxMemoryBytes: 1,
			IcebergDeleteTasks: []*pipeline.IcebergDeleteFileTask{{
				DeleteType:         "position",
				DeleteFilePath:     "delete-pos.parquet",
				ReferencedDataFile: "data.parquet",
			}},
			Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}},
		},
	}
	ext := &External{Es: param}
	err = ext.prepareIcebergDeleteApply(testutil.NewProc(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ICEBERG_PLANNING_LIMIT_EXCEEDED")
	require.Contains(t, err.Error(), "limit_bytes")
	require.False(t, strings.Contains(err.Error(), "delete-pos.parquet"))
}

func TestIcebergRowOrdinalSideChannelUsesRowGroupStart(t *testing.T) {
	data := writeIcebergDataParquetWithRowGroups(t, []int64{1, 2, 3, 4}, 2)
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx:      context.Background(),
			Attrs:    []plan.ExternAttr{{ColName: "id", ColIndex: 0}},
			Cols:     []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}}},
			Extern:   &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET}},
			FileSize: []int64{int64(len(data))},
			ParquetRowGroupShards: []*pipeline.ParquetRowGroupShard{
				{FileIndex: 0, RowGroupStart: 1, RowGroupEnd: 2},
			},
			NeedRowOrdinal: true,
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, Filepath: "data.parquet", FileCnt: 1}},
	}
	param.Extern.Data = string(data)
	proc := testutil.NewProc(t)
	bat := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_int64.ToType()})
	defer bat.Clean(proc.Mp())
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	_, err = reader.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), param.IcebergBatchStartRowOrdinal)
	require.Equal(t, "data.parquet", param.IcebergBatchDataFile)
	require.NoError(t, reader.Close())
}

func TestIcebergDMLMetadataColumnsSurviveDeleteApplyShrink(t *testing.T) {
	ctx := context.Background()
	data := writeIcebergDataParquetWithRowGroups(t, []int64{10, 20, 30, 40}, 4)
	deleteFS := &icebergDeleteTestFS{files: map[string][]byte{
		"delete-pos.parquet": writeIcebergPositionDeleteParquet(t, []icebergPositionDeleteRow{{FilePath: "data.parquet", Pos: 1}}),
	}}
	ref, err := icebergio.RegisterObjectIOProvider(ctx, icebergDeleteTestProvider{fs: deleteFS}, nil, time.Minute)
	require.NoError(t, err)
	defer icebergio.ReleaseObjectIORef(ref)

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: ctx,
			Attrs: []plan.ExternAttr{
				{ColName: "id", ColIndex: 0},
				{ColName: IcebergDMLDataFilePathAttr, ColIndex: 1},
				{ColName: IcebergDMLRowOrdinalAttr, ColIndex: 2},
			},
			Cols: []*plan.ColDef{
				{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
				{Typ: plan.Type{Id: int32(types.T_varchar), Width: 1024}},
				{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
			},
			Extern:             &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET}},
			FileSize:           []int64{int64(len(data))},
			IcebergObjectIORef: ref,
			IcebergDeleteTasks: []*pipeline.IcebergDeleteFileTask{{DeleteType: "position", DeleteFilePath: "delete-pos.parquet", ReferencedDataFile: "data.parquet"}},
			IcebergColumns:     []*pipeline.IcebergColumnMapping{{MoColIndex: 0, IcebergFieldId: 1, CurrentFieldName: "id", MoType: &plan.Type{Id: int32(types.T_int64)}}},
			NeedRowOrdinal:     true,
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, Filepath: "data.parquet", FileCnt: 1}},
	}
	param.Extern.Data = string(data)

	proc := testutil.NewProc(t)
	bat := batch.NewWithSchema(false,
		[]string{"id", IcebergDMLDataFilePathAttr, IcebergDMLRowOrdinalAttr},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()})
	defer bat.Clean(proc.Mp())
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	_, err = reader.ReadBatch(ctx, bat, proc, nil)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	ext := &External{Es: param}
	require.NoError(t, ext.applyIcebergDeletes(ctx, bat, proc))
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, []int64{10, 30, 40}, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...))
	require.Equal(t, []string{"data.parquet", "data.parquet", "data.parquet"}, vectorStrings(bat.Vecs[1], bat.RowCount()))
	require.Equal(t, []int64{0, 2, 3}, append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])...))
}

type icebergDeleteTestFS struct {
	files map[string][]byte
}

type icebergDeleteTestProvider struct {
	fs fileservice.ETLFileService
}

func (p icebergDeleteTestProvider) Resolve(ctx context.Context, scope icebergio.ObjectScope) (fileservice.ETLFileService, string, error) {
	return p.fs, scope.StorageLocation, nil
}

func (p icebergDeleteTestProvider) Refresh(ctx context.Context, scope icebergio.ObjectScope) (icebergio.ObjectScope, error) {
	return scope, nil
}

func (p icebergDeleteTestProvider) RedactPath(path string) string {
	return "<redacted>"
}

func (f *icebergDeleteTestFS) Name() string { return "iceberg-delete-test" }
func (f *icebergDeleteTestFS) Write(ctx context.Context, v fileservice.IOVector) error {
	return nil
}
func (f *icebergDeleteTestFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if len(v.Entries) == 0 {
		return moerr.NewInternalError(ctx, "empty entries")
	}
	data := f.files[v.FilePath]
	if data == nil {
		return os.ErrNotExist
	}
	entry := &v.Entries[0]
	if entry.Size < 0 {
		entry.Size = int64(len(data)) - entry.Offset
	}
	if entry.Offset < 0 || entry.Offset+entry.Size > int64(len(data)) {
		return io.EOF
	}
	if len(entry.Data) < int(entry.Size) {
		entry.Data = make([]byte, entry.Size)
	}
	copy(entry.Data, data[entry.Offset:entry.Offset+entry.Size])
	return nil
}
func (f *icebergDeleteTestFS) ReadCache(ctx context.Context, v *fileservice.IOVector) error {
	return nil
}
func (f *icebergDeleteTestFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return nil
}
func (f *icebergDeleteTestFS) Delete(ctx context.Context, filePaths ...string) error { return nil }
func (f *icebergDeleteTestFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	data := f.files[filePath]
	if data == nil {
		return nil, os.ErrNotExist
	}
	return &fileservice.DirEntry{Name: filePath, Size: int64(len(data))}, nil
}
func (f *icebergDeleteTestFS) PrefetchFile(ctx context.Context, filePath string) error { return nil }
func (f *icebergDeleteTestFS) Cost() *fileservice.CostAttr                             { return &fileservice.CostAttr{} }
func (f *icebergDeleteTestFS) Close(ctx context.Context)                               {}
func (f *icebergDeleteTestFS) ETLCompatible()                                          {}

type icebergPositionDeleteRow struct {
	FilePath string
	Pos      int64
}

func writeIcebergPositionDeleteParquet(t *testing.T, rows []icebergPositionDeleteRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"file_path": parquet.String(),
		"pos":       parquet.Leaf(parquet.Int64Type),
	})
	writer := parquet.NewWriter(&buf, schema)
	parquetRows := make([]parquet.Row, len(rows))
	for idx, row := range rows {
		parquetRows[idx] = parquet.Row{
			parquet.ValueOf(row.FilePath).Level(0, 0, 0),
			parquet.Int64Value(row.Pos).Level(0, 0, 1),
		}
	}
	_, err := writer.WriteRows(parquetRows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func writeIcebergEqualityDeleteParquet(t *testing.T, ids []int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1),
	})
	writer := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, len(ids))
	for idx, id := range ids {
		rows[idx] = parquet.Row{parquet.Int64Value(id).Level(0, 0, 0)}
	}
	_, err := writer.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func writeIcebergDateEqualityDeleteParquet(t *testing.T, days []int32) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"d": parquet.FieldID(parquet.Date(), 2),
	})
	writer := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, len(days))
	for idx, day := range days {
		rows[idx] = parquet.Row{parquet.Int32Value(day).Level(0, 0, 0)}
	}
	_, err := writer.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func writeIcebergDataParquetWithRowGroups(t *testing.T, values []int64, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{
		"id": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 1),
	})
	writer := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for idx, value := range values {
		rows[idx] = parquet.Row{parquet.Int64Value(value).Level(0, 0, 0)}
	}
	_, err := writer.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func icebergInt64Batch(t *testing.T, proc *process.Process, values []int64) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_int64.ToType()})
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, proc.Mp()))
	}
	bat.SetRowCount(len(values))
	return bat
}

func vectorStrings(vec *vector.Vector, rows int) []string {
	out := make([]string, rows)
	for i := range out {
		out[i] = vec.GetStringAt(i)
	}
	return out
}
