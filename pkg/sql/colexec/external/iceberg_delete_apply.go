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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergdelete"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const icebergDeleteReadBatchRows = 1024
const icebergDeleteApplyStateBytes = int64(128)

func (external *External) prepareIcebergDeleteApply(proc *process.Process) error {
	param := external.Es
	if param == nil || len(param.IcebergDeleteTasks) == 0 || param.icebergDeleteLoaded {
		return nil
	}
	opts := icebergDeleteApplyOptions(param)
	states := make(map[string]*icebergdelete.ApplyState)
	var totalMemoryBytes int64
	for _, task := range param.IcebergDeleteTasks {
		if task == nil {
			continue
		}
		dataFile := icebergDeleteTaskDataFile(task)
		state := states[dataFile]
		if state == nil {
			state = icebergdelete.NewApplyState(opts)
			states[dataFile] = state
			totalMemoryBytes = saturatingIcebergDeleteMemoryAdd(totalMemoryBytes, icebergDeleteApplyStateBytes+int64(len(dataFile)))
			if err := icebergdelete.CheckMemoryLimit(proc.Ctx, opts, totalMemoryBytes); err != nil {
				return err
			}
		}
		beforeMemoryBytes := state.MemoryBytes()
		state.Options.BaseMemoryBytes = totalMemoryBytes - beforeMemoryBytes
		switch strings.ToLower(strings.TrimSpace(task.DeleteType)) {
		case "position":
			if err := loadIcebergPositionDeleteFile(proc.Ctx, param, state, task); err != nil {
				return err
			}
		case "equality":
			if err := loadIcebergEqualityDeleteFile(proc, param, state, task); err != nil {
				return err
			}
		default:
			return icebergapi.ToMOErr(proc.Ctx, icebergapi.NewError(icebergapi.ErrUnsupportedFeature, "Iceberg delete type is unsupported", map[string]string{
				"delete_type": task.DeleteType,
			}))
		}
		if err := state.CheckMemory(proc.Ctx); err != nil {
			return err
		}
		delta := state.Profile.MemoryBytes - beforeMemoryBytes
		if delta < 0 {
			return icebergapi.ToMOErr(proc.Ctx, icebergapi.NewError(icebergapi.ErrInternal, "Iceberg delete apply memory accounting regressed", nil))
		}
		totalMemoryBytes = saturatingIcebergDeleteMemoryAdd(totalMemoryBytes, delta)
		if err := icebergdelete.CheckMemoryLimit(proc.Ctx, opts, totalMemoryBytes); err != nil {
			return err
		}
		state.Profile.DeleteFilesRead++
	}
	for _, state := range states {
		if state != nil {
			state.Options.BaseMemoryBytes = totalMemoryBytes - state.MemoryBytes()
		}
	}
	param.icebergDeleteStates = states
	param.icebergDeleteLoaded = true
	param.addIcebergDeleteLoadProfile(states, totalMemoryBytes)
	return nil
}

func saturatingIcebergDeleteMemoryAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func icebergDeleteApplyOptions(param *ExternalParam) icebergdelete.Options {
	maxMemory := int64(0)
	spillEnabled := false
	if param != nil {
		maxMemory = param.IcebergDeleteMaxMemoryBytes
		spillEnabled = param.IcebergDeleteSpillEnabled
	}
	if maxMemory == 0 {
		maxMemory = icebergapi.DefaultConfig().Write.DeleteMaxMemory
	}
	return icebergdelete.Options{
		MaxMemoryBytes: maxMemory,
		SpillEnabled:   spillEnabled,
	}
}

func (external *External) applyIcebergDeletes(ctx context.Context, bat *batch.Batch, proc *process.Process) error {
	param := external.Es
	if param == nil || bat == nil || bat.RowCount() == 0 || len(param.IcebergDeleteTasks) == 0 {
		return nil
	}
	if err := external.prepareIcebergDeleteApply(proc); err != nil {
		return err
	}
	states := icebergDeleteApplyStates(param.icebergDeleteStates, param.IcebergBatchDataFile)
	if len(states) == 0 {
		return nil
	}
	rowsBefore := bat.RowCount()
	keep := make([]bool, bat.RowCount())
	for i := range keep {
		keep[i] = true
	}
	profileBefore := make(map[*icebergdelete.ApplyState]icebergdelete.Profile, len(states))
	for _, state := range states {
		profileBefore[state] = state.Profile
		if param.NeedRowOrdinal {
			positionKeep, err := state.ApplyPositionMask(ctx, param.IcebergBatchDataFile, param.IcebergBatchStartRowOrdinal, bat.RowCount())
			if err != nil {
				return err
			}
			for i := range keep {
				keep[i] = keep[i] && positionKeep[i]
			}
		}
		for _, layout := range icebergEqualityLayouts(param.IcebergDeleteTasks) {
			equalityRows, err := icebergEqualityRows(param, bat, layout.fieldIDs)
			if err != nil {
				return err
			}
			if len(equalityRows) == 0 {
				continue
			}
			equalityKeep, err := state.ApplyEqualityMaskForLayout(ctx, layout.key, equalityRows)
			if err != nil {
				return err
			}
			for i := range keep {
				keep[i] = keep[i] && equalityKeep[i]
			}
		}
	}
	sels := keepMaskSelections(keep)
	for _, state := range states {
		param.addIcebergDeleteProfile(state, profileBefore[state], 0)
	}
	rowsFiltered := int64(rowsBefore - len(sels))
	if rowsFiltered > 0 {
		param.addParquetProfile(process.ParquetProfileStats{IcebergDeleteRowsFiltered: rowsFiltered})
	}
	if len(sels) == bat.RowCount() {
		return nil
	}
	bat.Shrink(sels, false)
	return nil
}

func icebergDeleteTaskDataFile(task *pipeline.IcebergDeleteFileTask) string {
	if task == nil {
		return ""
	}
	return strings.TrimSpace(task.ReferencedDataFile)
}

func icebergDeleteApplyStates(states map[string]*icebergdelete.ApplyState, dataFile string) []*icebergdelete.ApplyState {
	if len(states) == 0 {
		return nil
	}
	dataFile = strings.TrimSpace(dataFile)
	out := make([]*icebergdelete.ApplyState, 0, 2)
	if state := states[dataFile]; state != nil {
		out = append(out, state)
	}
	if global := states[""]; global != nil && (len(out) == 0 || out[0] != global) {
		out = append(out, global)
	}
	return out
}

func loadIcebergPositionDeleteFile(ctx context.Context, param *ExternalParam, state *icebergdelete.ApplyState, task *pipeline.IcebergDeleteFileTask) error {
	file, err := openIcebergDeleteParquet(ctx, param, state, task.DeleteFilePath)
	if err != nil {
		return err
	}
	filePathCol, ok := parquetColumnIndexByName(file, "file_path", "file")
	if !ok {
		return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg position delete file is missing file_path column", map[string]string{
			"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
		}))
	}
	posCol, ok := parquetColumnIndexByName(file, "pos", "position", "row_position")
	if !ok {
		return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg position delete file is missing pos column", map[string]string{
			"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
		}))
	}
	return readParquetRows(ctx, file, func(row parquet.Row) error {
		dataFile, ok := parquetRowString(row, filePathCol)
		if !ok || strings.TrimSpace(dataFile) == "" {
			return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg position delete row has an invalid file_path", map[string]string{
				"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
			}))
		}
		if task.ReferencedDataFile != "" && dataFile != task.ReferencedDataFile {
			return nil
		}
		pos, ok := parquetRowInt64(row, posCol)
		if !ok || pos < 0 {
			return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg position delete row has an invalid position", map[string]string{
				"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
			}))
		}
		state.Position.Add(dataFile, pos)
		return state.CheckMemory(ctx)
	})
}

func loadIcebergEqualityDeleteFile(proc *process.Process, param *ExternalParam, state *icebergdelete.ApplyState, task *pipeline.IcebergDeleteFileTask) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	file, err := openIcebergDeleteParquet(ctx, param, state, task.DeleteFilePath)
	if err != nil {
		return err
	}
	columns, err := equalityDeleteColumns(ctx, file, task, param.IcebergColumns)
	if err != nil {
		return err
	}
	return readParquetRows(ctx, file, func(row parquet.Row) error {
		values := make([]any, len(columns))
		for idx, col := range columns {
			value, err := parquetRowEqualityValue(ctx, proc, row, col)
			if err != nil {
				return err
			}
			values[idx] = value
		}
		state.AddEqualityKey(icebergEqualityLayoutKey(task.EqualityFieldIds), values...)
		return state.CheckMemory(ctx)
	})
}

func openIcebergDeleteParquet(ctx context.Context, param *ExternalParam, state *icebergdelete.ApplyState, location string) (*parquet.File, error) {
	fs, readPath, err := icebergFileServiceForLocation(ctx, param, location)
	if err != nil {
		return nil, err
	}
	stat, err := fs.StatFile(ctx, readPath)
	if err != nil {
		return nil, icebergapi.ToMOErr(ctx, icebergapi.WrapError(icebergapi.ErrObjectIO, "Iceberg delete file stat failed", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}, err))
	}
	reader := &fsReaderAt{fs: fs, readPath: readPath, ctx: ctx, param: param}
	retainedMemory, memoryOpts := icebergDeleteFooterMemory(state, param)
	if err := validateIcebergDeleteParquetFooter(ctx, reader, stat.Size, retainedMemory, memoryOpts, location); err != nil {
		return nil, err
	}
	file, err := parquet.OpenFile(reader, stat.Size)
	if err != nil {
		return nil, icebergapi.ToMOErr(ctx, icebergapi.WrapError(icebergapi.ErrObjectIO, "Iceberg delete file open failed", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}, err))
	}
	return file, nil
}

func icebergDeleteFooterMemory(state *icebergdelete.ApplyState, param *ExternalParam) (retained int64, opts icebergdelete.Options) {
	if state != nil {
		opts = state.Options
		retained = saturatingIcebergDeleteMemoryAdd(state.Options.BaseMemoryBytes, state.MemoryBytes())
	}
	if opts.MaxMemoryBytes <= 0 {
		opts = icebergDeleteApplyOptions(param)
	}
	return retained, opts
}

func validateIcebergDeleteParquetFooter(ctx context.Context, reader io.ReaderAt, size, retainedMemory int64, memoryOpts icebergdelete.Options, location string) error {
	if reader == nil || size < 8 {
		return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg delete file has an invalid Parquet size", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}))
	}
	var trailer [8]byte
	n, err := reader.ReadAt(trailer[:], size-8)
	if err != nil && !errors.Is(err, io.EOF) {
		return icebergapi.ToMOErr(ctx, icebergapi.WrapError(icebergapi.ErrObjectIO, "Iceberg delete file trailer read failed", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}, err))
	}
	if n != len(trailer) || string(trailer[4:]) != "PAR1" {
		return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg delete file has an invalid Parquet trailer", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}))
	}
	footerBytes := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerBytes <= 0 || footerBytes > size-8 {
		return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg delete file has an invalid Parquet footer size", map[string]string{
			"delete_file": icebergapi.RedactPath(location),
		}))
	}
	// parquet-go expands compact Thrift footer metadata into a graph of maps,
	// slices and column objects. Charge a conservative factor before OpenFile is
	// allowed to allocate. A future streaming footer decoder can replace this
	// factor with exact token accounting and safely relax the rejection point.
	estimatedFooterMemory := saturatingIcebergDeleteMemoryAdd(0, footerBytes*64)
	return icebergdelete.CheckMemoryLimit(ctx, memoryOpts,
		saturatingIcebergDeleteMemoryAdd(retainedMemory, estimatedFooterMemory))
}

func icebergFileServiceForLocation(ctx context.Context, param *ExternalParam, location string) (fileservice.ETLFileService, string, error) {
	if strings.TrimSpace(param.IcebergObjectIORef) != "" {
		return icebergio.ResolveObjectIORef(ctx, param.IcebergObjectIORef, location)
	}
	return plan2.GetForETLWithType(param.Extern, location)
}

func readParquetRows(ctx context.Context, file *parquet.File, visit func(parquet.Row) error) (retErr error) {
	rowGroup := parquet.MultiRowGroup(file.RowGroups()...)
	rows := rowGroup.Rows()
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			retErr = errors.Join(retErr, icebergapi.ToMOErr(ctx, icebergapi.WrapError(
				icebergapi.ErrObjectIO,
				"Iceberg delete file row reader close failed",
				nil,
				closeErr,
			)))
		}
	}()
	buf := make([]parquet.Row, icebergDeleteReadBatchRows)
	for {
		n, err := rows.ReadRows(buf)
		for i := 0; i < n; i++ {
			if visitErr := visit(buf[i]); visitErr != nil {
				return visitErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return icebergapi.ToMOErr(ctx, icebergapi.WrapError(icebergapi.ErrObjectIO, "Iceberg delete file row read failed", nil, err))
		}
		if n == 0 {
			return moerr.NewInternalError(ctx, "iceberg delete file reader made no progress")
		}
	}
}

func parquetColumnIndexByName(file *parquet.File, names ...string) (int, bool) {
	if file == nil || file.Root() == nil {
		return 0, false
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	for _, col := range file.Root().Columns() {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(col.Name()))]; ok {
			return int(col.Index()), true
		}
	}
	return 0, false
}

type equalityDeleteColumn struct {
	index       int
	mapping     *pipeline.IcebergColumnMapping
	parquetType parquet.Type
}

func equalityDeleteColumns(ctx context.Context, file *parquet.File, task *pipeline.IcebergDeleteFileTask, mappings []*pipeline.IcebergColumnMapping) ([]equalityDeleteColumn, error) {
	fieldIDs := normalizedEqualityFieldIDs(task.EqualityFieldIds)
	columns := make([]equalityDeleteColumn, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		mapping := icebergColumnMappingForField(mappings, fieldID)
		if mapping == nil || mapping.MoType == nil {
			return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg equality delete field is missing MO column mapping", map[string]string{
				"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
				"field_id":    int32String(fieldID),
			}))
		}
		var idx int
		if foundIdx, found := parquetColumnIndexByFieldID(file, fieldID); found {
			// Use Iceberg field-id metadata when present.
			idx = foundIdx
		} else if foundIdx, found = parquetColumnIndexByFieldName(file, fieldID, mappings); found {
			// Legacy files without field ids fall back to snapshot/current names.
			idx = foundIdx
		} else {
			return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg equality delete file is missing equality field", map[string]string{
				"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
				"field_id":    int32String(fieldID),
			}))
		}
		parquetType, typeOK := parquetColumnTypeByIndex(file, idx)
		if !typeOK {
			return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg equality delete file column index is invalid", map[string]string{
				"delete_file": icebergapi.RedactPath(task.DeleteFilePath),
				"field_id":    int32String(fieldID),
				"column":      strconv.Itoa(idx),
			}))
		}
		columns = append(columns, equalityDeleteColumn{
			index:       idx,
			mapping:     mapping,
			parquetType: parquetType,
		})
	}
	return columns, nil
}

func icebergColumnMappingForField(mappings []*pipeline.IcebergColumnMapping, fieldID int32) *pipeline.IcebergColumnMapping {
	for _, mapping := range mappings {
		if mapping != nil && mapping.IcebergFieldId == fieldID {
			return mapping
		}
	}
	return nil
}

func parquetColumnTypeByIndex(file *parquet.File, idx int) (parquet.Type, bool) {
	if file == nil || file.Root() == nil {
		return nil, false
	}
	for _, col := range file.Root().Columns() {
		if int(col.Index()) == idx {
			return col.Type(), true
		}
	}
	return nil, false
}

func parquetColumnIndexByFieldID(file *parquet.File, fieldID int32) (int, bool) {
	if file == nil || file.Root() == nil {
		return 0, false
	}
	for _, col := range file.Root().Columns() {
		if int32(col.ID()) == fieldID {
			return int(col.Index()), true
		}
	}
	return 0, false
}

func parquetColumnIndexByFieldName(file *parquet.File, fieldID int32, mappings []*pipeline.IcebergColumnMapping) (int, bool) {
	for _, mapping := range mappings {
		if mapping == nil || mapping.IcebergFieldId != fieldID {
			continue
		}
		if idx, ok := parquetColumnIndexByName(file, mapping.SnapshotFieldName, mapping.CurrentFieldName); ok {
			return idx, true
		}
	}
	return 0, false
}

func parquetRowString(row parquet.Row, col int) (string, bool) {
	for _, value := range row {
		if value.Column() != col || value.IsNull() {
			continue
		}
		switch value.Kind() {
		case parquet.ByteArray, parquet.FixedLenByteArray:
			return string(value.ByteArray()), true
		default:
			return "", false
		}
	}
	return "", false
}

func parquetRowInt64(row parquet.Row, col int) (int64, bool) {
	for _, value := range row {
		if value.Column() != col || value.IsNull() {
			continue
		}
		switch value.Kind() {
		case parquet.Int64:
			return value.Int64(), true
		case parquet.Int32:
			return int64(value.Int32()), true
		default:
			return 0, false
		}
	}
	return 0, false
}

func parquetRowEqualityValue(ctx context.Context, proc *process.Process, row parquet.Row, col equalityDeleteColumn) (any, error) {
	for _, value := range row {
		if value.Column() != col.index {
			continue
		}
		if value.IsNull() {
			return nil, nil
		}
		return parquetValueEqualityValue(ctx, proc, col, value)
	}
	return nil, nil
}

func parquetValueEqualityValue(ctx context.Context, proc *process.Process, col equalityDeleteColumn, value parquet.Value) (any, error) {
	if moType := col.mapping.GetMoType(); moType != nil {
		switch types.T(moType.Id) {
		case types.T_date:
			return parquetDateEqualityValue(ctx, col.parquetType, value)
		case types.T_datetime:
			return parquetDatetimeEqualityValue(ctx, col.parquetType, value)
		case types.T_timestamp:
			return parquetTimestampEqualityValue(ctx, proc, col.parquetType, value)
		}
	}
	switch value.Kind() {
	case parquet.Boolean:
		return value.Boolean(), nil
	case parquet.Int32:
		return value.Int32(), nil
	case parquet.Int64:
		return value.Int64(), nil
	case parquet.Float:
		return value.Float(), nil
	case parquet.Double:
		return value.Double(), nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(value.ByteArray()), nil
	default:
		return value.String(), nil
	}
}

func parquetDateEqualityValue(ctx context.Context, st parquet.Type, value parquet.Value) (any, error) {
	if st == nil || value.Kind() != parquet.Int32 {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_date)
	}
	lt := st.LogicalType()
	if lt == nil || lt.Date == nil {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_date)
	}
	return int32(types.DaysFromUnixEpochToDate(value.Int32())), nil
}

func parquetDatetimeEqualityValue(ctx context.Context, st parquet.Type, value parquet.Value) (any, error) {
	if st == nil {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_datetime)
	}
	lt := st.LogicalType()
	if lt == nil {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_datetime)
	}
	if lt.Date != nil {
		if value.Kind() != parquet.Int32 {
			return nil, unsupportedIcebergEqualityType(ctx, st, types.T_datetime)
		}
		return int64(types.DaysFromUnixEpochToDate(value.Int32()).ToDatetime()), nil
	}
	ts := lt.Timestamp
	if ts == nil || value.Kind() != parquet.Int64 {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_datetime)
	}
	switch {
	case ts.Unit.Nanos != nil:
		return int64(types.Datetime(types.UnixNanoToTimestamp(value.Int64()))), nil
	case ts.Unit.Micros != nil:
		return int64(types.Datetime(types.UnixMicroToTimestamp(value.Int64()))), nil
	case ts.Unit.Millis != nil:
		return int64(types.Datetime(types.UnixMicroToTimestamp(value.Int64() * 1000))), nil
	default:
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_datetime)
	}
}

func parquetTimestampEqualityValue(ctx context.Context, proc *process.Process, st parquet.Type, value parquet.Value) (any, error) {
	if st == nil || value.Kind() != parquet.Int64 {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_timestamp)
	}
	lt := st.LogicalType()
	if lt == nil || lt.Timestamp == nil {
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_timestamp)
	}
	ts := lt.Timestamp
	loc := icebergTimestampLocation(proc)
	switch {
	case ts.Unit.Nanos != nil:
		offsetMicros := icebergEqualityTimestampOffsetMicros(loc, value.Int64()/1000, ts.IsAdjustedToUTC)
		return int64(types.UnixNanoToTimestamp(value.Int64() - offsetMicros*1000)), nil
	case ts.Unit.Micros != nil:
		offsetMicros := icebergEqualityTimestampOffsetMicros(loc, value.Int64(), ts.IsAdjustedToUTC)
		return int64(types.UnixMicroToTimestamp(value.Int64() - offsetMicros)), nil
	case ts.Unit.Millis != nil:
		offsetMicros := icebergEqualityTimestampOffsetMicros(loc, value.Int64()*1000, ts.IsAdjustedToUTC)
		return int64(types.UnixMicroToTimestamp((value.Int64() - offsetMicros/1000) * 1000)), nil
	default:
		return nil, unsupportedIcebergEqualityType(ctx, st, types.T_timestamp)
	}
}

func icebergEqualityTimestampOffsetMicros(loc *time.Location, localMicros int64, isAdjustedToUTC bool) int64 {
	if isAdjustedToUTC {
		return 0
	}
	return icebergLocalTimestampOffsetMicros(loc, localMicros)
}

func unsupportedIcebergEqualityType(ctx context.Context, st parquet.Type, moType types.T) error {
	parquetType := "<nil>"
	if st != nil {
		parquetType = st.String()
	}
	return icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrUnsupportedFeature, "Iceberg equality delete column type is unsupported", map[string]string{
		"type":         (&planpb.Type{Id: int32(moType)}).String(),
		"parquet_type": parquetType,
	}))
}

type icebergEqualityLayout struct {
	key      string
	fieldIDs []int32
}

func icebergEqualityRows(param *ExternalParam, bat *batch.Batch, fieldIDs []int32) ([][]any, error) {
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	columns := make([]int, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		colIdx, ok := icebergMOColumnIndexForField(param.IcebergColumns, fieldID)
		if !ok {
			return nil, icebergapi.ToMOErr(param.Ctx, icebergapi.NewError(icebergapi.ErrMetadataInvalid, "Iceberg equality delete field is not available in scan batch", map[string]string{
				"field_id": int32String(fieldID),
			}))
		}
		columns = append(columns, int(colIdx))
	}
	rows := make([][]any, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		values := make([]any, len(columns))
		for idx, colIdx := range columns {
			if colIdx < 0 || colIdx >= len(bat.Vecs) {
				return nil, moerr.NewInternalErrorf(param.Ctx, "invalid iceberg equality column index %d", colIdx)
			}
			value, err := vectorEqualityValue(param.Ctx, bat.Vecs[colIdx], row)
			if err != nil {
				return nil, err
			}
			values[idx] = value
		}
		rows[row] = values
	}
	return rows, nil
}

func icebergEqualityLayouts(tasks []*pipeline.IcebergDeleteFileTask) []icebergEqualityLayout {
	seen := make(map[string]struct{})
	out := make([]icebergEqualityLayout, 0)
	for _, task := range tasks {
		if task == nil || strings.ToLower(strings.TrimSpace(task.DeleteType)) != "equality" {
			continue
		}
		fieldIDs := normalizedEqualityFieldIDs(task.EqualityFieldIds)
		if len(fieldIDs) == 0 {
			continue
		}
		key := icebergEqualityLayoutKey(fieldIDs)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, icebergEqualityLayout{key: key, fieldIDs: fieldIDs})
	}
	return out
}

func normalizedEqualityFieldIDs(fieldIDs []int32) []int32 {
	seen := make(map[int32]struct{}, len(fieldIDs))
	out := make([]int32, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		if fieldID <= 0 {
			continue
		}
		if _, ok := seen[fieldID]; ok {
			continue
		}
		seen[fieldID] = struct{}{}
		out = append(out, fieldID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func icebergEqualityLayoutKey(fieldIDs []int32) string {
	fieldIDs = normalizedEqualityFieldIDs(fieldIDs)
	if len(fieldIDs) == 0 {
		return ""
	}
	parts := make([]string, len(fieldIDs))
	for idx, fieldID := range fieldIDs {
		parts[idx] = int32String(fieldID)
	}
	return strings.Join(parts, ",")
}

func icebergMOColumnIndexForField(mappings []*pipeline.IcebergColumnMapping, fieldID int32) (int32, bool) {
	for _, mapping := range mappings {
		if mapping != nil && mapping.IcebergFieldId == fieldID {
			return mapping.MoColIndex, true
		}
	}
	return 0, false
}

func vectorEqualityValue(ctx context.Context, vec *vector.Vector, row int) (any, error) {
	if vec == nil {
		return nil, nil
	}
	if vec.IsNull(uint64(row)) {
		return nil, nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.MustFixedColWithTypeCheck[bool](vec)[row], nil
	case types.T_int8:
		return vector.MustFixedColWithTypeCheck[int8](vec)[row], nil
	case types.T_int16:
		return vector.MustFixedColWithTypeCheck[int16](vec)[row], nil
	case types.T_int32:
		return vector.MustFixedColWithTypeCheck[int32](vec)[row], nil
	case types.T_int64:
		return vector.MustFixedColWithTypeCheck[int64](vec)[row], nil
	case types.T_uint8:
		return vector.MustFixedColWithTypeCheck[uint8](vec)[row], nil
	case types.T_uint16:
		return vector.MustFixedColWithTypeCheck[uint16](vec)[row], nil
	case types.T_uint32:
		return vector.MustFixedColWithTypeCheck[uint32](vec)[row], nil
	case types.T_uint64:
		return vector.MustFixedColWithTypeCheck[uint64](vec)[row], nil
	case types.T_float32:
		return vector.MustFixedColWithTypeCheck[float32](vec)[row], nil
	case types.T_float64:
		return vector.MustFixedColWithTypeCheck[float64](vec)[row], nil
	case types.T_date:
		return int32(vector.MustFixedColWithTypeCheck[types.Date](vec)[row]), nil
	case types.T_datetime:
		return int64(vector.MustFixedColWithTypeCheck[types.Datetime](vec)[row]), nil
	case types.T_timestamp:
		return int64(vector.MustFixedColWithTypeCheck[types.Timestamp](vec)[row]), nil
	case types.T_varchar, types.T_text, types.T_json:
		return vec.GetStringAt(row), nil
	default:
		return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrUnsupportedFeature, "Iceberg equality delete column type is unsupported", map[string]string{
			"type": vec.GetType().String(),
		}))
	}
}

func keepMaskSelections(keep []bool) []int64 {
	sels := make([]int64, 0, len(keep))
	for idx, ok := range keep {
		if ok {
			sels = append(sels, int64(idx))
		}
	}
	return sels
}

func (param *ExternalParam) addIcebergDeleteProfile(state *icebergdelete.ApplyState, before icebergdelete.Profile, rowsFiltered int64) {
	if param == nil || state == nil {
		return
	}
	delta := process.ParquetProfileStats{
		IcebergDeleteRowsFiltered:         rowsFiltered,
		IcebergPositionDeleteRowsFiltered: state.Profile.PositionRowsFiltered - before.PositionRowsFiltered,
		IcebergEqualityDeleteRowsFiltered: state.Profile.EqualityRowsFiltered - before.EqualityRowsFiltered,
		IcebergDeleteApplyPeakMemoryBytes: state.Profile.MemoryBytes,
	}
	param.addParquetProfile(delta)
}

func (param *ExternalParam) addIcebergDeleteLoadProfile(states map[string]*icebergdelete.ApplyState, totalMemoryBytes int64) {
	if param == nil || len(states) == 0 {
		return
	}
	var filesRead int64
	for _, state := range states {
		if state == nil {
			continue
		}
		filesRead += state.Profile.DeleteFilesRead
	}
	param.addParquetProfile(process.ParquetProfileStats{
		IcebergDeleteFilesRead:            filesRead,
		IcebergDeleteApplyPeakMemoryBytes: totalMemoryBytes,
	})
}

// maskIcebergHiddenReadColumns resets columns that were force-read only to
// satisfy delete apply (equality delete keys) back to NULL in place.
//
// These columns are not part of the user projection. A P0 read leaves a
// non-projected Iceberg column NULL-filled at the same batch position (see
// ParquetHandler.fillIcebergDefaultNullColumns), and downstream operators
// reference scan output columns by absolute position. So we must preserve the
// batch shape (column count and ordinals) rather than physically dropping
// columns, which would shift every column after a non-trailing hidden one and
// corrupt downstream projection. Restoring NULL keeps the force-read key values
// from leaking while matching the exact layout a delete-free scan produces.
func maskIcebergHiddenReadColumns(param *ExternalParam, bat *batch.Batch) {
	if param == nil || bat == nil || len(param.IcebergHiddenReadCols) == 0 {
		return
	}
	rowCount := bat.RowCount()
	if rowCount == 0 {
		return
	}
	for _, col := range param.IcebergHiddenReadCols {
		idx := int(col)
		if idx < 0 || idx >= len(bat.Vecs) || bat.Vecs[idx] == nil {
			continue
		}
		nulls.AddRange(bat.Vecs[idx].GetNulls(), 0, uint64(rowCount))
	}
}

func int32String(value int32) string {
	return strconv.FormatInt(int64(value), 10)
}
