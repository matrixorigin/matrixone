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

package etl

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

const BatchSize = 8192

var _ table.RowWriter = (*TAEWriter)(nil)

type TAEWriter struct {
	ctx          context.Context
	columnsTypes []types.Type
	idxs         []uint16
	batchSize    int
	mp           *mpool.MPool
	filename     string
	fs           fileservice.FileService
	//writer       objectio.Writer
	writer *blockio.BlockWriter
	rows   []*table.Row
}

func NewTAEWriter(ctx context.Context, tbl *table.Table, mp *mpool.MPool, filePath string, fs fileservice.FileService) *TAEWriter {
	filename := defines.ETLFileServiceName + fileservice.ServiceNameSeparator + filePath
	w := &TAEWriter{
		ctx:       ctx,
		batchSize: BatchSize,
		mp:        mp,
		filename:  filename,
		fs:        fs,
		rows:      make([]*table.Row, 0, BatchSize),
	}

	w.idxs = make([]uint16, len(tbl.Columns))
	for idx, c := range tbl.Columns {
		w.columnsTypes = append(w.columnsTypes, c.ColType.ToType())
		w.idxs[idx] = uint16(idx)
	}
	w.writer, _ = blockio.NewBlockWriter(fs, filename)
	return w
}

func newBatch(batchSize int, typs []types.Type, pool *mpool.MPool) *batch.Batch {
	batch := batch.NewWithSize(len(typs))
	for i, typ := range typs {
		switch typ.Oid {
		case types.T_datetime:
			typ.Scale = 6
		}
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, pool)
		vec.SetOriginal(false)
		batch.Vecs[i] = vec
	}
	return batch
}

func (w *TAEWriter) GetContent() string { return "" }

// WriteStrings implement ETLWriter
func (w *TAEWriter) WriteStrings(Line []string) error {
	var elems = make([]any, len(w.columnsTypes))
	for colIdx, typ := range w.columnsTypes {
		field := Line[colIdx]
		id := typ.Oid
		switch id {
		case types.T_int64:
			val, err := strconv.ParseInt(field, 10, 64)
			if err != nil {
				// fixme: help merge to continue
				return moerr.NewInternalError(w.ctx, "the input value is not int64 type for column %d: %v, err: %s", colIdx, field, err)
			}
			elems[colIdx] = val
		case types.T_uint64:
			val, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return moerr.NewInternalError(w.ctx, "the input value is not uint64 type for column %d: %v, err: %s", colIdx, field, err)
			}
			elems[colIdx] = val
		case types.T_float64:
			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return moerr.NewInternalError(w.ctx, "the input value is not float64 type for column %d: %v, err: %s", colIdx, field, err)
			}
			elems[colIdx] = val
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			elems[colIdx] = field
		case types.T_json:
			elems[colIdx] = field
		case types.T_datetime:
			elems[colIdx] = field
		default:
			elems[colIdx] = field
		}
	}
	row := table.NewRow()
	row.Columns = elems
	w.rows = append(w.rows, row)
	return w.writeRows()
}

// WriteRow implement ETLWriter
func (w *TAEWriter) WriteRow(row *table.Row) error {
	w.rows = append(w.rows, row.Clone())
	return w.writeRows()
}

func (w *TAEWriter) writeRows() error {
	if len(w.rows) >= w.batchSize {
		if err := w.writeBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (w *TAEWriter) writeBatch() error {
	if len(w.rows) == 0 {
		return nil
	}
	batch := newBatch(len(w.rows), w.columnsTypes, w.mp)
	for rowId, row := range w.rows {
		err := getOneRowData(w.ctx, batch, row.GetRawColumn(), rowId, w.columnsTypes, w.mp)
		if err != nil {
			return err
		}
	}
	_, err := w.writer.WriteBatch(batch)
	if err != nil {
		return err
	}
	// clean
	for _, row := range w.rows {
		row.Free()
	}
	w.rows = w.rows[:0]
	batch.Clean(w.mp)
	return nil
}

func (w *TAEWriter) flush() error {
	w.writeBatch()
	_, _, err := w.writer.Sync(w.ctx)
	if err != nil {
		return err
	}
	return nil
}

// FlushAndClose implement ETLWriter
func (w *TAEWriter) FlushAndClose() (int, error) {
	return 0, w.flush()
}

func getOneRowData(ctx context.Context, bat *batch.Batch, Line []any, rowIdx int, typs []types.Type, mp *mpool.MPool) error {

	for colIdx, typ := range typs {
		field := Line[colIdx]
		id := typ.Oid
		vec := bat.Vecs[colIdx]
		switch id {
		case types.T_int64:
			cols := vector.MustTCols[int64](vec)
			switch t := field.(type) {
			case int32:
				cols[rowIdx] = (int64)(field.(int32))
			case int64:
				cols[rowIdx] = field.(int64)
			default:
				panic(moerr.NewInternalError(ctx, "not Support integer type %v", t))
			}
		case types.T_uint64:
			cols := vector.MustTCols[uint64](vec)
			switch t := field.(type) {
			case int32:
				cols[rowIdx] = (uint64)(field.(int32))
			case int64:
				cols[rowIdx] = (uint64)(field.(int64))
			case uint32:
				cols[rowIdx] = (uint64)(field.(uint32))
			case uint64:
				cols[rowIdx] = field.(uint64)
			default:
				panic(moerr.NewInternalError(ctx, "not Support integer type %v", t))
			}
		case types.T_float64:
			cols := vector.MustTCols[float64](vec)

			switch t := field.(type) {
			case float64:
				cols[rowIdx] = field.(float64)
			default:
				panic(moerr.NewInternalError(ctx, "not Support float64 type %v", t))
			}
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			switch t := field.(type) {
			case string:
				err := vector.SetStringAt(vec, rowIdx, field.(string), mp)
				if err != nil {
					return err
				}
			default:
				panic(moerr.NewInternalError(ctx, "not Support string type %v", t))
			}
		case types.T_json:
			switch t := field.(type) {
			case string:
				byteJson, err := types.ParseStringToByteJson(field.(string))
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not json type for column %d: %v", colIdx, field)
				}
				jsonBytes, err := types.EncodeJson(byteJson)
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not json type for column %d: %v", colIdx, field)
				}
				err = vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
				if err != nil {
					return err
				}
			default:
				panic(moerr.NewInternalError(ctx, "not Support json type %v", t))
			}

		case types.T_datetime:
			cols := vector.MustTCols[types.Datetime](vec)
			switch t := field.(type) {
			case time.Time:
				datetimeStr := Time2DatetimeString(field.(time.Time))
				d, err := types.ParseDatetime(datetimeStr, vec.Typ.Scale)
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not Datetime type for column %d: %v", colIdx, field)
				}
				cols[rowIdx] = d
			case string:
				datetimeStr := field.(string)
				if len(datetimeStr) == 0 {
					cols[rowIdx] = types.Datetime(0)
				} else {
					d, err := types.ParseDatetime(datetimeStr, vec.Typ.Scale)
					if err != nil {
						return moerr.NewInternalError(ctx, "the input value is not Datetime type for column %d: %v", colIdx, field)
					}
					cols[rowIdx] = d
				}
			default:
				panic(moerr.NewInternalError(ctx, "not Support datetime type %v", t))
			}
		default:
			return moerr.NewInternalError(ctx, "the value type %s is not support now", vec.Typ)
		}
	}
	return nil
}

type TAEReader struct {
	ctx      context.Context
	filepath string
	filesize int64
	fs       fileservice.FileService
	mp       *mpool.MPool
	typs     []types.Type
	idxs     []uint16

	blockReader *blockio.BlockReader

	bs       []objectio.BlockObject
	batchs   []*batch.Batch
	batchIdx int
	rowIdx   int
}

func NewTaeReader(ctx context.Context, tbl *table.Table, filePath string, filesize int64, fs fileservice.FileService, mp *mpool.MPool) (*TAEReader, error) {
	var err error
	path := defines.ETLFileServiceName + fileservice.ServiceNameSeparator + filePath
	r := &TAEReader{
		ctx:      ctx,
		filepath: path,
		filesize: filesize,
		fs:       fs,
		mp:       mp,
	}
	r.idxs = make([]uint16, len(tbl.Columns))
	for idx, c := range tbl.Columns {
		r.typs = append(r.typs, c.ColType.ToType())
		r.idxs[idx] = uint16(idx)
	}
	r.blockReader, err = blockio.NewFileReader(r.fs, r.filepath)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *TAEReader) ReadAll(ctx context.Context) ([]*batch.Batch, error) {
	ioVec, err := r.blockReader.LoadAllColumns(ctx, r.idxs, r.filesize, r.mp)
	if err != nil {
		return nil, err
	}
	r.batchs = append(r.batchs, ioVec...)
	return r.batchs, nil
}

func (r *TAEReader) ReadLine() ([]string, error) {
	var record = make([]string, len(r.idxs))
	if r.batchIdx >= len(r.batchs) {
		return nil, nil
	}
	if r.rowIdx >= r.batchs[r.batchIdx].Vecs[0].Length() {
		r.batchIdx++
		r.rowIdx = 0
	}
	if r.batchIdx >= len(r.batchs) || r.rowIdx >= r.batchs[r.batchIdx].Vecs[0].Length() {
		return nil, nil
	}
	vecs := r.batchs[r.batchIdx].Vecs
	for idx, vecIdx := range r.idxs {
		val, err := ValToString(r.ctx, vecs[vecIdx], r.rowIdx)
		if err != nil {
			return nil, err
		}
		record[idx] = val
	}
	r.rowIdx++
	return record, nil
}

func (r *TAEReader) ReadRow(row *table.Row) error {
	panic("NOT implement")
}

func (r *TAEReader) Close() {
	for _, b := range r.batchs {
		b.Clean(r.mp)
	}
}

func GetVectorArrayLen(ctx context.Context, vec *vector.Vector) (int, error) {
	typ := vec.Typ
	switch typ.Oid {
	case types.T_int64:
		cols := vector.MustTCols[int64](vec)
		return len(cols), nil
	case types.T_uint64:
		cols := vector.MustTCols[uint64](vec)
		return len(cols), nil
	case types.T_float64:
		cols := vector.MustTCols[float64](vec)
		return len(cols), nil
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		cols := vector.MustTCols[types.Varlena](vec)
		return len(cols), nil
	case types.T_json:
		cols := vector.MustTCols[types.Varlena](vec)
		return len(cols), nil
	case types.T_datetime:
		cols := vector.MustTCols[types.Datetime](vec)
		return len(cols), nil
	default:
		return 0, moerr.NewInternalError(ctx, "the value type %d is not support now", vec.Typ)
	}
}

func ValToString(ctx context.Context, vec *vector.Vector, rowIdx int) (string, error) {
	typ := vec.Typ
	switch typ.Oid {
	case types.T_int64:
		cols := vector.MustTCols[int64](vec)
		return fmt.Sprintf("%d", cols[rowIdx]), nil
	case types.T_uint64:
		cols := vector.MustTCols[uint64](vec)
		return fmt.Sprintf("%d", cols[rowIdx]), nil
	case types.T_float64:
		cols := vector.MustTCols[float64](vec)
		return fmt.Sprintf("%f", cols[rowIdx]), nil
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		cols, area := vector.MustVarlenaRawData(vec)
		return cols[rowIdx].GetString(area), nil
	case types.T_json:
		cols, area := vector.MustVarlenaRawData(vec)
		val := cols[rowIdx].GetByteSlice(area)
		bjson := types.DecodeJson(val)
		return bjson.String(), nil
	case types.T_datetime:
		cols := vector.MustTCols[types.Datetime](vec)
		return Time2DatetimeString(cols[rowIdx].ConvertToGoTime(time.Local)), nil
	default:
		return "", moerr.NewInternalError(ctx, "the value type %d is not support now", vec.Typ)
	}
}

const timestampFormatter = "2006-01-02 15:04:05.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
