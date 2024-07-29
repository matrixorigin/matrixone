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
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

const BatchSize = 8192

var _ table.RowWriter = (*TAEWriter)(nil)

// TAEWriter implements table.RowWriter and writes data to a tae file.
// Deprecated
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

	flushRows int
}

// NewTAEWriter returns a new instance of TAEWriter
// Deprecated
func NewTAEWriter(ctx context.Context, tbl *table.Table, mp *mpool.MPool, filePath string, fs fileservice.FileService) *TAEWriter {
	w := &TAEWriter{
		ctx:       ctx,
		batchSize: BatchSize,
		mp:        mp,
		filename:  filePath,
		fs:        fs,
		rows:      make([]*table.Row, 0, BatchSize),
	}

	w.idxs = make([]uint16, len(tbl.Columns))
	for idx, c := range tbl.Columns {
		w.columnsTypes = append(w.columnsTypes, c.ColType.ToType())
		w.idxs[idx] = uint16(idx)
	}
	w.writer, _ = blockio.NewBlockWriter(fs, filePath)
	return w
}

func newBatch(batchSize int, typs []types.Type, pool *mpool.MPool) *batch.Batch {
	batch := batch.NewWithSize(len(typs))
	for i, typ := range typs {
		switch typ.Oid {
		case types.T_datetime:
			typ.Scale = 6
		}
		vec := vector.NewVec(typ)
		if err := vec.PreExtend(batchSize, pool); err != nil {
			panic(err)
		}
		vec.SetLength(batchSize)
		//vec.SetOriginal(false)
		batch.Vecs[i] = vec
	}
	return batch
}

func (w *TAEWriter) GetContent() string { return "" }

func (w *TAEWriter) GetContentLength() int { return 0 }

// WriteStrings implement ETLWriter
func (w *TAEWriter) WriteStrings(Line []string) error {
	var elems = make([]table.ColumnField, len(w.columnsTypes))
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
			elems[colIdx] = table.Int64Field(val)
		case types.T_uint64:
			val, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return moerr.NewInternalError(w.ctx, "the input value is not uint64 type for column %d: %v, err: %s", colIdx, field, err)
			}
			elems[colIdx] = table.Uint64Field(val)
		case types.T_float64:
			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return moerr.NewInternalError(w.ctx, "the input value is not float64 type for column %d: %v, err: %s", colIdx, field, err)
			}
			elems[colIdx] = table.Float64Field(val)
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
			//TAEWriter is deprecated. So no need to add T_array here.
			elems[colIdx] = table.StringField(field)
		case types.T_json:
			elems[colIdx] = table.StringField(field)
		case types.T_datetime:
			elems[colIdx] = table.StringField(field)
		default:
			elems[colIdx] = table.StringField(field)
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
		err := getOneRowData(w.ctx, batch, row.GetRawColumns(), rowId, w.columnsTypes, w.mp)
		if err != nil {
			return err
		}
	}
	_, err := w.writer.WriteBatch(batch)
	if err != nil {
		return err
	}
	// check if empty
	w.flushRows += len(w.rows)
	// clean
	for idx, row := range w.rows {
		row.Free()
		w.rows[idx] = nil
	}
	w.rows = w.rows[:0]
	batch.Clean(w.mp)
	return nil
}

func (w *TAEWriter) flush() error {
	err := w.writeBatch()
	if err != nil {
		return err
	}
	_, _, err = w.writer.Sync(w.ctx)
	if err != nil {
		return err
	}
	if w.flushRows == 0 {
		return moerr.NewEmptyRange(w.ctx, w.filename)
	}
	return nil
}

// FlushAndClose implement ETLWriter
func (w *TAEWriter) FlushAndClose() (int, error) {
	return 0, w.flush()
}

func getOneRowData(ctx context.Context, bat *batch.Batch, Line []table.ColumnField, rowIdx int, typs []types.Type, mp *mpool.MPool) error {

	for colIdx, typ := range typs {
		field := Line[colIdx]
		id := typ.Oid
		vec := bat.Vecs[colIdx]
		switch id {
		case types.T_int64:
			cols := vector.MustFixedCol[int64](vec)
			cols[rowIdx] = field.Integer
		case types.T_uint64:
			cols := vector.MustFixedCol[uint64](vec)
			cols[rowIdx] = uint64(field.Integer)
		case types.T_float64:
			cols := vector.MustFixedCol[float64](vec)
			cols[rowIdx] = field.GetFloat64()
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
			//TODO: How to handle T_array here?
			switch field.Type {
			case table.TVarchar, table.TText:
				err := vector.SetStringAt(vec, rowIdx, field.String, mp)
				if err != nil {
					return err
				}
			case table.TBytes:
				err := vector.SetBytesAt(vec, rowIdx, field.Bytes, mp)
				if err != nil {
					return err
				}
			case table.TUuid:
				dst := field.EncodeUuid()
				err := vector.SetBytesAt(vec, rowIdx, dst[:], mp)
				if err != nil {
					return err
				}
			default:
				return moerr.NewInternalError(ctx, "not Support string type %v", field.Type)
			}
		case types.T_json:
			switch field.Type {
			case table.TVarchar, table.TText:
				// convert normal json-string to bytejson-bytes
				jsonBytes, err := bytejson.ParseJsonByteFromString(field.String)
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not json type for column %d: %v", colIdx, field)
				}
				err = vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
				if err != nil {
					return err
				}
			case table.TBytes:
				val := field.Bytes
				if len(val) == 0 {
					val = util.UnsafeStringToBytes(field.String)
				}
				err := vector.SetBytesAt(vec, rowIdx, val, mp)
				if err != nil {
					return err
				}
			}

		case types.T_datetime:
			cols := vector.MustFixedCol[types.Datetime](vec)
			switch field.Type {
			case table.TDatetime:
				var buf [64]byte
				dst := field.EncodedDatetime(buf[:0])
				d, err := types.ParseDatetime(string(dst), vec.GetType().Scale)
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not Datetime type for column %d: %v", colIdx, field)
				}
				cols[rowIdx] = d
			case table.TVarchar, table.TText:
				datetimeStr := field.String
				if len(datetimeStr) == 0 {
					cols[rowIdx] = types.Datetime(0)
				} else {
					d, err := types.ParseDatetime(datetimeStr, vec.GetType().Scale)
					if err != nil {
						return moerr.NewInternalError(ctx, "the input value is not Datetime type for column %d: %v", colIdx, field)
					}
					cols[rowIdx] = d
				}
			default:
				return moerr.NewInternalError(ctx, "not Support datetime type %v", field.Type)
			}
		default:
			return moerr.NewInternalError(ctx, "the value type %s is not support now", *vec.GetType())
		}
	}
	return nil
}

// TAEReader implements the io.Reader interface for reading a tae file.
// Deprecated
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

	release func()
}

// NewTaeReader returns a TAEReader.
// Deprecated
func NewTaeReader(ctx context.Context, tbl *table.Table, filePath string, filesize int64, fs fileservice.FileService, mp *mpool.MPool) (*TAEReader, error) {
	var err error
	r := &TAEReader{
		ctx:      ctx,
		filepath: filePath,
		filesize: filesize,
		fs:       fs,
		mp:       mp,
	}
	r.idxs = make([]uint16, len(tbl.Columns))
	for idx, c := range tbl.Columns {
		r.typs = append(r.typs, c.ColType.ToType())
		r.idxs[idx] = uint16(idx)
	}
	r.blockReader, err = blockio.NewFileReaderNoCache(r.fs, r.filepath)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *TAEReader) ReadAll(ctx context.Context) ([]*batch.Batch, error) {
	if r.release != nil {
		panic("can only call once")
	}
	ioVec, release, err := r.blockReader.LoadAllColumns(ctx, r.idxs, r.mp)
	if err != nil {
		return nil, err
	}
	r.release = release
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
	for idx := range r.batchs {
		// do NOT release it in mpool (like r.batchs[idx].Clean(r.mp)). right now, the buffer is new one.
		r.batchs[idx] = nil
	}
	r.batchs = nil
	if r.release != nil {
		r.release()
	}
}

func GetVectorArrayLen(ctx context.Context, vec *vector.Vector) (int, error) {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_int64:
		cols := vector.MustFixedCol[int64](vec)
		return len(cols), nil
	case types.T_uint64:
		cols := vector.MustFixedCol[uint64](vec)
		return len(cols), nil
	case types.T_float64:
		cols := vector.MustFixedCol[float64](vec)
		return len(cols), nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		cols := vector.MustFixedCol[types.Varlena](vec)
		return len(cols), nil
	case types.T_json:
		cols := vector.MustFixedCol[types.Varlena](vec)
		return len(cols), nil
	case types.T_datetime:
		cols := vector.MustFixedCol[types.Datetime](vec)
		return len(cols), nil
	default:
		return 0, moerr.NewInternalError(ctx, "the value type with oid %d is not support now", vec.GetType().Oid)
	}
}

func ValToString(ctx context.Context, vec *vector.Vector, rowIdx int) (string, error) {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_int64:
		cols := vector.MustFixedCol[int64](vec)
		return fmt.Sprintf("%d", cols[rowIdx]), nil
	case types.T_uint64:
		cols := vector.MustFixedCol[uint64](vec)
		return fmt.Sprintf("%d", cols[rowIdx]), nil
	case types.T_float64:
		cols := vector.MustFixedCol[float64](vec)
		return fmt.Sprintf("%f", cols[rowIdx]), nil
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		cols, area := vector.MustVarlenaRawData(vec)
		return cols[rowIdx].UnsafeGetString(area), nil
	case types.T_array_float32:
		cols, area := vector.MustVarlenaRawData(vec)
		return types.ArrayToString[float32](types.GetArray[float32](&cols[rowIdx], area)), nil
	case types.T_array_float64:
		cols, area := vector.MustVarlenaRawData(vec)
		return types.ArrayToString[float64](types.GetArray[float64](&cols[rowIdx], area)), nil
	case types.T_json:
		cols, area := vector.MustVarlenaRawData(vec)
		val := cols[rowIdx].GetByteSlice(area)
		bjson := types.DecodeJson(val)
		return bjson.String(), nil
	case types.T_datetime:
		cols := vector.MustFixedCol[types.Datetime](vec)
		return table.Time2DatetimeString(cols[rowIdx].ConvertToGoTime(time.Local)), nil
	default:
		return "", moerr.NewInternalError(ctx, "the value type with oid %d is not support now", vec.GetType().Oid)
	}
}
