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

package writer

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"time"
)

const BatchSize = 8192

type TAEWriter struct {
	ctx          context.Context
	columnsTypes []types.Type
	idxs         []uint16
	batchSize    int
	mp           *mpool.MPool
	filename     string
	fs           fileservice.FileService
	//writer       objectio.Writer
	objectFS *objectio.ObjectFS
	writer   *blockio.Writer
	buffer   [][]any
}

func NewTAEWriter(ctx context.Context, tbl *table.Table, mp *mpool.MPool, filename string, fs fileservice.FileService) (*TAEWriter, error) {
	var err error
	w := &TAEWriter{
		ctx:       ctx,
		batchSize: BatchSize,
		mp:        mp,
		filename:  filename,
		fs:        fs,
		buffer:    make([][]any, 0, BatchSize),
	}

	w.idxs = make([]uint16, len(tbl.Columns))
	for idx, c := range tbl.Columns {
		w.columnsTypes = append(w.columnsTypes, c.ColType.ToType())
		w.idxs[idx] = uint16(idx)
	}
	w.objectFS = objectio.NewObjectFS(fs, "")
	w.writer = blockio.NewWriter(ctx, w.objectFS, filename)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func newBatch(batchSize int, typs []types.Type, pool *mpool.MPool) *batch.Batch {
	batch := batch.NewWithSize(len(typs))
	for i, typ := range typs {
		if typ.Oid == types.T_datetime {
			typ.Precision = 6
		}
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, pool)
		vec.SetOriginal(false)
		batch.Vecs[i] = vec
	}
	return batch
}

func (w *TAEWriter) WriteElems(line []any) error {
	w.buffer = append(w.buffer, line)
	if len(w.buffer) >= w.batchSize {
		if err := w.WriteBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (w *TAEWriter) WriteBatch() error {
	batch := newBatch(len(w.buffer), w.columnsTypes, w.mp)
	for rowId, line := range w.buffer {
		err := getOneRowData(w.ctx, batch, line, rowId, w.columnsTypes, w.mp)
		if err != nil {
			return err
		}
	}
	w.writer.WriteBlockAndZoneMap(batch, w.idxs)
	w.buffer = w.buffer[:0]
	return nil
}

func (w *TAEWriter) Flush() error {
	if len(w.buffer) > 0 {
		w.WriteBatch()
	}
	_, err := w.writer.Sync()
	if err != nil {
		return err
	}
	return nil
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
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
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
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(ctx, "the input value '%v' is not json type for column %d", field, colIdx)
				}
				jsonBytes, err := types.EncodeJson(byteJson)
				if err != nil {
					logutil.Errorf("encode json[%v] err:%v", field, err)
					return moerr.NewInternalError(ctx, "the input value '%v' is not json type for column %d", field, colIdx)
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
				d, err := types.ParseDatetime(datetimeStr, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", datetimeStr, err)
					return moerr.NewInternalError(ctx, "the input value '%v' is not Datetime type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
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
	typs         []types.Type
	batchs       []*batch.Batch
	filepath     string
	filesize     int64
	fs           fileservice.FileService
	objectReader objectio.Reader
}

func NewTaeReader(tbl *table.Table, filepath string, filesize int64, fs fileservice.FileService) (*TAEReader, error) {
	var err error
	r := &TAEReader{
		batchs:   []*batch.Batch{},
		filepath: filepath,
		filesize: filesize,
		fs:       fs,
	}
	for _, c := range tbl.Columns {
		r.typs = append(r.typs, c.ColType.ToType())
	}
	r.objectReader, err = objectio.NewObjectReader(r.filepath, r.fs)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *TAEReader) ReadAll(ctx context.Context, pool *mpool.MPool) ([]*batch.Batch, error) {
	var err error
	bs, err := r.objectReader.ReadAllMeta(context.Background(), r.filesize, pool)
	if err != nil {
		return nil, err
	}
	columnIdx := make([]uint16, len(r.typs))
	for idx := range columnIdx {
		columnIdx[idx] = uint16(idx)
	}
	for _, bss := range bs {
		ioVec, err := r.objectReader.Read(context.Background(), bss.GetExtent(), columnIdx, pool)
		if err != nil {
			return nil, err
		}
		batch := batch.NewWithSize(len(r.typs))
		for idx, entry := range ioVec.Entries {
			vec := newVector(r.typs[idx], entry.Object.([]byte))
			batch.Vecs[idx] = vec
		}
		r.batchs = append(r.batchs, batch)
	}

	return r.batchs, nil
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
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
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
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
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

func newVector(tye types.Type, buf []byte) *vector.Vector {
	vector := vector.New(tye)
	vector.Read(buf)
	return vector
}
