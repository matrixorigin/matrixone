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

package external

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

func scanParquetFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	_, span := trace.Start(ctx, "scanParquetFile")
	defer span.End()

	bat := batch.New(false, param.Attrs)
	for i := range param.Attrs {
		col := param.Cols[i]

		if col.Hidden {
			continue
		}

		bat.Vecs[i] = proc.GetVector(types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale))
	}

	if param.parqh == nil {
		var err error
		param.parqh, err = newParquetHandler(param)
		if err != nil {
			return nil, err
		}
	}

	err := param.parqh.getData(bat, param, proc)
	if err != nil {
		return nil, err
	}

	return bat, nil
}

var parquetBatchCnt int64 = 1000

func newParquetHandler(param *ExternalParam) (*ParquetHandler, error) {
	h := ParquetHandler{
		batchCnt: parquetBatchCnt,
	}
	err := h.openFile(param)
	if err != nil {
		return nil, err
	}

	err = h.prepare(param)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func (h *ParquetHandler) openFile(param *ExternalParam) error {
	var r io.ReaderAt
	switch {
	case param.Extern.ScanType == tree.INLINE:
		r = bytes.NewReader(util.UnsafeStringToBytes(param.Extern.Data))
	case param.Extern.Local:
		return moerr.NewNYI(param.Ctx, "unsupported load parquet local now")
	default:
		fs, readPath, err := plan.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
		if err != nil {
			return err
		}
		r = &fsReaderAt{
			fs:       fs,
			readPath: readPath,
			ctx:      param.Ctx,
		}
	}
	var err error
	h.file, err = parquet.OpenFile(r, param.FileSize[param.Fileparam.FileIndex-1])
	return moerr.ConvertGoError(param.Ctx, err)
}

func (h *ParquetHandler) prepare(param *ExternalParam) error {
	h.cols = make([]*parquet.Column, len(param.Attrs))
	h.mappers = make([]*columnMapper, len(param.Attrs))
	for colIdx, attr := range param.Attrs {
		def := param.Cols[colIdx]
		if def.Hidden {
			continue
		}

		col := h.file.Root().Column(attr)
		if col == nil {
			return moerr.NewInvalidInput(param.Ctx, "column %s not found", attr)
		}
		if !col.Leaf() {
			return moerr.NewNYI(param.Ctx, "can't load group column %s", attr)
		}

		h.cols[colIdx] = col
		fn := h.getMapper(col, def.Typ)
		if fn == nil {
			st := col.Type().String()
			if col.Optional() {
				st += "(optional)"
			} else {
				st += "(required)"
			}
			dt := types.T(def.Typ.Id).String()
			if def.NotNull {
				dt += " NOT NULL"
			} else {
				dt += " NULL"
			}
			return moerr.NewNYI(param.Ctx, "load %s to %s", st, dt)
		}
		h.mappers[colIdx] = fn
	}

	return nil
}

func (*ParquetHandler) getMapper(sc *parquet.Column, dt plan.Type) *columnMapper {
	st := sc.Type()
	if st.PhysicalType() == nil {
		return nil
	}
	if sc.Optional() && dt.NotNullable {
		return nil
	}

	mp := &columnMapper{
		srcNull:            sc.Optional(),
		dstNull:            !dt.NotNullable,
		maxDefinitionLevel: byte(sc.MaxDefinitionLevel()),
	}
	switch types.T(dt.Id) {
	case types.T_bool:
		if st.Kind() == parquet.Boolean {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				p := make([]bool, page.NumValues())
				r := page.Values().(parquet.BooleanReader)
				n, err := r.ReadBooleans(p)
				if err != nil && !errors.Is(err, io.EOF) {
					return moerr.ConvertGoError(proc.Ctx, err)
				}
				if n != int(page.NumValues()) {
					return moerr.NewInternalError(proc.Ctx, "short read bool")
				}
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, p, nil, proc.Mp())
				}
				return appendFixed(vec, p, isNulls, proc.Mp())
			}
		}
	case types.T_int32:
		if st.Kind() == parquet.Int32 {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Int32(), nil, proc.Mp())
				}
				return appendFixed(vec, data.Int32(), isNulls, proc.Mp())
			}
		}
	case types.T_int64:
		if st.Kind() == parquet.Int64 {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Int64(), nil, proc.Mp())
				}
				return appendFixed(vec, data.Int64(), isNulls, proc.Mp())
			}
		}
	case types.T_uint32:
		if st.Kind() == parquet.Int32 {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Uint32(), nil, proc.Mp())
				}
				return appendFixed(vec, data.Uint32(), isNulls, proc.Mp())
			}
		}
	case types.T_uint64:
		if st.Kind() == parquet.Int64 {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Uint64(), nil, proc.Mp())
				}
				return appendFixed(vec, data.Uint64(), isNulls, proc.Mp())
			}
		}
	case types.T_float32:
		if st.Kind() == parquet.Float {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Float(), nil, proc.Mp())
				}
				return appendFixed(vec, data.Float(), isNulls, proc.Mp())
			}
		}
	case types.T_float64:
		if st.Kind() == parquet.Double {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				isNulls, err := mp.pageIsNulls(proc.Ctx, page)
				if err != nil {
					return err
				}
				if len(isNulls) == 0 {
					return vector.AppendFixedList(vec, data.Double(), nil, proc.GetMPool())
				}
				return appendFixed(vec, data.Double(), isNulls, proc.Mp())
			}
		}
	case types.T_char, types.T_varchar:
		if st.Kind() == parquet.ByteArray {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				buf, offsets := data.ByteArray()
				if len(offsets) > 0 {
					baseOffset := offsets[0]
					j := 1
					for i := 0; i < int(page.NumRows()); i++ {
						isNull, err := mp.pageIsNull(proc.Ctx, page, i)
						if err != nil {
							return err
						}
						if isNull {
							err = vector.AppendBytes(vec, nil, true, proc.GetMPool())
						} else {
							endOffset := offsets[j]
							err = vector.AppendBytes(vec, buf[baseOffset:endOffset:endOffset], false, proc.GetMPool())
							baseOffset = endOffset
							j++
						}
						if err != nil {
							return err
						}
					}
				}
				return nil
			}
		}
		if st.Kind() == parquet.FixedLenByteArray {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				buf, size := data.FixedLenByteArray()
				i := 0
				for len(buf) > 0 {
					isNull, err := mp.pageIsNull(proc.Ctx, page, i)
					if err != nil {
						return err
					}
					if isNull {
						err = vector.AppendBytes(vec, nil, true, proc.GetMPool())
					} else {
						err = vector.AppendBytes(vec, buf[:size:size], false, proc.GetMPool())
						buf = buf[size:]
					}
					if err != nil {
						return err
					}
				}
				return nil
			}
			return mp
		}
	}
	if mp.mapper != nil {
		return mp
	}
	return nil
}

func appendFixed[T any](vec *vector.Vector, ls []T, isNulls []bool, mp *mpool.MPool) error {
	err := vec.PreExtend(len(isNulls), mp)
	if err != nil {
		return err
	}
	j := 0
	for _, null := range isNulls {
		if null {
			vector.AppendFixed(vec, 0, true, mp)
		} else {
			vector.AppendFixed(vec, ls[j], false, mp)
			j++
		}
	}
	return nil
}

func (h *ParquetHandler) getData(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	length := 0
	finish := false
	for colIdx, col := range h.cols {
		if param.Cols[colIdx].Hidden {
			continue
		}

		vec := bat.Vecs[colIdx]
		pages := col.Pages()
		n := h.batchCnt
		o := h.offset
	L:
		for n > 0 {
			page, err := pages.ReadPage()
			switch {
			case errors.Is(err, io.EOF):
				finish = true
				break L
			case err != nil:
				return moerr.ConvertGoError(param.Ctx, err)
			}

			nr := page.NumRows()
			if nr < o {
				o -= nr
				continue
			}

			if len(page.RepetitionLevels()) != 0 {
				return moerr.NewNYI(param.Ctx, "page has repetition")
			}

			if o > 0 || o+n < nr {
				page = page.Slice(o, min(n+o, nr))
			}
			o = 0
			n -= page.NumRows()

			err = h.mappers[colIdx].mapping(page, proc, vec)
			if err != nil {
				return err
			}
		}
		err := pages.Close()
		if err != nil {
			return moerr.ConvertGoError(param.Ctx, err)
		}
		length = vec.Length()
	}

	for i := range h.cols {
		if !param.Cols[i].Hidden {
			continue
		}
		col := param.Cols[i]
		typ := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
		bat.Vecs[i] = vector.NewConstNull(typ, length, proc.GetMPool())
	}
	bat.SetRowCount(length)

	if finish {
		param.parqh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
	} else {
		h.offset += int64(length)
	}

	return nil
}

func (mp *columnMapper) pageIsNulls(ctx context.Context, page parquet.Page) ([]bool, error) {
	if !mp.srcNull || !mp.dstNull {
		return nil, nil
	}
	if page.NumNulls() == 0 {
		return nil, nil
	}
	def := page.DefinitionLevels()
	if len(def) != int(page.NumRows()) {
		return nil, moerr.NewInvalidInput(ctx, "malformed page")
	}
	isNulls := make([]bool, len(def))
	for i, level := range def {
		isNulls[i] = level != mp.maxDefinitionLevel
	}
	return isNulls, nil
}

func (mp *columnMapper) pageIsNull(ctx context.Context, page parquet.Page, index int) (bool, error) {
	if !mp.srcNull || !mp.dstNull {
		return false, nil
	}
	if page.NumNulls() == 0 {
		return false, nil
	}
	levels := page.DefinitionLevels()
	if len(levels) != int(page.NumRows()) {
		return false, moerr.NewInvalidInput(ctx, "malformed page")
	}
	return levels[index] != mp.maxDefinitionLevel, nil
}

func (mp *columnMapper) mapping(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
	return mp.mapper(mp, page, proc, vec)
}

type fsReaderAt struct {
	fs       fileservice.ETLFileService
	readPath string
	ctx      context.Context
}

func (r *fsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	vec := fileservice.IOVector{
		FilePath: r.readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset: off,
				Size:   int64(len(p)),
				Data:   p,
			},
		},
	}

	err = r.fs.Read(r.ctx, &vec)
	if err != nil {
		return 0, err
	}
	return int(vec.Entries[0].Size), nil
}
