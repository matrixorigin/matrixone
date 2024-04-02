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
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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

	var h ParquetHandler
	err := h.openFile(param)
	if err != nil {
		return nil, err
	}

	err = h.prepare(param)
	if err != nil {
		return nil, err
	}

	for colIdx, col := range h.cols {
		vec := bat.Vecs[colIdx]
		if param.Cols[colIdx].Hidden {
			col := param.Cols[colIdx]
			typ := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
			bat.Vecs[colIdx] = vector.NewConstNull(typ, int(h.file.NumRows()), proc.GetMPool())
			continue
		}

		pages := col.Pages()
	L:
		for {
			page, err := pages.ReadPage()
			switch {
			case errors.Is(err, io.EOF):
				break L
			case err != nil:
				return nil, err
			}

			if len(page.DefinitionLevels()) != 0 || len(page.RepetitionLevels()) != 0 {
				return nil, moerr.NewNYINoCtx("page has repetition or definition not support")
			}

			err = h.dataFn[colIdx](page, proc, vec)
			if err != nil {
				return nil, err
			}
		}
		_ = pages.Close()
	}

	param.Fileparam.FileFin++
	if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
		param.Fileparam.End = true
	}

	n := bat.Vecs[0].Length()
	bat.SetRowCount(n)
	return bat, nil
}

func (h *ParquetHandler) openFile(param *ExternalParam) error {
	var r io.ReaderAt
	switch {
	case param.Extern.ScanType == tree.INLINE:
		r = bytes.NewReader(util.UnsafeStringToBytes(param.Extern.Data))
	case param.Extern.Local:
		return moerr.NewNYINoCtx("")
	default:
		fs, readPath, err := plan2.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
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
	return err
}

func (h *ParquetHandler) prepare(param *ExternalParam) error {
	h.cols = make([]*parquet.Column, len(param.Attrs))
	h.dataFn = make([]dataFn, len(param.Attrs))
	for colIdx, attr := range param.Attrs {
		def := param.Cols[colIdx]
		if def.Hidden {
			continue
		}
		col := h.file.Root().Column(attr)
		if col == nil {
			return moerr.NewInvalidInputNoCtx("column %s not found", attr)
		}

		h.cols[colIdx] = col
		h.dataFn[colIdx] = h.getDataFn(col.Type(), types.T(def.Typ.Id))
	}

	return nil
}

func (*ParquetHandler) getDataFn(st parquet.Type, dt types.T) dataFn {
	switch dt {
	case types.T_bool:
		if st == parquet.BooleanType {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				var err error
				p := make([]bool, page.NumValues())
				r := page.Values().(parquet.BooleanReader)
				var n int
				n, err = r.ReadBooleans(p)
				if err != nil && !errors.Is(err, io.EOF) {
					return moerr.NewInternalErrorNoCtx("")
				}
				if n != int(page.NumValues()) {
					return moerr.NewInternalErrorNoCtx("")
				}
				return vector.AppendFixedList(vec, p, nil, proc.GetMPool())
			}
		}
	case types.T_int32:
		if st == parquet.Int32Type {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Int32(), nil, proc.GetMPool())
			}
		}
	case types.T_int64:
		if st == parquet.Int64Type {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Int64(), nil, proc.GetMPool())
			}
		}
	case types.T_uint32:
		if st.Length() == 32 && st.Kind() == parquet.Int32 {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Uint32(), nil, proc.GetMPool())
			}
		}
	case types.T_uint64:
		if st.Length() == 64 && st.Kind() == parquet.Int64 {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Uint32(), nil, proc.GetMPool())
			}
		}
	case types.T_float32:
		if st == parquet.FloatType {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Float(), nil, proc.GetMPool())
			}
		}
	case types.T_float64:
		if st == parquet.DoubleType {
			return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				return vector.AppendFixedList(vec, data.Double(), nil, proc.GetMPool())
			}
		}
	case types.T_char, types.T_varchar:
		if st.PhysicalType() != nil {
			if st.Kind() == parquet.ByteArray {
				return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
					data := page.Data()
					buf, offsets := data.ByteArray()
					if len(offsets) > 0 {
						baseOffset := offsets[0]
						for _, endOffset := range offsets[1:] {
							err := vector.AppendBytes(vec, buf[baseOffset:endOffset:endOffset], false, proc.GetMPool())
							baseOffset = endOffset
							if err != nil {
								return moerr.NewInternalErrorNoCtx("")
							}
						}
					}
					return nil
				}
			}
			if st.Kind() == parquet.FixedLenByteArray {
				return func(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
					data := page.Data()
					buf, size := data.FixedLenByteArray()
					for len(buf) > 0 {
						err := vector.AppendBytes(vec, buf[:size:size], false, proc.GetMPool())
						buf = buf[size:]
						if err != nil {
							return moerr.NewInternalErrorNoCtx("")
						}
					}
					return nil
				}
			}
		}
	}
	return nil
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
