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
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

func scanParquetFile() (*batch.Batch, error) {
	return nil, nil
}

func makeVecFromPage(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
	if len(page.DefinitionLevels()) != 0 || len(page.RepetitionLevels()) != 0 {
		return moerr.NewNYINoCtx("page has repetition or definition not support")
	}

	var err error
	data := page.Data()
	switch page.Type().Kind() {
	case parquet.Boolean:
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
		err = vector.AppendFixedList(vec, p, nil, proc.GetMPool())
	case parquet.Int32:
		err = vector.AppendFixedList(vec, data.Int32(), nil, proc.GetMPool())
	case parquet.Int64:
		err = vector.AppendFixedList(vec, data.Int64(), nil, proc.GetMPool())
	// case parquet.Int96:
	case parquet.Float:
		err = vector.AppendFixedList(vec, data.Float(), nil, proc.GetMPool())
	case parquet.Double:
		err = vector.AppendFixedList(vec, data.Double(), nil, proc.GetMPool())
	case parquet.ByteArray:
		buf, offsets := data.ByteArray()
		if len(offsets) > 0 {
			baseOffset := offsets[0]
			for _, endOffset := range offsets[1:] {
				err = vector.AppendBytes(vec, buf[baseOffset:endOffset:endOffset], false, proc.GetMPool())
				baseOffset = endOffset
				if err != nil {
					return moerr.NewInternalErrorNoCtx("")
				}
			}
		}
	case parquet.FixedLenByteArray:
		buf, size := data.FixedLenByteArray()
		for len(buf) > 0 {
			err = vector.AppendBytes(vec, buf[:size:size], false, proc.GetMPool())
			buf = buf[size:]
			if err != nil {
				return moerr.NewInternalErrorNoCtx("")
			}
		}
	default:
		return moerr.NewInternalErrorNoCtx("")
	}
	if err != nil {
		return moerr.NewInternalErrorNoCtx("")
	}
	return nil
}
