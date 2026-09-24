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
	"context"
	"errors"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parquetNestedTestRows struct {
	seekErr  error
	readErr  error
	readRows int
	readEOF  bool
	readSeq  []int
	readCall int
	seekRows []int64
}

func (r *parquetNestedTestRows) ReadRows(rows []parquet.Row) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	n := r.readRows
	if r.readCall < len(r.readSeq) {
		n = r.readSeq[r.readCall]
	}
	r.readCall++
	if n > len(rows) {
		return n, nil
	}
	if r.readEOF {
		return n, io.EOF
	}
	return n, nil
}

type parquetNestedTestPages struct {
	page     parquet.Page
	readErr  error
	closeErr error
	closed   bool
}

func (p *parquetNestedTestPages) ReadPage() (parquet.Page, error) {
	if p.readErr != nil {
		return nil, p.readErr
	}
	if p.page == nil {
		return nil, io.EOF
	}
	page := p.page
	p.page = nil
	return page, nil
}

func (*parquetNestedTestPages) SeekToRow(int64) error { return nil }

func (p *parquetNestedTestPages) Close() error {
	p.closed = true
	return p.closeErr
}

func (r *parquetNestedTestRows) SeekToRow(row int64) error {
	r.seekRows = append(r.seekRows, row)
	return r.seekErr
}

func (*parquetNestedTestRows) Close() error { return nil }

func (*parquetNestedTestRows) Schema() *parquet.Schema {
	return parquet.NewSchema("test", parquet.Group{})
}

func TestParquetHybridReaderBoundaryErrors(t *testing.T) {
	proc := testutil.NewProc(t)
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background()}}

	tests := []struct {
		name      string
		configure func(*ParquetHandler, *process.Process)
		wantErr   string
	}{
		{
			name: "empty batch",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 0
			},
		},
		{
			name: "missing row reader",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
			},
			wantErr: "hybrid mode has no row reader",
		},
		{
			name: "seek failure",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
				h.rowReader = &parquetNestedTestRows{seekErr: errors.New("seek failed")}
			},
			wantErr: "seek failed",
		},
		{
			name: "read failure",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
				h.rowReader = &parquetNestedTestRows{readErr: errors.New("read failed")}
			},
			wantErr: "read failed",
		},
		{
			name: "invalid row count",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
				h.rowReader = &parquetNestedTestRows{readRows: 2}
			},
			wantErr: "returned 2 rows for buffer of 1",
		},
		{
			name: "empty read",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
				h.rowReader = &parquetNestedTestRows{}
			},
		},
		{
			name: "canceled while materializing",
			configure: func(h *ParquetHandler, p *process.Process) {
				h.batchCnt = 1
				h.rowReader = &parquetNestedTestRows{readRows: 1}
				ctx, cancel := context.WithCancel(p.Ctx)
				cancel()
				p.Ctx = ctx
			},
			wantErr: context.Canceled.Error(),
		},
		{
			name: "eof closes at row group boundary",
			configure: func(h *ParquetHandler, _ *process.Process) {
				h.batchCnt = 1
				h.rowGroupRows = 1
				h.rowReader = &parquetNestedTestRows{readRows: 1, readEOF: true}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Use a fresh process because the cancellation case changes its context.
			p := proc
			if test.name == "canceled while materializing" {
				p = testutil.NewProc(t)
			}
			h := &ParquetHandler{}
			test.configure(h, p)
			bat := batch.NewWithSize(0)
			t.Cleanup(func() {
				bat.Clean(p.Mp())
				h.cleanup()
			})

			err := h.getDataByRowAndPage(bat, param, p)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}

func TestParquetHybridReaderPageBoundaries(t *testing.T) {
	proc := testutil.NewProc(t)

	newScalarPage := func(t *testing.T, rows int) (*parquet.File, parquet.Page, *columnMapper) {
		t.Helper()
		values := make([]parquet.Row, rows)
		for i := range values {
			values[i] = parquet.Row{parquet.Int64Value(int64(i+1)).Level(0, 0, 0)}
		}
		file, page := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int64Type), values)
		mapper := (&ParquetHandler{}).getMapper(file.Root().Column("c"), plan.Type{
			Id: int32(types.T_int64), NotNullable: true,
		})
		require.NotNil(t, mapper)
		return file, page, mapper
	}

	tests := []struct {
		name    string
		setup   func(*testing.T, *ParquetHandler, *ExternalParam, *process.Process, *batch.Batch)
		wantErr string
	}{
		{
			name: "page reader error",
			setup: func(_ *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				h.batchCnt = 1
				h.rowGroupRows = 1
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{readErr: errors.New("page failed")}}
				h.rowReader = &parquetNestedTestRows{readRows: 1}
			},
			wantErr: "page failed",
		},
		{
			name: "page eof at empty row group",
			setup: func(_ *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				h.batchCnt = 1
				h.rowGroupRows = 0
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{}}
				h.rowReader = &parquetNestedTestRows{}
			},
		},
		{
			name: "page eof before row group end",
			setup: func(_ *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				h.batchCnt = 1
				h.rowGroupRows = 1
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{}}
				h.rowReader = &parquetNestedTestRows{}
			},
			wantErr: "page columns ended after 0 rows, expected 1",
		},
		{
			name: "repeated scalar page rejected",
			setup: func(t *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				file, page := writeListAndGetPage(t, parquet.Leaf(parquet.Int64Type), []parquet.Row{{
					parquet.Int64Value(1).Level(0, 1, 0),
				}})
				require.NotNil(t, file)
				h.batchCnt = 1
				h.rowGroupRows = 1
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{page: page}}
				h.mappers = []*columnMapper{{}}
				h.rowReader = &parquetNestedTestRows{readRows: 1}
			},
			wantErr: "page has repetition",
		},
		{
			name: "page row count mismatch",
			setup: func(t *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				_, page, _ := newScalarPage(t, 1)
				h.batchCnt = 1
				h.rowGroupRows = 0
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{page: page}}
				h.mappers = []*columnMapper{{}}
				h.rowReader = &parquetNestedTestRows{readRows: 1}
			},
			wantErr: "page: 1 rows remain after row 0, but row group has 0 rows",
		},
		{
			name: "page mapper error",
			setup: func(t *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				_, page, _ := newScalarPage(t, 1)
				h.batchCnt = 1
				h.rowGroupRows = 1
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{page: page}}
				h.mappers = []*columnMapper{{mapper: func(*columnMapper, parquet.Page, *process.Process, *vector.Vector) error {
					return errors.New("map failed")
				}}}
				h.rowReader = &parquetNestedTestRows{readRows: 1}
			},
			wantErr: "map failed",
		},
		{
			name: "byte budget rewinds unread rows",
			setup: func(t *testing.T, h *ParquetHandler, p *ExternalParam, _ *process.Process, _ *batch.Batch) {
				file, page, mapper := newScalarPage(t, 3)
				h.batchCnt = 3
				h.rowGroupRows = 3
				h.dataColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{page: page}}
				h.mappers = []*columnMapper{mapper}
				h.cols = []*parquet.Column{file.Root().Column("c")}
				h.rowReader = &parquetNestedTestRows{readSeq: []int{1, 2}}
				p.maxBatchSize = 20
			},
		},
		{
			name: "source budget limits second read",
			setup: func(t *testing.T, h *ParquetHandler, p *ExternalParam, _ *process.Process, _ *batch.Batch) {
				file, page, mapper := newScalarPage(t, 2)
				h.batchCnt = 2
				h.rowGroupRows = 2
				h.dataColIndices = []int{0}
				h.budgetColIndices = []int{0}
				h.currentPage = []parquet.Page{nil}
				h.pageOffset = []int64{0}
				h.pages = []parquet.Pages{&parquetNestedTestPages{page: page}}
				h.mappers = []*columnMapper{mapper}
				h.cols = []*parquet.Column{file.Root().Column("c")}
				h.rowReader = &parquetNestedTestRows{readSeq: []int{1, 1}}
				p.maxBatchSize = 1024
			},
		},
		{
			name: "row eof validation",
			setup: func(_ *testing.T, h *ParquetHandler, _ *ExternalParam, _ *process.Process, _ *batch.Batch) {
				h.batchCnt = 1
				h.rowGroupRows = 2
				h.rowReader = &parquetNestedTestRows{readRows: 1, readEOF: true}
			},
			wantErr: "row reader ended after 1 rows, expected 2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := &ParquetHandler{}
			p := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background()}}
			bat := vectorBatch([]types.Type{types.T_int64.ToType()})
			t.Cleanup(func() {
				bat.Clean(proc.Mp())
				h.cleanup()
				_ = h.closePages(p.Ctx)
			})
			test.setup(t, h, p, proc, bat)
			err := h.getDataByRowAndPage(bat, p, proc)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				if test.name == "byte budget rewinds unread rows" {
					reader, ok := h.rowReader.(*parquetNestedTestRows)
					require.True(t, ok)
					require.Equal(t, []int64{0, 2}, reader.seekRows)
					require.Equal(t, 2, bat.RowCount())
				}
			}
		})
	}
}

func TestParquetHybridReaderNestedRowDispatch(t *testing.T) {
	proc := testutil.NewProc(t)
	param := &ExternalParam{ExParamConst: ExParamConst{
		Ctx:  context.Background(),
		Cols: []*plan.ColDef{{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}}},
	}}
	bat := vectorBatch([]types.Type{types.New(types.T_array_float32, 2, 0)})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })

	t.Run("skip hidden or missing nested mapper", func(t *testing.T) {
		h := &ParquetHandler{
			cols:             []*parquet.Column{nil},
			nestedColIndices: []int{0},
			mappers:          []*columnMapper{nil},
		}
		require.NoError(t, h.processNestedRow(nil, bat, param, proc))
	})

	t.Run("return nested conversion error", func(t *testing.T) {
		file, _ := writeListNodeAndGetPage(t,
			parquet.Optional(parquet.List(parquet.Leaf(parquet.DoubleType))),
			[]parquet.Row{{parquet.NullValue().Level(0, 0, 0)}})
		col := file.Root().Column("c")
		h := &ParquetHandler{cols: []*parquet.Column{col}, nestedColIndices: []int{0}}
		_, mapper := h.getNestedListMapper(col, param.Cols[0].Typ)
		h.mappers = []*columnMapper{mapper}
		// An empty row is malformed for this list and must propagate from the
		// nested materializer instead of being swallowed by the hybrid loop.
		err := h.processNestedRow(nil, bat, param, proc)
		require.ErrorContains(t, err, "1 rows but no values")
	})
}

func TestParquetValueToGo(t *testing.T) {
	tests := []struct {
		name     string
		value    parquet.Value
		expected any
	}{
		{"null", parquet.NullValue(), nil},
		{"bool_true", parquet.BooleanValue(true), true},
		{"bool_false", parquet.BooleanValue(false), false},
		{"int32", parquet.Int32Value(42), int64(42)},
		{"int64", parquet.Int64Value(123456789), int64(123456789)},
		{"float", parquet.FloatValue(3.14), float64(float32(3.14))},
		{"double", parquet.DoubleValue(3.14159), 3.14159},
		{"string", parquet.ByteArrayValue([]byte("hello")), "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parquetValueToGo(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringifyMapKey(t *testing.T) {
	tests := []struct {
		name     string
		value    parquet.Value
		expected string
	}{
		{"string_key", parquet.ByteArrayValue([]byte("key1")), "key1"},
		{"int_key", parquet.Int32Value(123), "123"},
		{"null_key", parquet.NullValue(), "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringifyMapKey(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNestedTargetTypeSupported(t *testing.T) {
	tests := []struct {
		typ      types.T
		expected bool
	}{
		{types.T_json, true},
		{types.T_text, true},
		{types.T_varchar, true},
		{types.T_char, true},
		{types.T_int32, false},
		{types.T_float64, false},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			result := isNestedTargetTypeSupported(tt.typ)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNestedColumnNull(t *testing.T) {
	t.Run("empty_values", func(t *testing.T) {
		result := isNestedColumnNull([]parquet.Value{}, nil)
		assert.True(t, result)
	})

	t.Run("non_null_values", func(t *testing.T) {
		values := []parquet.Value{parquet.Int32Value(1)}
		result := isNestedColumnNull(values, nil)
		assert.False(t, result)
	})
}
