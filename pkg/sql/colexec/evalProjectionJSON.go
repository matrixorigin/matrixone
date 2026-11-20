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

package colexec

import (
	"context"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	jsonProjectionPageSize   = 32 << 10
	jsonProjectionCacheLimit = 4 << 20
	jsonProjectionValueLimit = 4 << 10
	// Offset (4), length (2), and ByteJSON type (1).
	jsonProjectionRecordSize = 7
)

type jsonDocumentColumn struct{ relation, column int32 }

// ProjectionJSONFrame owns this invocation's extraction metadata and values.
// Executable bindings are immutable between invocations.
type ProjectionJSONFrame struct {
	groups     []*jsonProjectionGroup
	pages      [][]byte
	input      *batch.Batch
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	cacheBytes int
	pagesFull  bool
	parse      func(context.Context, []byte) (bytejson.ByteJson, error)
}

type jsonProjectionGroup struct {
	frame     *ProjectionJSONFrame
	paths     []bytejson.Path
	metadata  []byte
	attempted bool
}

type jsonProjectionBinding struct {
	group *jsonProjectionGroup
	slot  int
}

// NewProjectionExecutors binds repeated text JSON_EXTRACT leaves in the final
// physical project list. Other callers of the expression builder stay local
// to a single expression.
func NewProjectionExecutors(proc *process.Process, exprs []*plan.Expr, allocation *vector.AllocationAccountSelection) ([]ExpressionExecutor, *ProjectionJSONFrame, error) {
	bindings, frame := bindProjectionJSON(exprs)
	if frame != nil {
		frame.mp, frame.allocation = proc.Mp(), allocation
	}
	executors, err := newExpressionExecutorsWithBindings(proc, exprs, allocation, expressionDiagnosticScope{}, bindings)
	return executors, frame, err
}

func bindProjectionJSON(exprs []*plan.Expr) (map[*plan.Function]*jsonProjectionBinding, *ProjectionJSONFrame) {
	type candidate struct {
		fn   *plan.Function
		path bytejson.Path
	}
	var columns map[jsonDocumentColumn][]candidate
	var visit func(*plan.Expr)
	visit = func(expr *plan.Expr) {
		// Existing memoized subtrees have their own evaluation contract.
		if expr == nil || expr.AuxId < 0 {
			return
		}
		if f := expr.GetF(); f != nil {
			fid, overload := function.DecodeOverloadID(f.GetFunc().GetObj())
			// These executors can mask their arguments per row. A shared
			// group would query their paths even when the branch is unused.
			switch fid {
			case function.CASE, function.IFF, function.COALESCE:
				return
			case function.ELT:
				if overload == 1 {
					return
				}
			}
			if fid == function.JSON_EXTRACT && overload == 0 && len(f.Args) == 2 {
				doc, arg := f.Args[0], f.Args[1]
				col, lit := doc.GetCol(), arg.GetLit()
				if col != nil && doc.AuxId >= 0 && arg.AuxId >= 0 && types.T(doc.Typ.Id).IsMySQLString() && lit != nil && !lit.Isnull {
					if str, ok := lit.Value.(*plan.Literal_Sval); ok && len(str.Sval) <= 1024 {
						if path, err := bytejson.ParseJsonPath(str.Sval); err == nil && path.IsSimple() {
							if columns == nil {
								columns = make(map[jsonDocumentColumn][]candidate)
							}
							key := jsonDocumentColumn{col.RelPos, col.ColPos}
							columns[key] = append(columns[key], candidate{f, path})
						}
					}
				}
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		}
	}
	for _, expr := range exprs {
		visit(expr)
	}
	var frame *ProjectionJSONFrame
	var bindings map[*plan.Function]*jsonProjectionBinding
	for _, candidates := range columns {
		if len(candidates) < 2 {
			continue
		}
		if frame == nil {
			frame = &ProjectionJSONFrame{parse: bytejson.ParseFromByteSliceWithCancellation}
			bindings = make(map[*plan.Function]*jsonProjectionBinding)
		}
		group := &jsonProjectionGroup{frame: frame, paths: make([]bytejson.Path, 0, len(candidates))}
		for _, c := range candidates {
			if _, ok := bindings[c.fn]; ok {
				continue
			}
			bindings[c.fn] = &jsonProjectionBinding{group, len(group.paths)}
			group.paths = append(group.paths, c.path)
		}
		frame.groups = append(frame.groups, group)
	}
	return bindings, frame
}

func (f *ProjectionJSONFrame) Begin(input *batch.Batch) {
	f.input = input
}

func (f *ProjectionJSONFrame) End() {
	f.input = nil
	for _, page := range f.pages {
		f.mp.Free(page)
	}
	f.pages = nil
	for _, group := range f.groups {
		if group.metadata != nil {
			f.mp.Free(group.metadata)
		}
		group.metadata, group.attempted = nil, false
	}
	f.cacheBytes, f.pagesFull = 0, false
}

// allocate admits the retained capacity before allocating either metadata or
// payload. Storage belongs to the same MPool and execution selection as the
// projection, including when an error exits the batch early.
func (f *ProjectionJSONFrame) allocate(size, headroom int) []byte {
	capacity := int64(size)
	if f.allocation != nil {
		var ok bool
		capacity, ok = mpool.GrowCapacity(0, capacity)
		if !ok {
			return nil
		}
	}
	if capacity > int64(jsonProjectionCacheLimit-f.cacheBytes-headroom) {
		return nil
	}

	var data []byte
	if f.allocation == nil {
		var err error
		data, err = f.mp.Alloc(size, true)
		if err != nil {
			return nil
		}
	} else {
		buf, err := f.allocation.NewAccountedBuffer(f.mp)
		if err != nil {
			return nil
		}
		if err = buf.Resize(size); err != nil {
			buf.Free()
			return nil
		}
		data, _, err = buf.Detach()
		if err != nil {
			buf.Free()
			return nil
		}
	}
	f.cacheBytes += cap(data)
	return data
}

func (g *jsonProjectionGroup) admit() {
	g.attempted = true
	rows := g.frame.input.RowCount()
	// Check the shape before multiplying. Leave room for a payload page.
	if g.frame.pagesFull || rows == 0 || len(g.paths) > (jsonProjectionCacheLimit/rows-1)/jsonProjectionRecordSize {
		return
	}
	g.metadata = g.frame.allocate(rows*(1+len(g.paths)*jsonProjectionRecordSize), jsonProjectionPageSize)
	clear(g.metadata)
}

func (f *ProjectionJSONFrame) keep(record []byte, value bytejson.ByteJson) {
	size := len(value.Data)
	// A zero length is a missing path. A negative length is a matched
	// value that needs to be evaluated directly for this consumer.
	binary.LittleEndian.PutUint16(record[4:], ^uint16(0))
	if size > jsonProjectionValueLimit {
		return
	}
	if len(f.pages) == 0 || len(f.pages[len(f.pages)-1])+size > jsonProjectionPageSize {
		page := f.allocate(jsonProjectionPageSize, 0)
		if page == nil {
			f.pagesFull = true
			return
		}
		f.pages = append(f.pages, page[:0])
	}
	last := len(f.pages) - 1
	page := f.pages[last]
	offset := last*jsonProjectionPageSize + len(page)
	f.pages[last] = append(page, value.Data...)
	binary.LittleEndian.PutUint32(record, uint32(offset))
	binary.LittleEndian.PutUint16(record[4:], uint16(size))
	record[6] = value.Type
}

func (b *jsonProjectionBinding) extract(ctx context.Context, row int, raw []byte) (bytejson.ByteJson, bool, error) {
	g := b.group
	if !g.attempted {
		g.admit()
	}
	var rowCache []byte
	if g.metadata != nil {
		stride := 1 + len(g.paths)*jsonProjectionRecordSize
		cached := g.metadata[row*stride : (row+1)*stride]
		if cached[0] != 0 {
			record := cached[1+b.slot*jsonProjectionRecordSize:]
			length := int16(binary.LittleEndian.Uint16(record[4:]))
			if length > 0 {
				offset := int(binary.LittleEndian.Uint32(record))
				page := g.frame.pages[offset/jsonProjectionPageSize]
				start := offset % jsonProjectionPageSize
				return bytejson.ByteJson{Type: record[6], Data: page[start : start+int(length)]}, true, nil
			}
			if length == 0 {
				return bytejson.ByteJson{}, false, nil
			}
		} else if !g.frame.pagesFull {
			rowCache = cached
		}
	}
	if err := function.CheckJSONExtractRow(ctx, 0); err != nil {
		return bytejson.ByteJson{}, false, err
	}
	doc, err := g.frame.parse(ctx, raw)
	if err != nil {
		return bytejson.ByteJson{}, false, err
	}
	paths := [1]*bytejson.Path{&g.paths[b.slot]}
	out, exists := doc.QuerySimpleWithExists(paths[:])
	if rowCache != nil {
		for slot := range g.paths {
			if err := function.CheckJSONExtractRow(ctx, slot); err != nil {
				return bytejson.ByteJson{}, false, err
			}
			value, present := out, exists
			if slot != b.slot {
				paths[0] = &g.paths[slot]
				value, present = doc.QuerySimpleWithExists(paths[:])
			}
			if present {
				g.frame.keep(rowCache[1+slot*jsonProjectionRecordSize:], value)
				if g.frame.pagesFull {
					// With no payload page, this group's speculative
					// metadata can be returned to the execution budget.
					if len(g.frame.pages) == 0 {
						g.frame.cacheBytes -= cap(g.metadata)
						g.frame.mp.Free(g.metadata)
						g.metadata = nil
					}
					return out, exists, nil
				}
			}
		}
		rowCache[0] = 1
	}
	return out, exists, nil
}

func (b *jsonProjectionBinding) eval(proc *process.Process, arguments []*vector.Vector, result vector.FunctionResultWrapper, rows int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	input := vector.GenerateFunctionStrParameter(arguments[0])
	for row := 0; row < rows; row++ {
		if err := function.CheckJSONExtractRow(proc.Ctx, row); err != nil {
			return err
		}
		raw, null := input.GetStrValue(uint64(row))
		if null {
			if err := rs.AppendByteJson(bytejson.ByteJson{}, true); err != nil {
				return err
			}
			continue
		}
		out, exists, err := b.extract(proc.Ctx, row, raw)
		if err != nil {
			return err
		}
		if err := rs.AppendByteJson(out, !exists); err != nil {
			return err
		}
	}
	return nil
}
