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
)

type jsonDocumentColumn struct{ relation, column int32 }

// ProjectionJSONFrame owns only this invocation's small extracted values.
// Executable bindings are immutable between invocations.
type ProjectionJSONFrame struct {
	groups []*jsonProjectionGroup
	pages  [][]byte
	input  *batch.Batch
	parse  func(context.Context, []byte) (bytejson.ByteJson, error)
}

type jsonProjectionGroup struct {
	frame  *ProjectionJSONFrame
	paths  []bytejson.Path
	ready  []bool
	values []jsonProjectionValue
}

type jsonProjectionBinding struct {
	group *jsonProjectionGroup
	slot  int
}

type jsonProjectionValue struct {
	offset int32
	// Zero means missing, -1 means the value exceeded the cache budget.
	length int16
	kind   bytejson.TpCode
}

// NewProjectionExecutors binds repeated text JSON_EXTRACT leaves in the final
// physical project list. Other callers of the expression builder stay local
// to a single expression.
func NewProjectionExecutors(proc *process.Process, exprs []*plan.Expr, allocation *vector.AllocationAccountSelection) ([]ExpressionExecutor, *ProjectionJSONFrame, error) {
	bindings, frame := bindProjectionJSON(exprs)
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
	f.pages = nil
	for _, group := range f.groups {
		group.ready, group.values = nil, nil
	}
}

func (f *ProjectionJSONFrame) keep(value bytejson.ByteJson) jsonProjectionValue {
	n := len(value.Data)
	if n > jsonProjectionValueLimit {
		return jsonProjectionValue{length: -1}
	}
	if len(f.pages) == 0 || cap(f.pages[len(f.pages)-1])-len(f.pages[len(f.pages)-1]) < n {
		if len(f.pages) == jsonProjectionCacheLimit/jsonProjectionPageSize {
			return jsonProjectionValue{length: -1}
		}
		f.pages = append(f.pages, make([]byte, 0, jsonProjectionPageSize))
	}
	last := len(f.pages) - 1
	page := f.pages[last]
	offset := last*jsonProjectionPageSize + len(page)
	f.pages[last] = append(page, value.Data...)
	return jsonProjectionValue{offset: int32(offset), length: int16(n), kind: value.Type}
}

func (b *jsonProjectionBinding) extract(ctx context.Context, row int, raw []byte) (bytejson.ByteJson, bool, error) {
	g := b.group
	if g.ready == nil {
		rows := g.frame.input.RowCount()
		g.ready = make([]bool, rows)
		g.values = make([]jsonProjectionValue, rows*len(g.paths))
	}
	start := row * len(g.paths)
	if g.ready[row] {
		value := g.values[start+b.slot]
		switch {
		case value.length == 0:
			return bytejson.ByteJson{}, false, nil
		case value.length > 0:
			page := g.frame.pages[value.offset/jsonProjectionPageSize]
			offset := int(value.offset % jsonProjectionPageSize)
			return bytejson.ByteJson{Type: value.kind, Data: page[offset : offset+int(value.length)]}, true, nil
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
	if !g.ready[row] {
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
				g.values[start+slot] = g.frame.keep(value)
			}
		}
		g.ready[row] = true
	}
	return out, exists, nil
}

func (b *jsonProjectionBinding) eval(proc *process.Process, arguments []*vector.Vector, result vector.FunctionResultWrapper, rows int, mask []bool) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	input := vector.GenerateFunctionStrParameter(arguments[0])
	for row := 0; row < rows; row++ {
		if err := function.CheckJSONExtractRow(proc.Ctx, row); err != nil {
			return err
		}
		if mask != nil && !mask[row] {
			if err := rs.AppendByteJson(bytejson.ByteJson{}, true); err != nil {
				return err
			}
			continue
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
