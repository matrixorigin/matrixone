// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func compileExactScanFilter(exprs []*pb.Expr, columns []pb.Type) *objectio.ExactScanFilter {
	if len(exprs) == 0 {
		return nil
	}
	if len(exprs) == 1 && exprs[0] != nil {
		if call := exprs[0].GetF(); call != nil && call.Func != nil {
			fid, _ := function.DecodeOverloadID(call.Func.Obj)
			if fid == function.BETWEEN && call.Func.ObjName == "between" {
				// A lone BETWEEN already has one fused native value pass.
				// Keep that path rather than adding selection/gather work;
				// compound filters can amortize cached exact materialization.
				return nil
			}
		}
	}
	// V1 keeps varlen projection materialization and its metadata on the old path.
	for _, typ := range columns {
		if !types.T(typ.Id).IsFixedLen() {
			return nil
		}
	}
	var predicates []objectio.ExactScanPredicate
	var add func(*pb.Expr) bool
	add = func(e *pb.Expr) bool {
		if e == nil {
			return false
		}
		call := e.GetF()
		if call == nil || call.Func == nil {
			return false
		}
		fid, _ := function.DecodeOverloadID(call.Func.Obj)
		if fid == function.AND && call.Func.ObjName == "and" && len(call.Args) == 2 {
			return add(call.Args[0]) && add(call.Args[1])
		}
		if fid == function.BETWEEN && call.Func.ObjName == "between" && len(call.Args) == 3 {
			return addComparison(call.Args[0], call.Args[1], objectio.ExactScanGE, columns, &predicates) &&
				addComparison(call.Args[0], call.Args[2], objectio.ExactScanLE, columns, &predicates)
		}
		var op objectio.ExactScanOp
		var name string
		switch fid {
		case function.EQUAL:
			op, name = objectio.ExactScanEQ, "="
		case function.NOT_EQUAL:
			op, name = objectio.ExactScanNE, "<>"
		case function.LESS_THAN:
			op, name = objectio.ExactScanLT, "<"
		case function.LESS_EQUAL:
			op, name = objectio.ExactScanLE, "<="
		case function.GREAT_THAN:
			op, name = objectio.ExactScanGT, ">"
		case function.GREAT_EQUAL:
			op, name = objectio.ExactScanGE, ">="
		default:
			return false
		}
		if call.Func.ObjName != name || len(call.Args) != 2 {
			return false
		}
		if call.Args[0].GetCol() != nil {
			return addComparison(call.Args[0], call.Args[1], op, columns, &predicates)
		}
		switch op {
		case objectio.ExactScanLT:
			op = objectio.ExactScanGT
		case objectio.ExactScanLE:
			op = objectio.ExactScanGE
		case objectio.ExactScanGT:
			op = objectio.ExactScanLT
		case objectio.ExactScanGE:
			op = objectio.ExactScanLE
		}
		return addComparison(call.Args[1], call.Args[0], op, columns, &predicates)
	}
	for _, e := range exprs {
		if !add(e) {
			return nil
		}
	}
	return objectio.NewExactScanFilter(predicates)
}

func addComparison(column, literal *pb.Expr, op objectio.ExactScanOp, columns []pb.Type, out *[]objectio.ExactScanPredicate) bool {
	if column == nil || literal == nil {
		return false
	}
	col := column.GetCol()
	lit := literal.GetLit()
	if col == nil || col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(columns) || lit == nil || lit.Isnull || lit.Src != nil {
		return false
	}
	typ := columns[col.ColPos]
	if typ.Id != column.Typ.Id || typ.Scale != column.Typ.Scale || typ.Id != literal.Typ.Id || typ.Scale != literal.Typ.Scale {
		return false
	}
	var bits uint64
	switch types.T(typ.Id) {
	case types.T_int8:
		v, ok := lit.Value.(*pb.Literal_I8Val)
		if !ok {
			return false
		}
		bits = uint64(int64(int8(v.I8Val)))
	case types.T_int16:
		v, ok := lit.Value.(*pb.Literal_I16Val)
		if !ok {
			return false
		}
		bits = uint64(int64(int16(v.I16Val)))
	case types.T_int32:
		v, ok := lit.Value.(*pb.Literal_I32Val)
		if !ok {
			return false
		}
		bits = uint64(int64(v.I32Val))
	case types.T_int64:
		v, ok := lit.Value.(*pb.Literal_I64Val)
		if !ok {
			return false
		}
		bits = uint64(v.I64Val)
	case types.T_uint8:
		v, ok := lit.Value.(*pb.Literal_U8Val)
		if !ok {
			return false
		}
		bits = uint64(v.U8Val)
	case types.T_uint16:
		v, ok := lit.Value.(*pb.Literal_U16Val)
		if !ok {
			return false
		}
		bits = uint64(v.U16Val)
	case types.T_uint32:
		v, ok := lit.Value.(*pb.Literal_U32Val)
		if !ok {
			return false
		}
		bits = uint64(v.U32Val)
	case types.T_uint64:
		v, ok := lit.Value.(*pb.Literal_U64Val)
		if !ok {
			return false
		}
		bits = v.U64Val
	case types.T_date:
		v, ok := lit.Value.(*pb.Literal_Dateval)
		if !ok {
			return false
		}
		bits = uint64(int64(v.Dateval))
	case types.T_decimal64:
		v, ok := lit.Value.(*pb.Literal_Decimal64Val)
		if !ok || v.Decimal64Val == nil {
			return false
		}
		bits = uint64(v.Decimal64Val.A)
	default:
		return false
	}
	*out = append(*out, objectio.ExactScanPredicate{Column: int(col.ColPos), Type: types.New(types.T(typ.Id), typ.Width, typ.Scale), Op: op, Value: bits})
	return true
}

func (tableScan *TableScan) prepareReaderFilter(proc *process.Process) {
	if tableScan.ctr.readerFilter != nil && tableScan.ctr.readerFilter.Exact != nil {
		tableScan.ctr.readerFilter.Exact.Close()
	}
	var program *objectio.ExactScanFilter
	if len(tableScan.RuntimeFilterExprs) == 0 && !tableScan.ctr.padCharToFullLength {
		program = compileExactScanFilter(tableScan.FilterExprs, tableScan.Types)
	}
	if program != nil && len(tableScan.ctr.earlyColumns) == 0 {
		tableScan.ctr.earlyColumns = make([]int, len(tableScan.Types))
		for i := range tableScan.ctr.earlyColumns {
			tableScan.ctr.earlyColumns[i] = i
		}
	}
	if len(tableScan.ctr.earlyColumns) == 0 {
		tableScan.ctr.readerFilter = nil
		return
	}
	tableScan.ctr.readerFilter = &engine.ReaderFilterRequest{
		Apply: func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
			return tableScan.applyReaderFilter(proc, bat, loaded)
		},
		Exact: objectio.NewExactScanState(program),
		Observe: func(stats objectio.ExactScanStats) {
			tableScan.ctr.filterActiveDuration += stats.FilterTime
			if stats.InputRows > 0 {
				tableScan.OpAnalyzer.InputBlock()
				op := tableScan.OpAnalyzer.GetOpStats()
				op.InputRows += int64(stats.InputRows)
				op.InputSize += stats.InputBytes
				op.ScanBytes += stats.InputBytes
			}
			if tableScan.ctr.buf != nil {
				tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, tableScan.ctr.buf.Size())
			}
			tableScan.ctr.filterReadMetrics = true
			tableScan.ctr.filterLateMaterialized = false
		},
	}
}
