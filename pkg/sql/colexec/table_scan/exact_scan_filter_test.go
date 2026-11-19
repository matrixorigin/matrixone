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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCompileExactScanFilterAdmission(t *testing.T) {
	typ := types.T_int32.ToType()
	columns := make([]pb.Type, 1, 2)
	columns[0] = pb.Type{Id: int32(typ.Oid)}
	require.Nil(t, compileExactScanFilter([]*pb.Expr{nil}, columns))
	expr := lateTestCompare(t, ">=", 0, 2)
	require.NotNil(t, compileExactScanFilter([]*pb.Expr{expr}, columns))
	mismatch := lateTestCompare(t, ">=", 0, 2)
	mismatch.GetF().Args[1].Typ.Scale = 1
	require.Nil(t, compileExactScanFilter([]*pb.Expr{mismatch}, columns))
	dynamic := lateTestCompare(t, ">=", 0, 2)
	dynamic.GetF().Args[1].Expr = &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}
	require.Nil(t, compileExactScanFilter([]*pb.Expr{dynamic}, columns))
	require.Nil(t, compileExactScanFilter([]*pb.Expr{expr, lateTestCol(0, typ)}, columns))
	require.Nil(t, compileExactScanFilter([]*pb.Expr{expr}, append(columns, pb.Type{Id: int32(types.T_text)})))
	between := lateTestCompare(t, ">=", 0, 2)
	fn, err := function.GetFunctionByName(t.Context(), "between", []types.Type{typ, typ, typ})
	require.NoError(t, err)
	between.GetF().Func = &pb.ObjectRef{ObjName: "between", Obj: fn.GetEncodedOverloadID()}
	between.GetF().Args = append(between.GetF().Args, lateTestCompare(t, "<=", 0, 8).GetF().Args[1])
	require.Nil(t, compileExactScanFilter([]*pb.Expr{between}, columns))
	require.NotNil(t, compileExactScanFilter([]*pb.Expr{between, expr}, columns))
	andFn, err := function.GetFunctionByName(t.Context(), "and", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()})
	require.NoError(t, err)
	compound := &pb.Expr{Typ: between.Typ, Expr: &pb.Expr_F{F: &pb.Function{
		Func: &pb.ObjectRef{ObjName: "and", Obj: andFn.GetEncodedOverloadID()},
		Args: []*pb.Expr{between, expr},
	}}}
	require.NotNil(t, compileExactScanFilter([]*pb.Expr{compound}, columns))
	proc := testutil.NewProc(t)
	nativeScan := &TableScan{Attrs: []string{"v"}, Types: columns, FilterExprs: []*pb.Expr{between}}
	t.Cleanup(func() { nativeScan.Free(proc, false, nil) })
	require.NoError(t, nativeScan.Prepare(proc))
	require.Nil(t, nativeScan.ctr.readerFilter)
	scan := &TableScan{Attrs: []string{"v"}, Types: columns, FilterExprs: []*pb.Expr{expr}}
	t.Cleanup(func() { scan.Free(proc, false, nil) })
	require.NoError(t, scan.Prepare(proc))
	require.NotNil(t, scan.ctr.readerFilter)
	require.NotNil(t, scan.ctr.readerFilter.Exact)
	require.Equal(t, []int{0}, scan.ctr.earlyColumns)
	scan.Reset(proc, false, nil)
	require.Nil(t, scan.ctr.readerFilter)
	require.NoError(t, scan.Prepare(proc))
	require.NotNil(t, scan.ctr.readerFilter.Exact)
}
