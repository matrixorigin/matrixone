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

package plan

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestTableDumpReleasedBitCountBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	const ddl = "create table t (d double not null, g bigint generated always as " +
		"(bit_count(cast(d as double)) + bit_count(cast(-d as double))) stored)"
	current := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, ddl))
	released := DeepCopyTableDef(current, true)
	var calls []*pb.ObjectRef
	require.NoError(t, pb.VisitExprTree(released.Cols[1].GeneratedCol.Expr, func(e *pb.Expr) error {
		if fn := e.GetF(); fn != nil && fn.Func.ObjName == "bit_count" {
			fid, overload := function.DecodeOverloadID(fn.Func.Obj)
			require.Equal(t, int32(function.BIT_COUNT), fid)
			require.Equal(t, int32(16), overload)
			// 4.2.8 used overload 9 for both DOUBLE calls.
			fn.Func.Obj = fn.Func.Obj&^0xffffffff | 9
			calls = append(calls, fn.Func)
		}
		return nil
	}))
	require.Len(t, calls, 2)

	sensitive, err := AnalyzeTableDumpBindings(compiler, released, released)
	require.NoError(t, err)
	require.True(t, sensitive, "DUMP must carry the released bound expression")
	sensitive, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.NoError(t, err)
	require.True(t, sensitive)
	calls[0].Obj = calls[0].Obj&^0xffffffff | 16
	_, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.ErrorContains(t, err, "cannot verify bound generated expression", "mixed modes have no SQL binding")
}
