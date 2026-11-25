// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestVarExpressionExecutorBoundDomainValidationAndLegacyReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	textType := types.T_text.ToType()
	makeExpr := func(domain uint32) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(textType.Oid), Charset: uint32(textType.Charset)},
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "s", BoundStringDomain: domain}},
		}
	}
	for _, raw := range []uint32{4, 256, ^uint32(0)} {
		executor, err := NewExpressionExecutor(proc, makeExpr(raw))
		require.Nil(t, executor)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}
	for _, system := range []bool{false, true} {
		expr := makeExpr(1)
		expr.GetV().System = system
		if !system {
			expr.Typ.Id = int32(types.T_json)
		}
		executor, err := NewExpressionExecutor(proc, expr)
		require.Nil(t, executor)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}

	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "你", nil })
	// A freed frozen executor must not leak its binding into a subsequently
	// allocated legacy executor. Each scope owns and releases its own vector.
	// The bound domain is validated but no longer stamped on the result: the
	// static type is the only source of string semantics.
	for _, raw := range []uint32{2, 0, 3, 0, 1, 0} {
		func() {
			executor, err := NewExpressionExecutor(proc, makeExpr(raw))
			require.NoError(t, err)
			defer executor.Free()
			for range 2 {
				vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.Equal(t, "你", vec.GetStringAt(0))
			}
		}()
		require.Zero(t, proc.Mp().CurrNB())
	}
}
