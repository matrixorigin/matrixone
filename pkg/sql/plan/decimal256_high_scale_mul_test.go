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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal256HighScaleMultiplicationPublicPath(t *testing.T) {
	stmt, err := runOneExprStmt(NewMockOptimizer(false), t,
		"select cast('1e-65' as decimal(65,65)) * cast('1e-65' as decimal(65,65))")
	require.NoError(t, err)
	expr := stmt.GetQuery().Nodes[1].ProjectList[0]
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(65), expr.Typ.Scale)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsNull(0))
	require.Equal(t, types.Decimal256{}, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0))
}
