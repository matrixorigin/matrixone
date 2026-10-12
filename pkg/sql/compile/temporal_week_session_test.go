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

package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestWeekRemoteSessionRoundTrip(t *testing.T) {
	local := testutil.NewProcess(t)
	local.Ctx = context.WithValue(local.Ctx, defines.TenantIDKey{}, uint32(0))
	local.Base.TxnOperator = fakeTxnOperator{}
	local.Base.SessionInfo.TimeZone = time.UTC
	mode := int64(3)
	local.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		switch name {
		case "default_week_format":
			return mode, nil
		case "sql_mode":
			return "", nil
		case "lock_wait_timeout":
			return int64(50), nil
		}
		return nil, nil
	})
	info, err := local.BuildProcessInfo("select week(d)")
	require.NoError(t, err)
	decoded, err := process.ConvertToProcessSessionInfo(info.SessionInfo)
	require.NoError(t, err)
	remote := testutil.NewProcess(t)
	remote.Base.SessionInfo = decoded
	require.Nil(t, remote.GetResolveVariableFunc())
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "week", []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	eval := func(p *process.Process) uint8 {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_date.ToType())
		defer input.Clean(p.Mp())
		require.NoError(t, vector.AppendFixed(input.Vecs[0], types.DateFromCalendar(2021, 1, 1), false, p.Mp()))
		input.SetRowCount(1)
		e, err := colexec.NewExpressionExecutor(p, expr)
		require.NoError(t, err)
		defer e.Free()
		out, err := e.Eval(p, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		return vector.MustFixedColNoTypeCheck[uint8](out)[0]
	}
	a, b := eval(local), eval(remote)
	t.Logf("same WEEK(date-column): initiating CN=%d decoded process=%d", a, b)
	require.Equal(t, uint8(53), a)
	require.Equal(t, uint8(53), b)
	mode = 0
	second, err := local.BuildProcessInfo("select week(d)")
	require.NoError(t, err)
	remote.Base.SessionInfo, err = process.ConvertToProcessSessionInfo(second.SessionInfo)
	require.NoError(t, err)
	require.Equal(t, uint8(0), eval(local))
	require.Equal(t, uint8(0), eval(remote))
}
