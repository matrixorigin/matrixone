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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGetVariables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	ses := newSes(nil, ctrl)
	defer ses.Close()

	tempExecCtx := ExecCtx{
		reqCtx: ctx,
		ses:    ses,
	}
	defer tempExecCtx.Close()
	ses.txnCompileCtx.SetExecCtx(&tempExecCtx)

	proc := testutil.NewProc()
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)

	{
		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowsSizeForceUpdateVarName, "yes"))
		require.True(t, function.GetForceUpdateVariable(proc))

		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowsSizeForceUpdateVarName, "no"))
		require.False(t, function.GetForceUpdateVariable(proc))
	}

	{
		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowSizeUseOldImplVarName, "yes"))
		require.True(t, function.GetUseOldImplVariable(proc))

		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowSizeUseOldImplVarName, "no"))
		require.False(t, function.GetUseOldImplVariable(proc))
	}

	{
		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowSizeResetUpdateTimeVarName, "yes"))
		require.True(t, function.GetResetUpdateTimeVariable(proc))

		require.NoError(t, ses.SetSessionSysVar(ctx, function.MoTableRowSizeResetUpdateTimeVarName, "no"))
		require.False(t, function.GetResetUpdateTimeVariable(proc))
	}
}
