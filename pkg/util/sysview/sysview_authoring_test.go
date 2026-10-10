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

package sysview

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestInitSchemaDefersViewsUntilAuthoringFence(t *testing.T) {
	const serviceID = "sysview-authoring-gate-test"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else if current, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion109))

	var statements []string
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: serviceID}).AnyTimes()
	txnExecutor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		statements = append(statements, sql)
		return executor.Result{}, nil
	}, txnOperator)

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	require.NoError(t, InitSchema(context.Background(), txnExecutor))
	joined := strings.Join(statements, "\n")
	require.Contains(t, joined, InformationSchemaViewsLegacyDDL)
	require.NotContains(t, joined, "mo_view_definition(")

	statements = nil
	rt.SetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor,
		int64(defines.MORPCVersion109))
	require.NoError(t, InitSchema(context.Background(), txnExecutor))
	joined = strings.Join(statements, "\n")
	require.Contains(t, joined, InformationSchemaViewsDDL)
	require.Contains(t, joined, "mo_view_definition(")
}
