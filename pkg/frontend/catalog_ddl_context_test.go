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

package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type catalogDDLContextExec struct {
	BackgroundExec
	t        *testing.T
	internal []string
	ordinary []string
}

func (e *catalogDDLContextExec) Exec(ctx context.Context, sql string) error {
	account, err := defines.GetAccountId(ctx)
	require.NoError(e.t, err)
	require.Equal(e.t, uint32(42), account)
	if defines.IsInternalExecutor(ctx) {
		e.internal = append(e.internal, sql)
	} else {
		e.ordinary = append(e.ordinary, sql)
	}
	return e.BackgroundExec.Exec(ctx, sql)
}

func TestCatalogDDLOwnsInternalExecutionContext(t *testing.T) {
	for _, scenario := range []string{"new account", "current-schema restore"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := defines.AttachAccountId(t.Context(), 42)
			bh := &backgroundExecTest{}
			bh.init()
			capture := &catalogDDLContextExec{BackgroundExec: bh, t: t}
			if scenario == "new account" {
				pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
				pu.SV.SetDefaultValues()
				tenant := &TenantInfo{TenantID: 42, UserID: 2, DefaultRoleID: accountAdminRoleID}
				err := createTablesInMoCatalogOfGeneralTenant2(capture,
					&createAccount{AdminName: "admin", IdentTyp: tree.AccountIdentifiedByPassword, IdentStr: "111"},
					ctx, tenant, pu)
				require.NoError(t, err)
			} else {
				require.NoError(t, restoreUserDefinedFunctionCatalogWithCurrentSchema(ctx, capture, " {MO_TS = 123}", 42, 42))
				require.Equal(t, []string{MoCatalogMoUserDefinedFunctionDDL}, capture.internal)
			}
			require.Contains(t, capture.internal, MoCatalogMoUserDefinedFunctionDDL)
			require.NotEmpty(t, capture.ordinary)
			for _, sql := range capture.internal {
				require.True(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "create "), sql)
			}
			for _, sql := range capture.ordinary {
				require.False(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "create "), sql)
			}
			// Context.WithValue must not widen the caller's admission policy.
			require.False(t, defines.IsInternalExecutor(ctx))
		})
	}
}
