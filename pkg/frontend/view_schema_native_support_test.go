//go:build integration

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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// 此构造器只进入 integration 测试产物。集群、事务、workspace 和 process
// 均由调用者真实持有；这里仅补齐普通 Session 的私有 ExecCtx 接线。
func WithNativeViewSchemaCompilerForTest(
	ctx context.Context, service string, storage *disttae.Engine,
	op client.TxnOperator, proc *process.Process, database string,
	tenant *TenantInfo, run func(*TxnCompilerContext) error,
) error {
	unit := getPuIfPresent(service)
	if unit == nil || unit.StorageEngine != storage || proc.GetTxnOperator() != op || proc.GetService() != service {
		return fmt.Errorf("native view schema fixture does not own the CN transaction")
	}
	if tenant == nil {
		tenant = &TenantInfo{Tenant: sysAccountName, User: rootName, DefaultRole: moAdminRoleName,
			TenantID: sysAccountID, UserID: rootID, DefaultRoleID: moAdminRoleID}
	}
	tenant = tenant.Copy()
	ctx = defines.AttachAccount(ctx, tenant.TenantID, tenant.UserID, tenant.DefaultRoleID)
	handler := InitTxnHandler(service, storage, ctx, op)
	defer handler.Close()
	compiler := InitTxnCompilerContext(database)
	defer compiler.Close()
	vars := &SystemVariables{mp: make(map[string]interface{}, len(gSysVarsDefs))}
	for name, definition := range gSysVarsDefs {
		vars.mp[name] = definition.Default
	}
	ses := &Session{
		feSessionImpl: feSessionImpl{
			pool: proc.Mp(), service: service, accountId: tenant.TenantID,
			timeZone: proc.GetSessionInfo().TimeZone, tenant: tenant,
			respr:      &NullResp{username: tenant.User, database: database},
			txnHandler: handler, txnCompileCtx: compiler, sesSysVars: vars, gSysVars: vars.Clone(),
		},
		proc: proc, errInfo: &errInfo{}, cache: &privilegeCache{},
		userDefinedVars: make(map[string]*UserDefinedVar), tempTables: make(map[string]string),
	}
	previousFS := proc.GetFileService()
	proc.SetFileService(unit.FileService)
	defer proc.SetFileService(previousFS)
	compiler.SetExecCtx(&ExecCtx{reqCtx: ctx, ses: ses, proc: proc})
	return run(compiler)
}

// 使用 production 的 derived shared-transaction catalog SELECT，验证其不会
// 推进外层 statement/catalog 代号，且 Close 不回滚调用者事务。
func ReadNativeViewSchemaCatalogForTest(ctx context.Context, compiler *TxnCompilerContext) error {
	bh := compiler.getOrCreateBackExec(ctx)
	bh.ClearExecResultSet()
	return bh.Exec(ctx, "select relname from mo_catalog.mo_tables where relname = 'mo_tables'")
}

// 测试角色只具有一个明确的 active role，直接复用既有 view privilege 查询。
// 此 helper 不把它推广为生产 SHOW/查询权限策略。
func NativeViewSchemaRoleAuthorizerForTest(compiler *TxnCompilerContext) ViewSchemaAuthorizer {
	return func(ctx context.Context, database, name string, _ *plan.Snapshot) error {
		ses := compiler.GetSession().(*Session)
		allowed, err := verifyViewPrivilegeForRole(ctx, compiler.getOrCreateBackExec(ctx), ses,
			nil, int64(ses.GetTenantInfo().GetDefaultRoleID()), PrivilegeTypeSelect, database, name, false)
		if err != nil {
			return err
		}
		if !allowed {
			return moerr.NewInternalError(ctx, "view schema access denied")
		}
		return nil
	}
}
