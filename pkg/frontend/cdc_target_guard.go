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

package frontend

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const cdcTargetGuardCall = "mo_cdc_target_identity"
const cdcTargetGuardCapabilityCall = "mo_cdc_target_guard_capability"

// This operation runs inside the caller's target DML transaction. It locks
// the same catalog name keys as DDL, then resolves the live physical ID.
func executeCDCTargetGuardCall(ctx context.Context, ses FeSession, call *tree.CallStmt) ([]ExecResult, bool, error) {
	if isCDCTargetGuardCapabilityCall(call) {
		if len(call.Args) != 0 {
			return nil, true, moerr.NewInvalidInput(ctx, "CDC target guard capability takes no arguments")
		}
		return nil, true, checkCDCTargetGuardTransaction(ctx, ses)
	}
	databaseName, tableName, matched, err := parseCDCTargetGuardCall(ctx, call)
	if !matched || err != nil {
		return nil, matched, err
	}
	id, err := lockCDCTargetIdentity(ctx, ses, databaseName, tableName)
	if err != nil {
		return nil, true, err
	}
	result := &MysqlResultSet{}
	column := new(MysqlColumn)
	column.SetName("target_table_id")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	result.AddColumn(column)
	result.AddRow([]interface{}{id})
	return []ExecResult{result}, true, nil
}

func isCDCTargetGuardCapabilityCall(call *tree.CallStmt) bool {
	return call != nil && call.Name != nil && call.Name.HasNoNameQualifier() &&
		strings.EqualFold(tree.String(call.Name, dialect.MYSQL), cdcTargetGuardCapabilityCall)
}

func parseCDCTargetGuardCall(ctx context.Context, call *tree.CallStmt) (string, string, bool, error) {
	if call == nil || call.Name == nil || !call.Name.HasNoNameQualifier() ||
		!strings.EqualFold(tree.String(call.Name, dialect.MYSQL), cdcTargetGuardCall) {
		return "", "", false, nil
	}
	if len(call.Args) != 2 {
		return "", "", true, moerr.NewInvalidInput(ctx, "CDC target identity guard requires database and table names")
	}
	args := [2]string{}
	for i, expr := range call.Args {
		switch value := expr.(type) {
		case *tree.StrVal:
			args[i] = value.String()
		case *tree.NumVal:
			if value.Kind() == tree.Str {
				args[i] = value.String()
			}
		}
		if args[i] == "" {
			return "", "", true, moerr.NewInvalidInput(ctx, "CDC target identity guard requires string literal names")
		}
	}
	return args[0], args[1], true, nil
}

func lockCDCTargetIdentity(ctx context.Context, ses FeSession, databaseName, tableName string) (uint64, error) {
	if err := checkCDCTargetGuardTransaction(ctx, ses); err != nil {
		return 0, err
	}
	proc := ses.GetProc()
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		return 0, moerr.NewInternalError(ctx, "CDC target identity guard has no tenant")
	}
	accountID := tenant.GetTenantID()
	tenantCtx := defines.AttachAccountId(ctx, accountID)
	storage := proc.GetSessionInfo().StorageEngine
	if storage == nil {
		return 0, moerr.NewInternalError(ctx, "CDC target identity guard has no storage engine")
	}
	catalogDB, err := storage.Database(tenantCtx, catalog.MO_CATALOG, proc.GetTxnOperator())
	if err != nil {
		return 0, err
	}
	for _, target := range []struct {
		name  string
		parts []string
	}{
		{catalog.MO_DATABASE, []string{databaseName}},
		{catalog.MO_TABLES, []string{databaseName, tableName}},
	} {
		rel, err := catalogDB.Relation(tenantCtx, target.name, nil)
		if err != nil {
			return 0, err
		}
		if err := lockCDCCatalogName(proc, tenantCtx, storage, rel, accountID, target.parts...); err != nil {
			return 0, err
		}
	}
	// A waited catalog lock does not always advance an RC snapshot by itself.
	now, _ := moruntime.ServiceRuntime(ses.GetService()).Clock().Now()
	if err = proc.GetTxnOperator().GetWorkspace().AdvanceSnapshot(tenantCtx, now); err != nil {
		return 0, err
	}
	db, err := storage.Database(tenantCtx, databaseName, proc.GetTxnOperator())
	if err != nil {
		return 0, err
	}
	target, err := db.Relation(tenantCtx, tableName, nil)
	if err != nil {
		return 0, err
	}
	return target.GetTableID(tenantCtx), nil
}

func checkCDCTargetGuardTransaction(ctx context.Context, ses FeSession) error {
	if handler := ses.GetTxnHandler(); handler == nil || !handler.OptionBitsIsSet(OPTION_BEGIN) {
		return moerr.NewNotSupported(ctx, "CDC target identity guard requires an explicit transaction")
	}
	proc := ses.GetProc()
	if proc == nil || proc.GetTxnOperator() == nil {
		return moerr.NewNotSupported(ctx, "CDC target identity guard requires an active transaction")
	}
	meta := proc.GetTxnOperator().Txn()
	if !meta.IsPessimistic() || meta.Isolation != txn.TxnIsolation_RC {
		return moerr.NewNotSupported(ctx, "CDC target identity guard requires a pessimistic read committed transaction")
	}
	return nil
}

func lockCDCCatalogName(
	proc *process.Process, ctx context.Context, storage engine.Engine,
	rel engine.Relation, accountID uint32, names ...string,
) error {
	lockBatch, err := cloneCatalogLockBatch(proc, accountID, names...)
	if err != nil {
		return err
	}
	defer lockBatch.Vecs[0].Free(proc.Mp())
	err = withCloneLockContext(proc, ctx, func() error {
		return lockop.LockRows(storage, proc, rel, rel.GetTableID(ctx), lockBatch, 0,
			*lockBatch.Vecs[0].GetType(), lock.LockMode_Shared, lock.Sharding_None, accountID)
	})
	// LockRows returns this code after it has acquired the lock and advanced
	// an RC snapshot past a concurrent catalog write. This guard has no bound
	// plan to retry: it resolves the name only after all locks and a further
	// snapshot advance. Definition changes still fail and retry the operation.
	if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) {
		return nil
	}
	return err
}
