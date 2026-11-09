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

package frontend

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// hiddenIndexCallerKey identifies SQL belonging to a client CALL, including
// its nested calls. It is never set from SQL text or persisted in a session.
type hiddenIndexCallerKey struct{}

func hiddenIndexCaller(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	caller, _ := ctx.Value(hiddenIndexCallerKey{}).(*Session)
	return caller
}

func withHiddenIndexCaller(ctx context.Context, ses FeSession) context.Context {
	if hiddenIndexCaller(ctx) != nil {
		return process.WithUserIndexRelationAccess(ctx)
	}
	if caller := upstreamUserSession(ses); caller != nil && caller.GetFromRealUser() {
		ctx = process.WithUserIndexRelationAccess(ctx)
		return context.WithValue(ctx, hiddenIndexCallerKey{}, caller)
	}
	return ctx
}

func sessionMayAccessIndexRelation(ses FeSession) bool {
	if ses == nil {
		return false
	}
	tenant := ses.GetTenantInfo()
	return tenant != nil && tenant.GetUser() != "" && verifyAccountCanExecMoCtrl(tenant)
}

// mayAccessIndexRelation evaluates the caller policy without constructing a
// denial, so callers can use it to choose whether catalog probes are needed.
func mayAccessIndexRelation(execCtx *ExecCtx) bool {
	if execCtx == nil {
		return false
	}
	// Background implementation SQL is explicitly server-owned, even when
	// it was requested by a user statement. Same-statement expression SQL
	// retains the user's policy and must never take this exemption.
	if execCtx.input != nil && execCtx.input.isInternal() && !execCtx.input.sameStatementAuthorization &&
		(execCtx.ses == nil || execCtx.ses.IsBackgroundSession() || !execCtx.ses.GetFromRealUser()) {
		return true
	}
	if caller := hiddenIndexCaller(execCtx.reqCtx); caller != nil {
		return sessionMayAccessIndexRelation(caller)
	}
	ses := execCtx.ses
	if ses != nil && !ses.IsBackgroundSession() && ses.GetFromRealUser() {
		return sessionMayAccessIndexRelation(ses)
	}
	if ses != nil && ses.GetIsInternal() {
		return true
	}
	return sessionMayAccessIndexRelation(ses)
}

func checkResolvedIndexRelation(execCtx *ExecCtx, requested, physical, kind string, flags uint64) error {
	if !catalog.RequiresPrivilegedRelationAccess(requested, kind, flags) && !catalog.RequiresPrivilegedRelationAccess(physical, kind, flags) {
		return nil
	}
	if mayAccessIndexRelation(execCtx) {
		return nil
	}
	if execCtx == nil {
		return moerr.NewInternalErrorNoCtx("do not have privilege to execute the statement")
	}
	return moerr.NewInternalError(execCtx.reqCtx, "do not have privilege to execute the statement")
}

// stampHiddenIndexCaller preserves the client principal for runtime operators
// and remote process encoding. Background executors otherwise start with an
// empty Account and Role, even when the SQL is a procedure body.
func stampHiddenIndexCaller(proc *process.Process, caller *Session) {
	if proc == nil || proc.Base == nil || caller == nil {
		return
	}
	info := proc.GetSessionInfo()
	info.Account, info.User, info.Role = "", "", ""
	if tenant := caller.GetTenantInfo(); tenant != nil {
		tenant.mu.Lock()
		info.Account, info.User, info.Role = tenant.Tenant, tenant.User, tenant.DefaultRole
		tenant.mu.Unlock()
	}
}

// checkFrontendIndexTarget covers explicit relation operands of operations that
// can execute in the frontend or commit before their plans are built.
func checkFrontendIndexTarget(ctx context.Context, ses FeSession, stmt tree.Statement) error {
	return checkFrontendIndexTargetInExec(&ExecCtx{reqCtx: ctx, ses: ses}, stmt)
}

// This checks explicit relation operands only. Nested SELECTs and expression
// bodies are checked when they bind; CTE names and aliases are not relations.
type explicitIndexTarget struct {
	database, table               string
	at                            *tree.AtTimeStamp
	cloneSource                   *tree.CloneTable
	snapshotName, pitrTimestamp   string
	publisherAccount, publication string
	withoutTemporaryAlias         bool
	temporaryOnly                 bool
	deferDestinationCatalog       bool
}

type explicitIndexTargetResolver func(explicitIndexTarget) error

func checkFrontendIndexTargetInExec(execCtx *ExecCtx, stmt tree.Statement) error {
	return checkFrontendIndexTargetWithResolver(execCtx, stmt, nil)
}

// checkFrontendIndexTargetWithCatalog additionally checks existing relation
// kinds when the current statement already owns a catalog transaction. If no
// transaction exists the caller must repeat it after transaction creation.
func checkFrontendIndexTargetWithCatalog(execCtx *ExecCtx, stmt tree.Statement) error {
	// Privileged callers and explicit server-owned SQL may access index
	// relations; they do not need additional metadata probes.
	if mayAccessIndexRelation(execCtx) {
		return nil
	}
	return checkFrontendIndexTargetWithResolver(execCtx, stmt, func(target explicitIndexTarget) error {
		return checkExplicitIndexTargetCatalog(execCtx, target)
	})
}

func checkExplicitIndexTargetCatalog(execCtx *ExecCtx, target explicitIndexTarget) error {
	if target.table == "" || execCtx == nil || execCtx.ses == nil {
		return nil
	}
	if target.deferDestinationCatalog {
		// The authorized clone/branch owner resolves TO ACCOUNT once and
		// checks the destination with that ID before its mutation admission.
		return nil
	}
	ses := execCtx.ses
	handler := ses.GetTxnHandler()
	if handler == nil || handler.GetTxn() == nil {
		return nil // repeated after transaction creation
	}
	tcc := ses.GetTxnCompileCtx()
	if tcc == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "missing compiler context for index target admission")
	}
	db := target.database
	if db == "" {
		db = effectiveDefaultDatabaseWithRemap(ses, execCtx, execCtx.effectiveTxnDefaultDatabase)
	}
	if db == "" {
		return nil // retain the statement's normal no-database error
	}
	if target.temporaryOnly {
		if _, ok := ses.GetTempTable(db, target.table); !ok {
			if owner, ok := ses.(process.TemporaryTableDDL); !ok || !owner.OwnsTemporaryTable(db, target.table) {
				return nil // a persistent table with this name is not the destination
			}
		}
	}
	if target.publisherAccount != "" && target.publication != "" {
		// Explicit publication snapshots name the publisher's database, not
		// a subscription alias in the caller's account. Reuse the existing
		// publication membership and physical-owner resolution before lookup.
		caller := hiddenIndexCaller(execCtx.reqCtx)
		if caller == nil {
			caller = upstreamUserSession(ses)
		}
		if caller == nil {
			return moerr.NewInternalError(execCtx.reqCtx, "do not have privilege to execute the statement")
		}
		tenant := caller.GetTenantInfo()
		if tenant == nil {
			return moerr.NewInternalError(execCtx.reqCtx, "do not have privilege to execute the statement")
		}
		binding, err := func() (*snapshotPublicationBinding, error) {
			bh := ses.GetShareTxnBackgroundExec(execCtx.reqCtx, false)
			defer bh.Close()
			return resolveSnapshotPublicationBinding(execCtx.reqCtx, bh, tenant.GetTenantID(), tenant.GetTenant(), target.publisherAccount, target.publication)
		}()
		if err != nil {
			return err
		}
		previous := tcc.GetContext()
		tcc.SetContext(defines.AttachAccountId(previous, binding.physID))
		defer tcc.SetContext(previous)
	}
	var snapshot *pbplan.Snapshot
	var err error
	switch {
	case target.cloneSource != nil:
		// Clone execution installs the source account before resolving a hint.
		// Restore it before checking the destination or another statement.
		previous := tcc.GetSnapshot()
		defer tcc.SetSnapshot(previous)
		builder := plan2.NewQueryBuilder(pbplan.Query_INSERT, tcc, false, true)
		snapshot, err = builder.ResolveCloneTableSourceSnapshot(target.cloneSource)
	case target.at != nil:
		builder := plan2.NewQueryBuilder(pbplan.Query_SELECT, tcc, false, false)
		snapshot, err = builder.ResolveTsHint(target.at)
	case target.snapshotName != "":
		snapshot, err = tcc.ResolveSnapshotWithSnapshotName(target.snapshotName)
	case target.pitrTimestamp != "":
		var ts int64
		ts, err = doResolveTimeStamp(target.pitrTimestamp)
		if err == nil {
			snapshot = &pbplan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: ts}}
		}
	}
	if err != nil {
		return err
	}
	resolveTemporary := !target.withoutTemporaryAlias && target.publication == ""
	obj, def, err := tcc.resolve(db, target.table, snapshot, resolveTemporary, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoDB) || moerr.IsMoErrCode(err, moerr.ErrBadDB) ||
			moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) || moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return nil // preserve IF EXISTS and the original missing-target result
		}
		return err
	}
	if def == nil {
		return nil
	}
	physical := def.Name
	if obj != nil && obj.ObjName != "" {
		physical = obj.ObjName
	}
	return checkResolvedIndexRelation(execCtx, target.table, physical, def.TableType, def.FeatureFlag)
}

// checkCloneDestinationAtAccount runs at the authorized owner after its
// existing account resolution and before clone or branch mutation admission.
// Only the catalog namespace changes; index access still uses the caller.
func checkCloneDestinationAtAccount(execCtx *ExecCtx, ses *Session, table *tree.TableName, opAccount, targetAccount uint32) error {
	if execCtx == nil || ses == nil || table == nil {
		return moerr.NewInternalErrorNoCtx("missing clone destination admission context")
	}
	admission := *execCtx
	admission.ses = ses
	name := string(table.ObjectName)
	if err := checkResolvedIndexRelation(&admission, name, name, "", 0); err != nil {
		return err
	}
	if mayAccessIndexRelation(&admission) {
		return nil
	}
	handler := ses.GetTxnHandler()
	tcc := ses.GetTxnCompileCtx()
	if handler == nil || handler.GetTxn() == nil || tcc == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "missing transaction for clone destination admission")
	}
	previous := tcc.GetContext()
	tcc.SetContext(defines.AttachAccountId(previous, targetAccount))
	defer tcc.SetContext(previous)
	if targetAccount != opAccount {
		// An active subscription belongs to the source namespace and must
		// not redirect a same-named database in the destination account.
		previousSub := tcc.GetQueryingSubscription()
		tcc.SetQueryingSubscription(nil)
		defer tcc.SetQueryingSubscription(previousSub)
	}
	return checkExplicitIndexTargetCatalog(&admission, explicitIndexTarget{
		database: string(table.SchemaName), table: name,
		withoutTemporaryAlias: targetAccount != opAccount,
	})
}

func checkFrontendIndexTargetWithResolver(execCtx *ExecCtx, stmt tree.Statement, resolve explicitIndexTargetResolver) error {
	checkTarget := func(target explicitIndexTarget) error {
		if err := checkResolvedIndexRelation(execCtx, target.table, target.table, "", 0); err != nil {
			return err
		}
		if resolve != nil && target.table != "" {
			return resolve(target)
		}
		return nil
	}
	checkTableInScope := func(table *tree.TableName, withoutTemporaryAlias bool) error {
		if table == nil {
			return nil
		}
		return checkTarget(explicitIndexTarget{
			database: string(table.SchemaName), table: string(table.ObjectName), at: table.AtTsExpr,
			withoutTemporaryAlias: withoutTemporaryAlias,
		})
	}
	checkTable := func(table *tree.TableName) error {
		return checkTableInScope(table, false)
	}
	checkCreateDestination := func(table *tree.TableName, temporary bool) error {
		if !temporary {
			return checkTable(table)
		}
		if table == nil {
			return nil
		}
		return checkTarget(explicitIndexTarget{
			database: string(table.SchemaName), table: string(table.ObjectName),
			temporaryOnly: true,
		})
	}
	checkCloneDestination := func(table *tree.TableName, temporary bool, account *tree.ToAccountOpt) error {
		if account == nil || temporary || table == nil {
			return checkCreateDestination(table, temporary)
		}
		return checkTarget(explicitIndexTarget{
			database: string(table.SchemaName), table: string(table.ObjectName),
			deferDestinationCatalog: true,
		})
	}
	checkTables := func(tables tree.TableNames) error {
		for _, table := range tables {
			if err := checkTable(table); err != nil {
				return err
			}
		}
		return nil
	}
	checkAlter := func(alter *tree.AlterTable, withoutTemporaryAlias bool) error {
		if alter == nil {
			return nil
		}
		if err := checkTableInScope(alter.Table, withoutTemporaryAlias); err != nil {
			return err
		}
		sourceTemporary := false
		if !withoutTemporaryAlias && alter.Table != nil && execCtx != nil && execCtx.ses != nil {
			db := string(alter.Table.SchemaName)
			if db == "" {
				db = effectiveDefaultDatabaseWithRemap(execCtx.ses, execCtx, execCtx.effectiveTxnDefaultDatabase)
			}
			name := string(alter.Table.ObjectName)
			_, sourceTemporary = execCtx.ses.GetTempTable(db, name)
			if !sourceTemporary {
				if owner, ok := execCtx.ses.(process.TemporaryTableDDL); ok {
					sourceTemporary = owner.OwnsTemporaryTable(db, name)
				}
			}
		}
		for _, option := range alter.Options {
			if rename, ok := option.(*tree.AlterOptionTableName); ok && rename.Name != nil {
				target := rename.Name.ToTableName()
				// Both planners consume the destination object name in the
				// source table's database, even if another schema was spelled.
				if alter.Table != nil {
					target.SchemaName = alter.Table.SchemaName
				}
				var err error
				if sourceTemporary {
					err = checkCreateDestination(&target, true)
				} else {
					err = checkTableInScope(&target, withoutTemporaryAlias)
				}
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	switch st := stmt.(type) {
	case *tree.CreateTable:
		// The prefix is reserved for physical indexes. Rejecting it here also
		// prevents creating a table that the normal resolver would hide.
		if err := checkCreateDestination(&st.Table, st.Temporary); err != nil {
			return err
		}
		if st.IsAsLike {
			return checkTable(&st.LikeTableName)
		}
	case *tree.DropTable:
		return checkTables(st.Names)
	case *tree.TruncateTable:
		return checkTable(st.Name)
	case *tree.AlterTable:
		return checkAlter(st, false)
	case *tree.RenameTable:
		for _, alter := range st.AlterTables {
			if err := checkAlter(alter, true); err != nil {
				return err
			}
		}
	case *tree.CreateIndex:
		return checkTable(st.Table)
	case *tree.DropIndex:
		return checkTable(st.TableName)
	case *tree.CreateView:
		return checkTable(st.Name)
	case *tree.AlterView:
		return checkTable(st.Name)
	case *tree.DropView:
		return checkTables(st.Names)
	case *tree.CreateSequence:
		return checkTable(st.Name)
	case *tree.AlterSequence:
		return checkTable(st.Name)
	case *tree.DropSequence:
		return checkTables(st.Names)
	case *tree.DumpTable:
		return checkTableInScope(st.Table, true)
	case *tree.LoadTable:
		return checkTableInScope(st.Table, true)
	case *tree.Load:
		return checkTable(st.Table)
	case *tree.AnalyzeStmt:
		for _, entry := range st.Entries {
			if entry != nil {
				if err := checkTable(entry.Table); err != nil {
					return err
				}
			}
		}
	case *tree.CloneTable:
		if err := checkTarget(explicitIndexTarget{
			database: string(st.SrcTable.SchemaName), table: string(st.SrcTable.ObjectName),
			at: st.SrcTable.AtTsExpr, cloneSource: st,
		}); err != nil {
			return err
		}
		return checkCloneDestination(&st.CreateTable.Table, st.CreateTable.Temporary, st.ToAccountOpt)
	case *tree.DataBranchCreateTable:
		if err := checkTable(&st.SrcTable); err != nil {
			return err
		}
		return checkCloneDestination(&st.CreateTable.Table, st.CreateTable.Temporary, st.ToAccountOpt)
	case *tree.DataBranchDeleteTable:
		return checkTable(&st.TableName)
	case *tree.DataBranchDiff:
		if err := checkTable(&st.TargetTable); err != nil {
			return err
		}
		if err := checkTable(&st.BaseTable); err != nil {
			return err
		}
		if st.OutputOpt != nil {
			return checkTable(&st.OutputOpt.As)
		}
	case *tree.DataBranchMerge:
		if err := checkTable(&st.SrcTable); err != nil {
			return err
		}
		return checkTable(&st.DstTable)
	case *tree.DataBranchPick:
		if err := checkTable(&st.SrcTable); err != nil {
			return err
		}
		return checkTable(&st.DstTable)
	case *tree.CreateSnapShot:
		if st.Object.SLevel.Level == tree.SNAPSHOTLEVELTABLE {
			name := string(st.Object.ObjName)
			var db string
			if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
				db, name = name[:dot], name[dot+1:]
			}
			return checkTarget(explicitIndexTarget{
				database: db, table: name,
				publisherAccount: string(st.Object.AccountName), publication: string(st.Object.PubName),
				withoutTemporaryAlias: true,
			})
		}
	case *tree.RestoreSnapShot:
		if st.Level == tree.RESTORELEVELTABLE {
			target := explicitIndexTarget{database: string(st.DatabaseName), table: string(st.TableName), withoutTemporaryAlias: true}
			if err := checkTarget(target); err != nil {
				return err
			}
			target.snapshotName = string(st.SnapShotName)
			return checkTarget(target)
		}
	case *tree.CreatePitr:
		if st.Level == tree.PITRLEVELTABLE {
			return checkTarget(explicitIndexTarget{database: string(st.DatabaseName), table: string(st.TableName), withoutTemporaryAlias: true})
		}
	case *tree.RestorePitr:
		if st.Level == tree.RESTORELEVELTABLE {
			target := explicitIndexTarget{database: string(st.DatabaseName), table: string(st.TableName), withoutTemporaryAlias: true}
			if err := checkTarget(target); err != nil {
				return err
			}
			target.pitrTimestamp = st.TimeStamp
			return checkTarget(target)
		}
	}
	return nil
}
