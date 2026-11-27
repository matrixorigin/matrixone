// Copyright 2021 - 2024 Matrix Origin
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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// migrateController is created in Routine and used to serialize lifecycle
// operations with routine cleanup.
type migrateController struct {
	sync.Mutex
	migrateOnce sync.Once
	// migrateErr is the stable result returned to duplicate migration calls.
	migrateErr error
	// closed indicates if the session has been closed.
	closed bool
	// inProgress indicates if a lifecycle operation is in progress.
	inProgress bool
	// operationCancel cancels the active lifecycle operation. It is published
	// together with inProgress while holding the controller lock.
	operationCancel context.CancelFunc
	// cond coordinates lifecycle operations and routine cleanup.
	cond *sync.Cond
	// the id of goroutine that executes the migration
	goroutineID uint64
}

func newMigrateController() *migrateController {
	mc := &migrateController{}
	mc.cond = sync.NewCond(&mc.Mutex)
	return mc
}

// waitAndClose is called before the routine is cleaned up. If a lifecycle
// operation is in progress, it waits for that operation to finish and marks
// the routine closed.
func (mc *migrateController) waitAndClose() {
	mc.startClose()
	mc.Lock()
	defer mc.Unlock()
	for mc.inProgress {
		mc.cond.Wait()
	}
}

// startClose seals lifecycle admission and cancels the active operation. It
// does not wait for the operation to finish, but acquiring its mutex can wait.
func (mc *migrateController) startClose() {
	mc.Lock()
	mc.closed = true
	cancel := mc.operationCancel
	mc.cond.Broadcast()
	mc.Unlock()
	if cancel != nil {
		cancel()
	}
}

// tryStartClose seals lifecycle admission and returns the active operation's
// cancellation function without waiting for the controller lock.
func (mc *migrateController) tryStartClose() (context.CancelFunc, bool) {
	if !mc.TryLock() {
		return nil, false
	}
	mc.closed = true
	cancel := mc.operationCancel
	mc.cond.Broadcast()
	mc.Unlock()
	return cancel, true
}

// tryStartCloseWhenIdle prevents new lifecycle operations without waiting for
// or interrupting one that has already started.
func (mc *migrateController) tryStartCloseWhenIdle() bool {
	if !mc.TryLock() {
		return false
	}
	defer mc.Unlock()
	if mc.inProgress {
		return false
	}
	mc.closed = true
	mc.cond.Broadcast()
	return true
}

// beginOperation starts a lifecycle operation unless the routine is closed or
// another operation is already running.
func (mc *migrateController) beginOperation() bool {
	_, ok := mc.beginOperationWithContext(context.Background())
	return ok
}

// beginOperationWithContext starts a cancelable lifecycle operation. A waiting
// caller is released by operation completion, routine close, or its own
// context cancellation.
func (mc *migrateController) beginOperationWithContext(ctx context.Context) (context.Context, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	stopWakeup := context.AfterFunc(ctx, func() {
		mc.Lock()
		mc.cond.Broadcast()
		mc.Unlock()
	})
	defer stopWakeup()

	mc.Lock()
	defer mc.Unlock()
	for mc.inProgress && !mc.closed && ctx.Err() == nil {
		mc.cond.Wait()
	}
	return mc.startOperationLocked(ctx)
}

// tryBeginOperation starts a lifecycle operation only if it can proceed
// immediately.
func (mc *migrateController) tryBeginOperation() bool {
	_, ok := mc.tryBeginOperationWithContext(context.Background())
	return ok
}

func (mc *migrateController) tryBeginOperationWithContext(ctx context.Context) (context.Context, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	mc.Lock()
	defer mc.Unlock()
	return mc.startOperationLocked(ctx)
}

func (mc *migrateController) startOperationLocked(ctx context.Context) (context.Context, bool) {
	if mc.closed || ctx.Err() != nil {
		return nil, false
	}
	if mc.inProgress {
		return nil, false
	}
	operationCtx, cancel := context.WithCancel(ctx)
	mc.goroutineID = GetRoutineId()
	mc.inProgress = true
	mc.operationCancel = cancel
	return operationCtx, true
}

// endOperation completes a lifecycle operation and wakes a routine waiting
// to close.
func (mc *migrateController) endOperation() {
	mc.Lock()
	defer mc.Unlock()
	if mc.operationCancel != nil {
		mc.operationCancel()
		mc.operationCancel = nil
	}
	mc.inProgress = false
	mc.goroutineID = 0
	mc.cond.Broadcast()
}

func (mc *migrateController) getGoroutineId() uint64 {
	mc.Lock()
	defer mc.Unlock()
	return mc.goroutineID
}

// Only verified, unrewritten SQL can currently be replayed without moving
// additional session state. Comparisons include the authenticated role, the
// preparation database and SQL mode, and client-visible protocol metadata.
func migrationPrepareProof(ses *Session, stmt *PrepareStmt) []byte {
	if ses.GetTenantInfo() == nil || stmt == nil || stmt.roleRules == nil || len(stmt.roleRules.first) != 0 ||
		stmt.optionalRewrites || stmt.IsCloudNonuser || stmt.Sql == "" ||
		len(stmt.getFromSendLongData) != 0 || len(stmt.remapDb) != 0 ||
		!migrationTempTablesMatch(ses, stmt) ||
		stmt.defaultDatabase != ses.GetDatabaseName() ||
		stmt.schedulingSQLMode != sessionSQLModeForParser(ses) ||
		stmt.NativeMode != ses.sqlModeHasMatrixOneNative() {
		return nil
	}
	if !canReplayMigrationStatement(stmt.PrepareStmt) {
		return nil
	}
	control := stmt.PreparePlan.GetDcl().GetPrepare()
	if control == nil {
		return nil
	}
	principal := currentAuthorizationPrincipal(ses)
	proof, err := json.Marshal(struct {
		Account, User, Role uint32
		Secondary, Native   bool
		Name, SQL, DB, Mode string
		ParamTypes          []byte
		Columns             [][]byte
		ParameterCount      int
		LowerCaseTableNames int64
		OptimizerHints      string
	}{
		Account: principal.account, User: principal.user, Role: principal.role,
		Secondary: principal.secondary, Native: stmt.NativeMode,
		Name: stmt.Name, SQL: stmt.Sql, DB: stmt.defaultDatabase, Mode: stmt.schedulingSQLMode,
		ParamTypes: stmt.ParamTypes, Columns: stmt.ColDefData, ParameterCount: len(control.ParamTypes),
		LowerCaseTableNames: parserLowerCaseTableNames(ses), OptimizerHints: stmt.optimizerHints,
	})
	if err != nil {
		return nil
	}
	digest := sha256.Sum256(proof)
	return digest[:]
}

func migrationTempTablesMatch(ses *Session, stmt *PrepareStmt) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return len(ses.tempTables) == 0 && stmt.tempTableVersion == ses.tempTableVersion
}

func canReplayMigrationStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.Insert, *tree.Update, *tree.Delete, *tree.Replace:
		return true
	default:
		return false
	}
}

func exportMigrationPrepares(ctx context.Context, ses *Session) ([]*query.PrepareStmt, error) {
	statements := ses.GetPrepareStmts()
	if len(statements) == 0 {
		return nil, nil
	}
	exported := make([]*query.PrepareStmt, 0, len(statements))
	for _, stmt := range statements {
		proof := migrationPrepareProof(ses, stmt)
		if len(proof) == 0 {
			return nil, moerr.GetOkExpectedNotSafeToStartTransfer()
		}
		exported = append(exported, &query.PrepareStmt{
			Name: stmt.Name, SQL: stmt.Sql, ParamTypes: slices.Clone(stmt.ParamTypes), ReplayProof: proof,
		})
	}
	// A role or rule publication can invalidate a plan after it was prepared.
	rules, err := captureRoleRuleCache(ctx, ses)
	if err != nil {
		return nil, err
	}
	if len(rules.first) != 0 {
		return nil, moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	return exported, nil
}

func replayMigrationPrepares(ctx context.Context, ses *Session, stmts []*query.PrepareStmt) (err error) {
	if len(stmts) == 0 {
		return nil
	}
	if len(ses.GetPrepareStmts()) != 0 {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	var maxID uint32
	defer func() {
		if err != nil {
			for _, stmt := range stmts {
				ses.RemovePrepareStmt(stmt.Name)
			}
		}
	}()
	for _, wire := range stmts {
		if err = replayMigrationPrepare(ctx, ses, wire); err != nil {
			return err
		}
		maxID = max(maxID, parsePrepareStmtID(wire.Name))
	}
	if err = context.Cause(ctx); err != nil {
		return err
	}
	ses.SetLastStmtID(maxID)
	return nil
}

func replayMigrationPrepare(ctx context.Context, ses *Session, wire *query.PrepareStmt) error {
	ec := &ExecCtx{reqCtx: ctx, inMigration: true}
	beginStatementAuthorization(ses, ec)
	defer ec.Close()
	rules, err := captureRoleRuleCache(ctx, ses)
	if err != nil {
		return err
	}
	if len(rules.first) != 0 {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	mode := sessionSQLModeForParser(ses)
	// The exported text must describe a single replayable parameterized
	// statement. Preparing it never executes the inner query.
	parsed, err := mysql.ParseWithSQLMode(ctx, wire.SQL, parserLowerCaseTableNames(ses), mode)
	if err != nil {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	supported := len(parsed) == 1 && canReplayMigrationStatement(parsed[0])
	freeStatements(parsed)
	if !supported {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	input := &UserInput{rewritePolicy: &rewritePolicySnapshot{roleRules: rules}, rewritePolicyMaterialized: true}
	stmt := tree.NewPrepareString(tree.Identifier(wire.Name), wire.SQL)
	input.stmt = stmt
	input.sql = "prepare " + quoteIdentifierForSQL(wire.Name) + " from ''"
	// PREPARE FROM a string retains ownership of the parsed inner statement.
	if err := doComQuery(ses, ec, input); err != nil {
		return err
	}
	replayed, err := ses.GetPrepareStmt(ctx, wire.Name)
	if err != nil {
		return err
	}
	replayed.ParamTypes = slices.Clone(wire.ParamTypes)
	if mode != replayed.schedulingSQLMode || !bytes.Equal(wire.ReplayProof, migrationPrepareProof(ses, replayed)) {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	return nil
}
