// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func supportsSessionTemporaryDDL(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := v.(int64)
	return ok && version >= defines.MORPCVersion55
}

// A schema transaction sees its own aliases without publishing them into the
// user session. One generation also names every hidden index consistently.
type temporaryDDLSession struct {
	process.Session
	sessionID  uuid.UUID
	generation string
	aliases    map[string]string
}

func (s *temporaryDDLSession) TemporaryTableName(db, alias string) string {
	return defines.GenTempTableName(s.sessionID, db, s.generation+"_"+alias)
}
func (s *temporaryDDLSession) GetTempTable(db, alias string) (string, bool) {
	if name, ok := s.aliases[db+"."+alias]; ok {
		return name, true
	}
	return s.Session.GetTempTable(db, alias)
}
func (s *temporaryDDLSession) AddTempTable(db, alias, name string) { s.aliases[db+"."+alias] = name }
func (s *temporaryDDLSession) AddTempIndexTable(db, alias, name string) {
	s.AddTempTable(db, alias, name)
}
func (s *temporaryDDLSession) RemoveTempTable(db, alias string) { delete(s.aliases, db+"."+alias) }
func (s *temporaryDDLSession) RemoveTempTableByRealName(name string) {
	for alias, physical := range s.aliases {
		if physical == name {
			delete(s.aliases, alias)
		}
	}
}

func temporaryIndexNames(def *plan.TableDef) []string {
	var names []string
	for _, index := range def.GetIndexes() {
		if index.GetIndexTableName() != "" {
			names = append(names, index.GetIndexTableName())
		}
	}
	return names
}

func (s *Scope) createSessionTemporaryTable(c *Compile, owner process.TemporaryTableDDL, tableCreated func()) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	db, alias := qry.Database, qry.TableDef.Name
	if db == "" {
		db = c.db
	}
	if _, exists := c.proc.GetSession().GetTempTable(db, alias); exists {
		if qry.IfNotExists {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.proc.Ctx, alias)
	}
	if err := owner.CheckTemporaryTableCapacity(c.proc.Ctx); err != nil {
		return err
	}
	parentDB, err := c.e.Database(c.proc.Ctx, db, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	parentID := parentDB.GetDatabaseId(c.proc.Ctx)

	stage := &temporaryDDLSession{Session: c.proc.GetSession(), sessionID: c.proc.GetSessionInfo().SessionId,
		generation: uuid.NewString(), aliases: make(map[string]string)}
	physical := stage.TemporaryTableName(db, alias)
	schemaPlan := planutil.DeepCopyPlan(s.Plan)
	schemaPlan.GetDdl().GetCreateTable().CreateAsSelectSql = ""
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalError(c.proc.Ctx, "temporary DDL executor is unavailable")
	}
	exec, ok := v.(executor.SQLExecutor)
	if !ok {
		return moerr.NewInternalError(c.proc.Ctx, "temporary DDL executor has an invalid type")
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	opts := executor.Options{}.WithAccountID(account).WithDatabase(db).
		WithTimeZone(c.proc.GetSessionInfo().TimeZone).WithWaitCommittedLogApplied()

	// Failed schema work or CTAS transfers its exact generation to cleanup.
	// This also covers an ambiguous schema commit and a CTAS panic.
	completed, created := false, false
	defer func() {
		if created && !completed {
			owner.RetireTemporaryTable(db, alias, physical, nil)
		}
	}()
	err = exec.ExecTxn(c.proc.Ctx, func(tx executor.TxnExecutor) (err error) {
		// ExecTxn rolls back returned errors; it does not recover callback panics.
		// Convert here so every admitted schema transaction reaches its owner.
		defer func() {
			if p := recover(); p != nil {
				err = moerr.ConvertPanicError(c.proc.Ctx, p)
			}
		}()
		schemaDB, err := c.e.Database(c.proc.Ctx, db, tx.Txn())
		if err != nil {
			return err
		}
		if schemaDB.GetDatabaseId(c.proc.Ctx) != parentID {
			return moerr.NewNotSupported(c.proc.Ctx, "temporary table in an uncommitted database generation")
		}
		oldTxn, oldSession, oldPlan, oldInternal := c.proc.Base.TxnOperator, c.proc.Session, c.pn, c.isInternal
		c.proc.Base.TxnOperator, c.proc.Session, c.pn, c.isInternal = tx.Txn(), stage, schemaPlan, true
		defer func() {
			c.proc.Base.TxnOperator, c.proc.Session, c.pn, c.isInternal = oldTxn, oldSession, oldPlan, oldInternal
		}()
		tx.Txn().GetWorkspace().SetHaveDDL(true)
		tx.Txn().GetWorkspace().StartStatement()
		defer tx.Txn().GetWorkspace().EndStatement()
		if err := tx.Txn().GetWorkspace().IncrStatementID(c.proc.Ctx, false); err != nil {
			return err
		}
		scope := &Scope{Plan: schemaPlan}
		return scope.createTable(c, func() { created = true })
	}, opts)
	if err != nil {
		return err
	}
	owner.PublishTemporaryTable(db, alias, physical)
	// New metadata is session-visible, but DML continues to use the parent
	// snapshot and workspace. Existing DDL routing keeps this transaction local.
	c.setHaveDDL(true)
	if err := c.populateCreatedTable(qry, true, db, alias, physical); err != nil {
		return err
	}
	if tableCreated != nil {
		tableCreated()
	}
	completed = true
	return nil
}

var _ process.Session = (*temporaryDDLSession)(nil)
