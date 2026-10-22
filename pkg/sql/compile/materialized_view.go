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
	"fmt"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/mv"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (c *Compile) mvTxn(f func(executor.TxnExecutor) error) error {
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalError(c.proc.Ctx, "materialized view SQL executor unavailable")
	}
	// The outer DDL/scan owns the active statement workspace. Nested catalog
	// operations reuse it without starting a second statement or advancing S.
	return v.(executor.SQLExecutor).ExecTxn(c.proc.Ctx, f, executor.Options{}.WithTxn(c.proc.GetTxnOperator()).WithDatabase(c.db).WithDisableIncrStatement())
}

func (c *Compile) registerMaterializedView(db engine.Database, dbName, name string, def *plan.TableDef) error {
	definition, err := catalog.MaterializedViewDefinition(def)
	if err != nil {
		return err
	}
	if definition == nil {
		return nil
	}
	if err := plan2.RequirePersistedProtocolVersionForAuthoring(c.proc.Ctx, c.proc, defines.MORPCVersion129); err != nil {
		return err
	}
	r, err := db.Relation(c.proc.Ctx, name, nil)
	if err != nil {
		return err
	}
	dbID, err := strconv.ParseUint(db.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return err
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return c.mvTxn(func(tx executor.TxnExecutor) error {
		return mv.Register(tx, mv.Key{Account: account, ID: r.GetTableID(c.proc.Ctx)}, dbID, name, definition)
	})
}

func (c *Compile) validateMaterializedViewScan(def *plan.TableDef) error {
	if def == nil || def.TableType != catalog.SystemMaterializedViewRel {
		return nil
	}
	if err := plan2.RequirePersistedProtocolVersion(c.proc.Ctx, c.proc, defines.MORPCVersion129); err != nil {
		return err
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	if defines.CanWriteMaterializedView(c.proc.Ctx, account, def.TblId, c.proc.GetTxnOperator().Txn().ID) {
		return nil
	}
	return c.mvTxn(func(tx executor.TxnExecutor) error {
		st, err := mv.Load(tx, mv.Key{Account: account, ID: def.TblId})
		if err != nil {
			return err
		}
		if !st.Populated {
			return moerr.NewInvalidInput(c.proc.Ctx, "MV_NOT_POPULATED: refresh materialized view before querying it")
		}
		return mv.ValidateSources(tx, st)
	})
}

// Guard source DDL and the MV through durable dependencies after existing
// lifecycle/name admission has acquired the target locks and refreshed RC state.
// Pessimistic CREATE MV pins those source names through publication. Optimistic
// SI has no name locks and retains a write/write fence through commit.
func (c *Compile) guardMaterializedViewDDL(dbName, name string, allowMV bool) error {
	if dbName == "" {
		dbName = c.db
	}
	if session := c.proc.GetSession(); session != nil {
		if _, temporary := session.GetTempTable(dbName, name); temporary {
			return nil
		}
	}
	if !catalog.IsMaterializedViewDatabase(dbName) {
		return nil
	}
	if _, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(mv.RuntimeKey); !ok {
		return nil
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	db, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	r, err := db.Relation(c.proc.Ctx, name, nil)
	if err != nil {
		return err
	}
	def := r.GetTableDef(c.proc.Ctx)
	if def.TableType == catalog.SystemMaterializedViewRel && !allowMV && !defines.CanWriteMaterializedView(c.proc.Ctx, account, def.TblId, c.proc.GetTxnOperator().Txn().ID) {
		return moerr.NewInvalidInput(c.proc.Ctx, "materialized view is read only; use DROP/ALTER MATERIALIZED VIEW")
	}
	return c.mvTxn(func(tx executor.TxnExecutor) error {
		ready, err := mv.CatalogReady(tx)
		if err != nil || !ready {
			return err
		}
		if !c.proc.GetTxnOperator().Txn().IsPessimistic() {
			if err := mv.DependencyGuard(tx, account); err != nil {
				return err
			}
		}
		res, err := tx.Exec(fmt.Sprintf("select mv_id from mo_catalog.mo_mv_dependencies where account_id=%d and source_table_id=%d limit 1", account, def.TblId), executor.StatementOption{}.WithAccountID(catalog.System_Account))
		if err != nil {
			return err
		}
		defer res.Close()
		rows := 0
		res.ReadRows(func(n int, _ []*vector.Vector) bool { rows += n; return false })
		if rows > 0 {
			return moerr.NewInvalidInput(c.proc.Ctx, "source has dependent materialized views; drop them before changing its schema/identity")
		}
		return nil
	})
}

// Both ordinary scopes and dependent programs enter through validateDMLTarget.
// Keep the transaction-bound write capability check before any target operator.
func (c *Compile) validateMaterializedViewDML(node *plan.Node) error {
	switch node.NodeType {
	case plan.Node_INSERT:
		if node.InsertCtx != nil {
			return c.validateMaterializedViewWrite(node.InsertCtx.TableDef)
		}
	case plan.Node_DELETE:
		if node.DeleteCtx != nil {
			return c.validateMaterializedViewWrite(node.DeleteCtx.TableDef)
		}
	case plan.Node_MULTI_UPDATE:
		for _, update := range node.UpdateCtxList {
			if err := c.validateMaterializedViewWrite(update.TableDef); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Compile) validateMaterializedViewWrite(def *plan.TableDef) error {
	if def == nil || def.TableType != catalog.SystemMaterializedViewRel {
		return nil
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	if !defines.CanWriteMaterializedView(c.proc.Ctx, account, def.TblId, c.proc.GetTxnOperator().Txn().ID) {
		return moerr.NewInvalidInput(c.proc.Ctx, "materialized view is read only")
	}
	return nil
}

// DROP DATABASE uses RESTRICT while materialized views or their sources exist.
// Dropping the views first leaves no global catalog rows orphaned.
func (c *Compile) guardMaterializedViewDatabaseDDL(dbName string) error {
	if !catalog.IsMaterializedViewDatabase(dbName) {
		return nil
	}
	if _, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(mv.RuntimeKey); !ok {
		return nil
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return c.mvTxn(func(tx executor.TxnExecutor) error {
		ready, err := mv.CatalogReady(tx)
		if err != nil || !ready {
			return err
		}
		if !c.proc.GetTxnOperator().Txn().IsPessimistic() {
			if err := mv.DependencyGuard(tx, account); err != nil {
				return err
			}
		}
		r, err := tx.Exec(fmt.Sprintf("select mv_id from mo_catalog.mo_materialized_views where account_id=%d and database_name='%s' limit 1", account, sqlquote.EscapeString(dbName)), executor.StatementOption{}.WithAccountID(catalog.System_Account))
		if err != nil {
			return err
		}
		defer r.Close()
		rows := 0
		r.ReadRows(func(n int, _ []*vector.Vector) bool { rows += n; return false })
		if rows != 0 {
			return moerr.NewInvalidInput(c.proc.Ctx, "database contains materialized views; drop them first")
		}
		return nil
	})
}

// SI creation pins the complete source/target name domain before publication.
// RC uses pinCreateNames, including source names, and its TN-ordered snapshot.
func (c *Compile) pinMaterializedViewSourceNames(q *plan.CreateTable, database string) error {
	definition, err := catalog.MaterializedViewDefinition(q.TableDef)
	if err != nil || definition == nil {
		return err
	}
	if !c.proc.GetTxnOperator().Txn().IsPessimistic() {
		return nil
	}
	if c.disableLock {
		return moerr.NewInternalError(c.proc.Ctx, "materialized view creation requires lifecycle name locks")
	}
	if c.isLifecycleRC() {
		return nil
	}
	if err := lockMoDatabase(c, database, lock.LockMode_Shared); err != nil {
		return err
	}
	names := make([]string, 0, len(definition.Sources)+1)
	names = append(names, q.TableDef.Name)
	for _, source := range definition.Sources {
		names = append(names, source.Table)
	}
	sort.Strings(names)
	for i, name := range names {
		if i > 0 && name == names[i-1] {
			continue
		}
		if err := lockMoTable(c, database, name, lock.LockMode_Exclusive); err != nil {
			return err
		}
	}
	return nil
}

// Empty-result compilation must still reach the existing MV scan validation.
// This bounded metadata walk runs only for a literal LIMIT 0 candidate; it does
// not fetch catalogs or create a second population/privilege validator.
func (c *Compile) containsMaterializedViewScan(root *plan.Node) bool {
	query := c.pn.GetQuery()
	queue := []*plan.Node{root}
	seen := make(map[*plan.Node]struct{})
	for len(queue) > 0 {
		node := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if node == nil {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.GetTableType() == catalog.SystemMaterializedViewRel {
			return true
		}
		for _, child := range node.Children {
			if child >= 0 && int(child) < len(query.GetNodes()) {
				queue = append(queue, query.Nodes[child])
			}
		}
		for _, step := range node.SourceStep {
			if step >= 0 && int(step) < len(query.GetSteps()) {
				child := query.Steps[step]
				if child >= 0 && int(child) < len(query.GetNodes()) {
					queue = append(queue, query.Nodes[child])
				}
			}
		}
	}
	return false
}
