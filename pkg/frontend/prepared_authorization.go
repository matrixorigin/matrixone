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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// These are immutable requirements of a selected plan, never an admission result.
// PrepareStmt owns their lifetime and clears them when it replaces its plan.
type preparedAuthorization struct {
	source    *plan.Plan
	statement tree.Statement
	admission *privilege
	plan      planAuthorization
}

type planAuthorization struct {
	privilege                      privilege
	writeTargets                   []string
	required, moCtrl, sysWhitelist bool
	dynamic                        bool
	createIndexTable               *plan.TableDef
}

func derivePlanAuthorization(ses *Session, stmt tree.Statement, p *plan.Plan, admission *privilege) planAuthorization {
	result := planAuthorization{privilege: *admission, moCtrl: hasMoCtrl(p), sysWhitelist: isTargetSysWhiteList(p)}
	if createIndex := p.GetDdl().GetCreateIndex(); createIndex != nil {
		result.createIndexTable = createIndex.GetTableDef()
	}
	arr := extractPrivilegeTipsFromPlan(p)
	if _, ok := stmt.(*tree.Replace); ok {
		arr = addReplaceDeletePrivilegeTips(arr, p)
	}
	if merge, ok := stmt.(*tree.Merge); ok {
		arr = appendMergeActionPrivilegeTips(ses, merge, p, arr)
		result.dynamic = true
	}
	if len(arr) == 0 {
		if ins, ok := stmt.(*tree.Insert); ok {
			result.dynamic = true
			if db, table, ok := getInsertTargetTableName(ins, ses); ok {
				arr = append(arr, privilegeTips{typ: PrivilegeTypeInsert, objType: objectTypeTable, databaseName: db, tableName: table, isClusterTable: isClusterTable(db, table), clusterTableOperation: clusterTableModify})
			}
		}
	}
	result.required = len(arr) != 0
	if !result.required {
		return result
	}
	for i := range arr {
		// Compiles can own mutable plan annotations; retained requirements own theirs.
		arr[i].originViews = append([]string(nil), arr[i].originViews...)
		arr[i].scanSnapshot = plan2.DeepCopySnapshot(arr[i].scanSnapshot)
		if privilegeTipWritesDatabase(arr[i]) {
			result.writeTargets = append(result.writeTargets, arr[i].databaseName)
		}
	}
	convertPrivilegeTipsToPrivilege(&result.privilege, arr)
	return result
}

func evaluatePlanAuthorization(ctx context.Context, ses *Session, requirement *planAuthorization) (bool, statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()
	if requirement.moCtrl && !verifyAccountCanExecMoCtrl(ses.GetTenantInfo()) {
		return false, stats, moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
	}
	if requirement.sysWhitelist && verifyAccountCanExecMoCtrl(ses.GetTenantInfo()) {
		ok, stats, _, err := determineUserHasPrivilegeSet(ctx, ses, nil)
		return ok, stats, err
	}
	if !requirement.required {
		return true, stats, nil
	}
	if !checkProtectedDatabaseWrite(ctx, ses, requirement.writeTargets...) {
		return false, stats, nil
	}
	priv := &requirement.privilege
	if requirement.createIndexTable != nil {
		owner, err := indexTargetOwnerInTransaction(ctx, ses, requirement.createIndexTable)
		if err != nil {
			return false, stats, err
		}
		if owner != nil {
			local := *priv
			local.transactionTableOwner = owner
			priv = &local
		}
	}
	ok, stats, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
	return ok, stats, err
}

type transactionTableOwner struct {
	roleID   int64
	database string
	table    string
}

// indexTargetOwnerInTransaction reads only the ownership of the plan's new
// table. Role membership and committed privileges still use the authorization
// snapshot, and the ordinary privilege cache never records this local grant.
func indexTargetOwnerInTransaction(ctx context.Context, ses *Session, table *plan.TableDef) (*transactionTableOwner, error) {
	txn := ses.GetTxnHandler().GetTxn()
	if txn == nil {
		return nil, nil
	}
	workspace, ok := txn.GetWorkspace().(interface{ IsTableCreatedInTxn(uint64) bool })
	physicalID, logicalID := table.GetTblId(), table.GetLogicalId()
	if !ok || physicalID == 0 || logicalID == 0 ||
		!workspace.IsTableCreatedInTxn(physicalID) || !workspace.IsTableCreatedInTxn(logicalID) {
		return nil, nil
	}

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	// Use the catalog's physical and logical IDs so a same-named table or a
	// replacement of an existing table cannot supply ownership for this plan.
	sql := fmt.Sprintf(`select t.owner from mo_catalog.mo_tables t
where t.account_id = current_account_id() and t.rel_id = %d and t.rel_logical_id = %d
and exists (select 1 from mo_catalog.mo_role_privs p
where p.obj_type = 'table' and p.obj_id = t.rel_logical_id and p.role_id = t.owner
and p.privilege_id = %d and p.privilege_level in ('d.t', 't'))`,
		physicalID, logicalID, PrivilegeTypeTableOwnership)
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil || !execResultArrayHasData(rows) {
		return nil, err
	}
	owner, err := rows[0].GetInt64(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	return &transactionTableOwner{roleID: owner, database: table.GetDbName(), table: table.GetName()}, nil
}

func (prepareStmt *PrepareStmt) authorizationRequirements(ses *Session, stmt tree.Statement, p *plan.Plan) *preparedAuthorization {
	if prepareStmt == nil || prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan() != p || prepareStmt.PrepareStmt != stmt {
		return nil
	}
	// Context-dependent DDL, MERGE and executable wrappers keep fresh derivation.
	switch stmt.(type) {
	case *tree.Select, *tree.Insert, *tree.Update, *tree.Delete, *tree.Replace:
	default:
		return nil
	}
	if saved := prepareStmt.authorization; saved != nil && saved.source == p && saved.statement == stmt {
		return saved
	}
	admission := determinePrivilegeSetOfStatement(stmt)
	if admission.needMatchedRole || admission.objectType() != objectTypeTable {
		return nil
	}
	derived := derivePlanAuthorization(ses, stmt, p, admission)
	if derived.dynamic {
		return nil
	}
	saved := &preparedAuthorization{source: p, statement: stmt, admission: admission, plan: derived}
	prepareStmt.authorization = saved
	return saved
}
