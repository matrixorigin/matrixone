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
}

func derivePlanAuthorization(ses *Session, stmt tree.Statement, p *plan.Plan, admission *privilege) planAuthorization {
	result := planAuthorization{privilege: *admission, moCtrl: hasMoCtrl(p), sysWhitelist: isTargetSysWhiteList(p)}
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
	ok, stats, _, err := determineUserHasPrivilegeSet(ctx, ses, &requirement.privilege)
	return ok, stats, err
}

func (prepared *PrepareStmt) authorizationRequirements(ses *Session, stmt tree.Statement, p *plan.Plan) *preparedAuthorization {
	if prepared == nil || prepared.PreparePlan.GetDcl().GetPrepare().GetPlan() != p || prepared.PrepareStmt != stmt {
		return nil
	}
	// Context-dependent DDL, MERGE and executable wrappers keep fresh derivation.
	switch stmt.(type) {
	case *tree.Select, *tree.Insert, *tree.Update, *tree.Delete, *tree.Replace:
	default:
		return nil
	}
	if saved := prepared.authorization; saved != nil && saved.source == p && saved.statement == stmt {
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
	prepared.authorization = saved
	return saved
}
