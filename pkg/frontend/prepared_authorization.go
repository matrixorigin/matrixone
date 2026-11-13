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

func derivePlanAuthorization(ses FeSession, stmt tree.Statement, p *plan.Plan, admission *privilege) planAuthorization {
	result := planAuthorization{privilege: *admission, moCtrl: hasMoCtrl(p), sysWhitelist: isTargetSysWhiteList(p)}
	arr := extractPrivilegeTipsFromPlan(p)
	if _, ok := stmt.(*tree.Replace); ok {
		arr = addReplaceDeletePrivilegeTips(arr, p)
	}
	if merge, ok := stmt.(*tree.Merge); ok {
		arr = appendMergeActionPrivilegeTips(ses, merge, p, arr)
		result.dynamic = true
	}
	if ins, ok := stmt.(*tree.Insert); ok {
		// Preserve the existing fallback when the plan has no concrete
		// object requirements; view-only requirements are not targets.
		hasConcreteRequirement := false
		for _, tip := range arr {
			if !tip.viewOnly {
				hasConcreteRequirement = true
				break
			}
		}
		if !hasConcreteRequirement {
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
	if allowed, err := checkProtectedDatabaseWrite(ctx, ses, requirement.writeTargets...); !allowed || err != nil {
		return false, stats, err
	}
	ok, stats, _, err := determineUserHasPrivilegeSet(ctx, ses, &requirement.privilege)
	return ok, stats, err
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

// Prepared DDL keeps the database binding from PREPARE even if USE changes the
// caller's current database. Check that binding rather than resolving an
// unqualified target against the later session state.
func bindPreparedWriteAuthorization(priv *privilege, p *plan.Plan, defaultDB string) *privilege {
	if priv == nil || len(priv.writeDatabaseTargets) == 0 {
		return priv
	}
	if target := resolvedDDLWriteDatabase(p); target != "" {
		defaultDB = target
	}
	if defaultDB == "" {
		return priv
	}
	copyPriv := *priv
	copyPriv.writeDatabaseTargets = append([]string(nil), priv.writeDatabaseTargets...)
	copyPriv.entries = append([]privilegeEntry(nil), priv.entries...)
	for i, target := range copyPriv.writeDatabaseTargets {
		if target == "" {
			copyPriv.writeDatabaseTargets[i] = defaultDB
		}
	}
	for i := range copyPriv.entries {
		if copyPriv.entries[i].databaseName == "" {
			copyPriv.entries[i].databaseName = defaultDB
		}
	}
	return &copyPriv
}

// resolvedDDLWriteDatabase follows the target bound into the executable plan.
// Multi-target statements retain their complete AST target list as well.
func resolvedDDLWriteDatabase(p *plan.Plan) string {
	ddl := p.GetDdl()
	switch {
	case ddl.GetCreateTable() != nil:
		return ddl.GetCreateTable().GetDatabase()
	case ddl.GetAlterTable() != nil:
		return ddl.GetAlterTable().GetDatabase()
	case ddl.GetDropTable() != nil:
		return ddl.GetDropTable().GetDatabase()
	case ddl.GetTruncateTable() != nil:
		return ddl.GetTruncateTable().GetDatabase()
	case ddl.GetCreateIndex() != nil:
		return ddl.GetCreateIndex().GetDatabase()
	case ddl.GetDropIndex() != nil:
		return ddl.GetDropIndex().GetDatabase()
	case ddl.GetCreateView() != nil:
		return ddl.GetCreateView().GetDatabase()
	case ddl.GetAlterView() != nil:
		return ddl.GetAlterView().GetDatabase()
	case ddl.GetCreateSequence() != nil:
		return ddl.GetCreateSequence().GetDatabase()
	case ddl.GetAlterSequence() != nil:
		return ddl.GetAlterSequence().GetDatabase()
	case ddl.GetDropSequence() != nil:
		return ddl.GetDropSequence().GetDatabase()
	case ddl.GetCloneTable() != nil:
		return ddl.GetCloneTable().GetDstDatabaseName()
	}
	return ""
}
