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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type viewRecoveryWork struct {
	kind string
	viewRefreshTarget
	cursorAccount, cursorRelation, visits uint64
}

func viewRecoveryUint(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }

func viewRecoveryQuery(txn executor.TxnExecutor, sql string, columns int) ([][]string, error) {
	result, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return nil, err
	}
	return viewRecoveryStrings(result, columns)
}

func viewRecoveryWorkKey(generation uint64, w viewRecoveryWork) string {
	return fmt.Sprintf("generation=%d and kind='%s' and account_id=%d and relation_id=%d", generation, sqlquote.EscapeString(w.kind), w.accountID, w.relationID)
}

func enqueueViewRecoveryWork(txn executor.TxnExecutor, s *ViewRecoveryState, w viewRecoveryWork) error {
	_, err := insertViewRecoveryWork(txn, s, w)
	return err
}

func insertViewRecoveryWork(txn executor.TxnExecutor, s *ViewRecoveryState, w viewRecoveryWork) (bool, error) {
	if s.WorkRows >= viewRecoveryMaxWork {
		existing, err := viewRecoveryQuery(txn, "select kind from mo_catalog.mo_view_recovery_work where "+viewRecoveryWorkKey(s.Generation, w), 1)
		if err != nil {
			return false, err
		}
		if len(existing) == 1 {
			return false, nil
		}
		return false, moerr.NewInvalidStateNoCtx("View recovery work budget exhausted")
	}
	count, err := viewRecoveryExec(txn, fmt.Sprintf("insert into mo_catalog.mo_view_recovery_work (generation,kind,account_id,relation_id,database_id,logical_id,database_name,relation_name) select %d,'%s',%d,%d,%d,%d,'%s','%s' where not exists (select 1 from mo_catalog.mo_view_recovery_work where %s)", s.Generation, w.kind, w.accountID, w.relationID, w.databaseID, w.logicalID, sqlquote.EscapeString(w.databaseName), sqlquote.EscapeString(w.relationName), viewRecoveryWorkKey(s.Generation, w)))
	if err != nil {
		return false, err
	}
	if count > 1 {
		return false, moerr.NewInvalidStateNoCtx("invalid View recovery work insertion")
	}
	s.WorkRows += count
	return count == 1, nil
}

func cleanupViewRecoveryWork(txn executor.TxnExecutor, s *ViewRecoveryState) (uint64, error) {
	predicate := fmt.Sprintf("generation<%d", s.Generation)
	if s.Generation == s.Completed {
		predicate = fmt.Sprintf("generation<=%d", s.Generation)
	}
	rows, err := viewRecoveryQuery(txn, "select cast(generation as char),kind,cast(account_id as char),cast(relation_id as char) from mo_catalog.mo_view_recovery_work where "+predicate+" order by generation,kind,account_id,relation_id limit 32", 4)
	if err != nil {
		return 0, err
	}
	var deleted uint64
	for _, row := range rows {
		generation, err := viewRecoveryUint(row[0])
		if err != nil {
			return 0, err
		}
		account, err := viewRecoveryUint(row[2])
		if err != nil || account > math.MaxUint32 {
			return 0, moerr.NewInvalidStateNoCtx("invalid View recovery work account")
		}
		relation, err := viewRecoveryUint(row[3])
		if err != nil {
			return 0, err
		}
		count, err := viewRecoveryExec(txn, "delete from mo_catalog.mo_view_recovery_work where "+viewRecoveryWorkKey(generation, viewRecoveryWork{kind: row[1], viewRefreshTarget: viewRefreshTarget{accountID: uint32(account), relationID: relation}}))
		if err != nil {
			return 0, err
		}
		deleted += count
	}
	if deleted > s.WorkRows {
		return 0, moerr.NewInvalidStateNoCtx("View recovery work quota is inconsistent")
	}
	s.WorkRows -= deleted
	return deleted, nil
}

func viewRecoveryScopePredicate(scope ViewRecoveryScope, prefix string, source bool) string {
	if scope.All {
		return "true"
	}
	account, database, relation := prefix+"account_id", prefix+"reldatabase", prefix+"relname"
	if source {
		account, database, relation = prefix+"source_account_id", prefix+"source_database_name", prefix+"source_relation_name"
	}
	predicate := fmt.Sprintf("%s=%d", account, scope.AccountID)
	if scope.Database != "" {
		predicate += fmt.Sprintf(" and %s='%s'", database, sqlquote.EscapeString(scope.Database))
	}
	if scope.Relation != "" {
		predicate += fmt.Sprintf(" and %s='%s'", relation, sqlquote.EscapeString(scope.Relation))
	}
	if source && scope.Database != "" {
		// A subscriber-local database mutation names the binding namespace,
		// whereas the physical edge is owned by the publisher's account/database.
		binding := fmt.Sprintf("%saccount_id=%d and %ssubscription_name='%s'", prefix, scope.AccountID, prefix, sqlquote.EscapeString(scope.Database))
		if scope.Relation != "" {
			binding += fmt.Sprintf(" and %s='%s'", relation, sqlquote.EscapeString(scope.Relation))
		}
		predicate = "(" + predicate + ") or (" + binding + ")"
	}
	return predicate
}

func loadNextViewRecoveryWork(txn executor.TxnExecutor, generation uint64) (*viewRecoveryWork, error) {
	rows, err := viewRecoveryQuery(txn, fmt.Sprintf("select kind,cast(account_id as char),cast(relation_id as char),cast(database_id as char),cast(logical_id as char),database_name,relation_name,cast(cursor_account as char),cast(cursor_relation as char),cast(visits as char) from mo_catalog.mo_view_recovery_work where generation=%d and done=false order by visits,kind,account_id,relation_id limit 1", generation), 10)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	row := rows[0]
	values := make([]uint64, 7)
	for i, column := range []int{1, 2, 3, 4, 7, 8, 9} {
		values[i], err = viewRecoveryUint(row[column])
		if err != nil {
			return nil, err
		}
	}
	if values[0] > math.MaxUint32 || values[4] > math.MaxUint32 {
		return nil, moerr.NewInvalidStateNoCtx("invalid View recovery work account")
	}
	return &viewRecoveryWork{kind: row[0], viewRefreshTarget: viewRefreshTarget{accountID: uint32(values[0]), relationID: values[1], databaseID: values[2], logicalID: values[3], databaseName: row[5], relationName: row[6]}, cursorAccount: values[4], cursorRelation: values[5], visits: values[6]}, nil
}

func viewRecoveryTargetRow(row []string) (viewRefreshTarget, error) {
	var target viewRefreshTarget
	account, err := viewRecoveryUint(row[0])
	if err != nil || account > math.MaxUint32 {
		return target, moerr.NewInvalidStateNoCtx("invalid View recovery target account")
	}
	target.accountID = uint32(account)
	target.databaseID, err = viewRecoveryUint(row[1])
	if err != nil {
		return target, err
	}
	target.relationID, err = viewRecoveryUint(row[2])
	if err != nil {
		return target, err
	}
	target.logicalID, err = viewRecoveryUint(row[3])
	if err != nil {
		return target, err
	}
	target.databaseName = row[4]
	target.relationName = row[5]
	return target, nil
}

// Each graph node is invalidated exactly once per catalog generation. The
// durable visited set, not an in-memory traversal, closes cycles and diamonds.
func seedViewRecoveryTarget(txn executor.TxnExecutor, s *ViewRecoveryState, target viewRefreshTarget) error {
	inserted, err := insertViewRecoveryWork(txn, s, viewRecoveryWork{kind: "node", viewRefreshTarget: target})
	if err != nil || !inserted {
		return err
	}
	_, err = viewRecoveryExec(txn, fmt.Sprintf("replace into mo_catalog.mo_view_refresh (%s) select t.account_id,t.reldatabase_id,t.rel_id,coalesce(nullif(t.rel_logical_id,0),t.rel_id),t.reldatabase,t.relname,coalesce(r.target_generation+1,1),coalesce(r.completed_generation,0),'DISCOVERING',0,null,'',coalesce(r.lease_epoch,0)+1,null,0 from mo_catalog.mo_tables t left join mo_catalog.mo_view_refresh r on r.account_id=t.account_id and r.target_relation_id=t.rel_id where t.account_id=%d and t.rel_id=%d and t.relkind='v'", catalog.MoViewRefreshColumns, target.accountID, target.relationID))
	return err
}

func advanceViewRecoveryPage(txn executor.TxnExecutor, s *ViewRecoveryState, attempted *viewRefreshTarget) (bool, error) {
	// Reclaim old generations first. The total quota includes unreclaimed rows.
	reclaimed, err := cleanupViewRecoveryWork(txn, s)
	if err != nil {
		return false, err
	}
	if reclaimed != 0 {
		return true, nil
	}
	work, err := loadNextViewRecoveryWork(txn, s.Generation)
	if err != nil {
		return false, err
	}
	if work == nil {
		return refreshViewRecoveryTarget(txn, s, attempted)
	}
	if work.visits == math.MaxUint64 {
		return false, moerr.NewInvalidStateNoCtx("View recovery page counter exhausted")
	}
	oldAccount, oldRelation := work.cursorAccount, work.cursorRelation
	var rows [][]string
	if work.kind == "orphan" || work.kind == "edges" {
		table := catalog.MO_VIEW_REFRESH
		if work.kind == "edges" {
			table = catalog.MO_VIEW_DEPENDENCIES
		}
		predicate := fmt.Sprintf("r.account_id>%d or (r.account_id=%d and r.target_relation_id>%d)", work.cursorAccount, work.cursorAccount, work.cursorRelation)
		scope := viewRecoveryScopePredicate(s.Scope, "r.", false)
		scope = strings.ReplaceAll(strings.ReplaceAll(scope, "r.reldatabase", "r.target_database_name"), "r.relname", "r.target_relation_name")
		rows, err = viewRecoveryQuery(txn, fmt.Sprintf("select cast(q.account_id as char),cast(q.target_database_id as char),cast(q.target_relation_id as char),cast(q.target_logical_id as char),q.target_database_name,q.target_relation_name from (select distinct r.account_id,r.target_database_id,r.target_relation_id,r.target_logical_id,r.target_database_name,r.target_relation_name from mo_catalog.%s r where r.target_relation_id<>0 and (%s) and (%s) order by r.account_id,r.target_relation_id limit 32) q order by q.account_id,q.target_relation_id", table, scope, predicate), 6)
		if err != nil {
			return false, err
		}
		for _, row := range rows {
			target, err := viewRecoveryTargetRow(row)
			if err != nil {
				return false, err
			}
			for _, table := range []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH} {
				_, err = viewRecoveryExec(txn, fmt.Sprintf("delete from mo_catalog.%s where account_id=%d and target_relation_id=%d and not exists (select 1 from mo_catalog.mo_tables t where t.account_id=%d and t.rel_id=%d and t.relkind='v')", table, target.accountID, target.relationID, target.accountID, target.relationID))
				if err != nil {
					return false, err
				}
			}
		}
	} else {
		from := "mo_catalog.mo_tables t"
		predicate := viewRecoveryScopePredicate(s.Scope, "t.", false)
		switch work.kind {
		case "scan":
		case "roots", "node":
			// Resolve the target by account-qualified name as COPY/restore can replace
			// its physical ID while old dependency edges still point at its predecessor.
			from = "mo_catalog.mo_view_dependencies d join mo_catalog.mo_tables t on t.account_id=d.account_id and t.reldatabase=d.target_database_name and t.relname=d.target_relation_name"
			predicate = viewRecoveryScopePredicate(s.Scope, "d.", true)
			if work.kind == "node" {
				predicate = viewDependencyMutationPredicate(viewRelationMutation{accountID: work.accountID, databaseID: work.databaseID, relationID: work.relationID, logicalID: work.logicalID, databaseName: work.databaseName, relationName: work.relationName}, 0, 0)
			}
		default:
			return false, moerr.NewInvalidStateNoCtx("unknown View recovery work kind")
		}
		after := fmt.Sprintf("t.account_id>%d or (t.account_id=%d and t.rel_id>%d)", work.cursorAccount, work.cursorAccount, work.cursorRelation)
		rows, err = viewRecoveryQuery(txn, fmt.Sprintf("select cast(q.account_id as char),cast(q.reldatabase_id as char),cast(q.rel_id as char),cast(q.rel_logical_id as char),q.reldatabase,q.relname from (select distinct t.account_id,t.reldatabase_id,t.rel_id,t.rel_logical_id,t.reldatabase,t.relname from %s where t.relkind='v' and t.reldatabase not in ('%s') and (%s) and (%s) order by t.account_id,t.rel_id limit 32) q order by q.account_id,q.rel_id", from, strings.Join(catalog.SystemDatabases, "','"), predicate, after), 6)
		if err != nil {
			return false, err
		}
		for _, row := range rows {
			target, err := viewRecoveryTargetRow(row)
			if err != nil {
				return false, err
			}
			if err = seedViewRecoveryTarget(txn, s, target); err != nil {
				return false, err
			}
		}
	}
	done := len(rows) < viewMetadataRecoveryPageSize
	if len(rows) > 0 {
		last, err := viewRecoveryTargetRow(rows[len(rows)-1])
		if err != nil {
			return false, err
		}
		work.cursorAccount = uint64(last.accountID)
		work.cursorRelation = last.relationID
	}
	affected, err := viewRecoveryExec(txn, fmt.Sprintf("update mo_catalog.mo_view_recovery_work set cursor_account=%d,cursor_relation=%d,visits=%d,done=%t where %s and cursor_account=%d and cursor_relation=%d and visits=%d", work.cursorAccount, work.cursorRelation, work.visits+1, done, viewRecoveryWorkKey(s.Generation, *work), oldAccount, oldRelation, work.visits))
	// The coordinator row serializes writers. A page's cursor still has a CAS so
	// corrupted/replayed work cannot silently report success.
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return true, nil
}

func refreshViewRecoveryTarget(txn executor.TxnExecutor, s *ViewRecoveryState, attempted *viewRefreshTarget) (bool, error) {
	rows, err := viewRecoveryQuery(txn, fmt.Sprintf("select cast(r.account_id as char),cast(r.target_relation_id as char),cast(r.target_generation as char) from mo_catalog.mo_view_refresh r join mo_catalog.mo_view_recovery_work w on w.account_id=r.account_id and w.relation_id=r.target_relation_id where w.generation=%d and w.kind='node' and ((r.status in ('PENDING','DISCOVERING') and (r.next_retry_at is null or r.next_retry_at<=now())) or (r.status='RUNNING' and r.lease_expires_at<=now())) order by r.next_retry_at,r.attempts,r.account_id,r.target_relation_id limit 1", s.Generation), 3)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	account, err := viewRecoveryUint(rows[0][0])
	if err != nil || account > math.MaxUint32 {
		return false, moerr.NewInvalidStateNoCtx("invalid View recovery account")
	}
	relation, err := viewRecoveryUint(rows[0][1])
	if err != nil {
		return false, err
	}
	generation, err := viewRecoveryUint(rows[0][2])
	if err != nil {
		return false, err
	}
	*attempted = viewRefreshTarget{accountID: uint32(account), relationID: relation, generation: generation}
	request, _ := json.Marshal(viewMetadataRecoveryCommand{WorkerID: s.Owner, AccountID: uint32(account), RelationID: relation, Generation: generation})
	result, err := txn.Exec(fmt.Sprintf("select mo_ctl('CN','RefreshViewMetadata','%s')", sqlquote.EscapeString(string(request))), executor.StatementOption{})
	if err != nil {
		return false, err
	}
	response, err := viewRecoveryStrings(result, 1)
	if err != nil {
		return false, err
	}
	if len(response) != 1 {
		return false, moerr.NewInvalidStateNoCtx("missing View recovery command response")
	}
	var status struct {
		Result int `json:"result"`
	}
	if err = json.Unmarshal([]byte(response[0][0]), &status); err != nil {
		return false, err
	}
	if status.Result != 1 {
		return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return true, nil
}

func viewRecoveryHasPending(txn executor.TxnExecutor, generation uint64) (bool, error) {
	rows, err := viewRecoveryQuery(txn, fmt.Sprintf("select kind from mo_catalog.mo_view_recovery_work where generation=%d and done=false limit 1", generation), 1)
	if err != nil || len(rows) > 0 {
		return len(rows) > 0, err
	}
	rows, err = viewRecoveryQuery(txn, fmt.Sprintf("select w.kind from mo_catalog.mo_view_recovery_work w join mo_catalog.mo_tables t on t.account_id=w.account_id and t.rel_id=w.relation_id and t.relkind='v' left join mo_catalog.mo_view_refresh r on r.account_id=w.account_id and r.target_relation_id=w.relation_id where w.generation=%d and w.kind='node' and (r.target_relation_id is null or r.status not in ('CURRENT','INVALID') or (r.status='CURRENT' and r.completed_generation<>r.target_generation)) limit 1", generation), 1)
	return len(rows) > 0, err
}
