// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/mv"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func materializedViewService(ctx context.Context, ses *Session) (*mv.Service, error) {
	v, ok := moruntime.ServiceRuntime(ses.GetService()).GetGlobalVariables(mv.RuntimeKey)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "materialized view service is unavailable")
	}
	return v.(*mv.Service), nil
}
func resolveMaterializedView(ctx context.Context, ses *Session, name *tree.TableName) (mv.Key, *catalog.MVDefinition, error) {
	db := string(name.SchemaName)
	if db == "" {
		db = ses.GetDatabaseName()
	}
	_, def, err := ses.GetTxnCompileCtx().Resolve(db, string(name.ObjectName), nil)
	if err != nil {
		return mv.Key{}, nil, err
	}
	if def == nil {
		return mv.Key{}, nil, moerr.NewNoSuchTable(ctx, db, string(name.ObjectName))
	}
	d, err := catalog.MaterializedViewDefinition(def)
	if err != nil {
		return mv.Key{}, nil, err
	}
	if d == nil {
		return mv.Key{}, nil, moerr.NewInvalidInput(ctx, "object is not a materialized view")
	}
	return mv.Key{Account: ses.GetAccountId(), ID: def.TblId}, d, nil
}
func authorizeMaterializedViewOwner(ctx context.Context, ses *Session, def *catalog.MVDefinition) error {
	tenant := ses.GetTenantInfo()
	if tenant == nil || (!tenant.IsAdminRole() && tenant.GetUserID() != def.OwnerUser) {
		return moerr.NewInvalidInput(ctx, "materialized view operation requires owner or administrative authority")
	}
	return nil
}
func checkMaterializedViewTransaction(ctx context.Context, ses *Session) error {
	if ses.GetTxnHandler().InMultiStmtTransactionMode() {
		return moerr.NewInvalidInput(ctx, "materialized view DDL/refresh cannot execute in an active user transaction")
	}
	return plan2.RequirePersistedProtocolVersionForAuthoring(ctx, ses.GetTxnCompileCtx().GetProcess(), defines.MORPCVersion112)
}
func handleRefreshMaterializedView(ctx context.Context, ses *Session, stmt *tree.RefreshMaterializedView) error {
	if err := checkMaterializedViewTransaction(ctx, ses); err != nil {
		return err
	}
	key, def, err := resolveMaterializedView(ctx, ses, stmt.Name)
	if err != nil {
		return err
	}
	if err := authorizeMaterializedViewOwner(ctx, ses, def); err != nil {
		return err
	}
	if _, background := defines.GetMaterializedViewTask(ctx); background {
		svc, err := materializedViewService(ctx, ses)
		if err != nil {
			return err
		}
		if err = validateMaterializedViewDefiner(ctx, ses, svc, def); err != nil {
			return err
		}
	}
	// CREATE captured every source before optimization. Authorize those native
	// sources directly so the caller's current SQL mode cannot rebind a stored
	// expression while checking permissions. Refresh separately fences IDs and
	// schema versions in its source transaction.
	priv := determinePrivilegeSetOfStatement(&tree.Select{})
	var tips privilegeTipsArray
	for _, source := range def.Sources {
		tips = append(tips, privilegeTips{typ: PrivilegeTypeSelect, objType: objectTypeTable, databaseName: source.Database, tableName: source.Table, clusterTableOperation: clusterTableSelect})
	}
	convertPrivilegeTipsToPrivilege(priv, tips)
	allowed, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
	if err != nil {
		return err
	}
	if !allowed {
		return moerr.NewInvalidInput(ctx, "materialized view source SELECT privilege is required")
	}
	svc, err := materializedViewService(ctx, ses)
	if err != nil {
		return err
	}
	expectedEpoch := uint64(0)
	if background, ok := defines.GetMaterializedViewTask(ctx); ok {
		if key.Account != background.Account || key.ID != background.Table {
			return moerr.NewInvalidInput(ctx, "obsolete materialized view task object")
		}
		expectedEpoch = background.Epoch

	}
	return svc.Refresh(ctx, key, expectedEpoch)
}
func handleDropMaterializedView(ctx context.Context, ses *Session, stmt *tree.DropMaterializedView) error {
	if err := checkMaterializedViewTransaction(ctx, ses); err != nil {
		return err
	}
	key, def, err := resolveMaterializedView(ctx, ses, stmt.Name)
	if err != nil {
		if stmt.IfExists && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil
		}
		return err
	}
	if err := authorizeMaterializedViewOwner(ctx, ses, def); err != nil {
		return err
	}
	svc, err := materializedViewService(ctx, ses)
	if err != nil {
		return err
	}
	return svc.SQL.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		if err := mv.DependencyGuard(tx, key.Account); err != nil {
			return err
		}
		st, err := mv.Load(tx, key)
		if err != nil {
			return err
		}
		writeCtx := defines.WithMaterializedViewWrite(ctx, key.Account, key.ID, tx.Txn().Txn().ID)
		r, err := svc.SQL.Exec(writeCtx, "drop table "+mv.Quote(st.Database)+"."+mv.Quote(st.Name), executor.Options{}.WithAccountID(key.Account).WithTxn(tx.Txn()))
		if err != nil {
			return err
		}
		r.Close()
		r, err = tx.Exec(fmt.Sprintf("delete from mo_catalog.mo_role_privs where obj_type='table' and obj_id=%d", key.ID), executor.StatementOption{}.WithAccountID(key.Account))
		if err != nil {
			return err
		}
		r.Close()
		for _, table := range []string{catalog.MO_MV_DEPENDENCIES, catalog.MO_MV_REFRESH_RUNS, catalog.MO_MATERIALIZED_VIEWS} {
			r, err := tx.Exec(fmt.Sprintf("delete from mo_catalog.%s where account_id=%d and mv_id=%d", table, key.Account, key.ID), executor.StatementOption{}.WithAccountID(catalog.System_Account))
			if err != nil {
				return err
			}
			r.Close()
		}
		return nil
	}, executor.Options{}.WithAccountID(key.Account).WithWaitCommittedLogApplied())
}
func handleAlterMaterializedView(ctx context.Context, ses *Session, stmt *tree.AlterMaterializedView) error {
	if err := checkMaterializedViewTransaction(ctx, ses); err != nil {
		return err
	}
	if stmt.IntervalSeconds != 0 && stmt.IntervalSeconds < 60 {
		return moerr.NewInvalidInput(ctx, "materialized view interval must be at least 60 seconds")
	}
	key, def, err := resolveMaterializedView(ctx, ses, stmt.Name)
	if err != nil {
		return err
	}
	if err := authorizeMaterializedViewOwner(ctx, ses, def); err != nil {
		return err
	}
	svc, err := materializedViewService(ctx, ses)
	if err != nil {
		return err
	}
	next := "null"
	if stmt.IntervalSeconds > 0 {
		next = fmt.Sprintf("date_add(current_timestamp(),interval %d second)", stmt.IntervalSeconds)
	}
	def.IntervalSeconds = stmt.IntervalSeconds
	definitionJSON, err := json.Marshal(def)
	if err != nil {
		return err
	}
	r, err := svc.SQL.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_materialized_views set epoch=epoch+1,definition='%s',interval_seconds=%d,next_due=%s where account_id=%d and mv_id=%d", sqlquote.EscapeString(string(definitionJSON)), stmt.IntervalSeconds, next, key.Account, key.ID), executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}
	r.Close()
	return nil
}
func handleShowMaterializedViews(ctx context.Context, ses *Session, stmt *tree.ShowMaterializedViews) error {
	svc, err := materializedViewService(ctx, ses)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("select name,database_name,epoch,populated,source_physical,source_logical,interval_seconds from mo_catalog.mo_materialized_views where account_id=%d", ses.GetAccountId())
	cols := []string{"name", "database", "definition_epoch", "populated", "source_physical", "source_logical", "interval_seconds"}
	if stmt.Name == nil && !ses.GetTenantInfo().IsAdminRole() {
		query += fmt.Sprintf(" and cast(json_extract(definition,'$.owner_user') as bigint unsigned)=%d", ses.GetTenantInfo().GetUserID())
	}
	if stmt.Name != nil {
		key, def, err := resolveMaterializedView(ctx, ses, stmt.Name)
		if err != nil {
			return err
		}
		if err := authorizeMaterializedViewOwner(ctx, ses, def); err != nil {
			return err
		}
		if stmt.Create {
			m := ses.GetMysqlResultSet()
			m.AddColumn(newSQLTaskColumn("Materialized View", defines.MYSQL_TYPE_VARCHAR))
			m.AddColumn(newSQLTaskColumn("Create Materialized View", defines.MYSQL_TYPE_VARCHAR))
			state, err := svc.State(ctx, key)
			if err != nil {
				return err
			}
			policy := "manual"
			if state.Interval > 0 {
				policy = fmt.Sprintf("every interval %d second", state.Interval)
			}
			m.AddRow([]interface{}{string(stmt.Name.ObjectName), "create materialized view " + mv.Quote(state.Database) + "." + mv.Quote(state.Name) + " refresh " + policy + " as " + def.Query})
			return nil
		}
		query = fmt.Sprintf("select run_id,status,sequence,epoch,error_message from mo_catalog.mo_mv_refresh_runs where account_id=%d and mv_id=%d order by sequence desc limit 100", key.Account, key.ID)
		cols = []string{"run_id", "status", "sequence", "definition_epoch", "error_message"}
	}
	r, err := svc.SQL.Exec(ctx, query, executor.Options{}.WithAccountID(catalog.System_Account))
	if err != nil {
		return err
	}
	defer r.Close()
	m := ses.GetMysqlResultSet()
	for _, c := range cols {
		m.AddColumn(newSQLTaskColumn(c, defines.MYSQL_TYPE_VARCHAR))
	}
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		for i := 0; i < n; i++ {
			row := make([]interface{}, len(c))
			for j, v := range c {
				switch v.GetType().Oid {
				case types.T_varchar, types.T_text:
					row[j] = v.GetStringAt(i)
				case types.T_bool:
					row[j] = strconv.FormatBool(vector.GetFixedAtWithTypeCheck[bool](v, i))
				case types.T_uint64:
					row[j] = strconv.FormatUint(vector.GetFixedAtWithTypeCheck[uint64](v, i), 10)
				case types.T_uint32:
					row[j] = strconv.FormatUint(uint64(vector.GetFixedAtWithTypeCheck[uint32](v, i)), 10)
				case types.T_int64:
					row[j] = strconv.FormatInt(vector.GetFixedAtWithTypeCheck[int64](v, i), 10)
				}
			}
			m.AddRow(row)
		}
		return true
	})
	return nil
}

// A definer task revalidates IDs and names rather than inheriting the internal
// executor's sys administrator role name. A dropped/recreated user is rejected.
func validateMaterializedViewDefiner(ctx context.Context, ses *Session, svc *mv.Service, def *catalog.MVDefinition) error {
	query := fmt.Sprintf("select u.user_name,u.status,r.role_name from mo_catalog.mo_user u join mo_catalog.mo_user_grant g on u.user_id=g.user_id join mo_catalog.mo_role r on r.role_id=g.role_id where u.user_id=%d and r.role_id=%d", def.OwnerUser, def.OwnerRole)
	result, err := svc.SQL.Exec(ctx, query, executor.Options{}.WithAccountID(ses.GetAccountId()))
	if err != nil {
		return err
	}
	defer result.Close()
	valid := false
	roleName := ""
	result.ReadRows(func(n int, c []*vector.Vector) bool {
		if n == 1 {
			valid = c[0].GetStringAt(0) == def.OwnerName && strings.EqualFold(c[1].GetStringAt(0), userStatusUnlock)
			roleName = c[2].GetStringAt(0)
		}
		return false
	})
	if !valid {
		return moerr.NewInvalidInput(ctx, "materialized view definer is unavailable or its role was revoked")
	}
	ses.GetTenantInfo().SetDefaultRole(roleName)
	ses.GetTenantInfo().SetUser(def.OwnerName)
	ses.InvalidatePrivilegeCache()
	return nil
}

func isMaterializedViewCatalogDDL(sql string) bool {
	for _, name := range []string{catalog.MO_MATERIALIZED_VIEWS, catalog.MO_MV_DEPENDENCIES, catalog.MO_MV_REFRESH_RUNS, catalog.MO_MV_ACCOUNT_STATE} {
		if strings.HasPrefix(sql, "create cluster table mo_catalog."+name+" ") {
			return true
		}
	}
	return false
}
