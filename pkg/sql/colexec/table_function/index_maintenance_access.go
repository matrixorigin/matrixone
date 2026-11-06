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

package table_function

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type maintenanceOperand = vectorindex.MaintenanceOperand

// admitVectorIndexMaintenance keeps vector operators' config and relation
// checks identical while each operator owns its lifecycle and cleanup.
func admitVectorIndexMaintenance(proc *process.Process, tf *TableFunction, function, config, previous string, initialized bool,
	operands func(vectorindex.IndexTableConfig) []maintenanceOperand,
) (vectorindex.IndexTableConfig, *executor.IndexRelationPolicy, error) {
	var cfg vectorindex.IndexTableConfig
	if config == "" {
		return cfg, nil, moerr.NewInvalidInput(proc.Ctx, "IndexTableConfig is empty")
	}
	if initialized && config != previous {
		return cfg, nil, moerr.NewInvalidInput(proc.Ctx, function+": config changed during operator reuse")
	}
	if err := sonic.Unmarshal([]byte(config), &cfg); err != nil {
		return cfg, nil, err
	}
	if cfg.SrcTable == "" {
		return cfg, nil, moerr.NewInvalidInput(proc.Ctx, function+": source is empty")
	}
	policy, err := prepareIndexMaintenance(proc, tf, function, cfg.DbName, cfg.SrcTable, operands(cfg)...)
	return cfg, policy, err
}

func prepareIndexMaintenance(proc *process.Process, tf *TableFunction, function, database, source string, operands ...maintenanceOperand) (*executor.IndexRelationPolicy, error) {
	policy, views, err := admitIndexMaintenance(proc, tf, function, database, source, operands...)
	if err != nil {
		return nil, err
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.IndexRelationPolicy = policy
	for _, view := range views {
		sql := fmt.Sprintf("SELECT * FROM %s", sqlquote.QualifiedIdent(database, view))
		if err := sqlexec.BindIndexMaintenanceView(sqlproc, sql); err != nil {
			return nil, err
		}
	}
	return policy, nil
}

// admitIndexMaintenance resolves each consumed relation before any effect.
// Views need recursive, no-evaluation binding after all direct checks pass.
func admitIndexMaintenance(proc *process.Process, tf *TableFunction, function, database, source string, operands ...maintenanceOperand) (*executor.IndexRelationPolicy, []string, error) {
	if proc == nil || proc.Ctx == nil || proc.Base == nil || tf == nil || database == "" {
		return nil, nil, moerr.NewInternalErrorNoCtx("index maintenance identity or database is unavailable")
	}
	eng, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || eng == nil || proc.GetTxnOperator() == nil {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance engine or transaction is unavailable")
	}
	account, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return nil, nil, err
	}
	info := proc.GetSessionInfo()
	policy := &executor.IndexRelationPolicy{CallerMayAccess: info.User != "" && strings.EqualFold(info.Account, "sys") && strings.EqualFold(info.Role, "moadmin")}
	// A user call must not inherit its parent's internal access capability.
	userProc := &process.Process{Base: proc.Base, Ctx: process.WithUserIndexRelationAccess(proc.Ctx)}
	db, err := eng.Database(proc.Ctx, database, proc.GetTxnOperator())
	if err != nil {
		return nil, nil, err
	}
	if db == nil {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance database is unavailable")
	}
	spec := tf.IndexMaintenance
	if spec != nil {
		if err := spec.Validate(); err != nil {
			return nil, nil, err
		}
		dbID, parseErr := strconv.ParseUint(db.GetDatabaseId(proc.Ctx), 10, 64)
		if parseErr != nil || dbID != spec.DatabaseID || spec.AccountID != account || !strings.EqualFold(spec.Database, database) || !strings.EqualFold(spec.Function, function) || len(spec.Targets) != len(operands) {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance provenance does not match this operation")
		}
		base, err := db.Relation(proc.Ctx, spec.BaseName, nil)
		if err != nil {
			return nil, nil, err
		}
		if base == nil || base.GetTableID(proc.Ctx) != spec.BaseID || base.GetTableDef(proc.Ctx) == nil || !strings.EqualFold(base.GetTableName(), spec.BaseName) || (source != "" && !strings.EqualFold(source, spec.BaseName)) {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance base identity changed")
		}
		policy.Spec = spec.Clone()
	}
	var views []string
	if source != "" {
		if err = process.CheckIndexRelationAccess(userProc, source, "", 0); err != nil {
			return nil, nil, err
		}
		rel, err := db.Relation(proc.Ctx, source, nil)
		if err != nil {
			return nil, nil, err
		}
		if rel == nil {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance source is unavailable")
		}
		def := rel.GetTableDef(proc.Ctx)
		if err = process.CheckIndexRelationDefinitionAccess(userProc, rel.GetTableName(), def); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(def.TableType, catalog.SystemViewRel) {
			views = append(views, source)
		}
	}
	seen := make(map[string]bool, len(operands))
	for _, operand := range operands {
		if operand.Role == "" || operand.Name == "" || seen[operand.Role] {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "invalid index maintenance operand")
		}
		seen[operand.Role] = true
		var target *executor.IndexMaintenanceTarget
		if spec != nil {
			for i := range spec.Targets {
				if spec.Targets[i].Role == operand.Role {
					target = &spec.Targets[i]
					break
				}
			}
			if target == nil || !strings.EqualFold(target.Name, operand.Name) {
				return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance target does not match provenance")
			}
		} else if err = process.CheckIndexRelationAccess(userProc, operand.Name, "", 0); err != nil {
			return nil, nil, err
		}
		rel, err := db.Relation(proc.Ctx, operand.Name, nil)
		if err != nil {
			return nil, nil, err
		}
		if rel == nil {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance relation is unavailable")
		}
		def := rel.GetTableDef(proc.Ctx)
		accessProc := userProc
		if target != nil {
			if def == nil || !catalog.RequiresPrivilegedRelationAccess(rel.GetTableName(), def.TableType, def.FeatureFlag) {
				return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance target definition changed")
			}
			if rel.GetTableID(proc.Ctx) != target.ID || !strings.EqualFold(rel.GetTableName(), target.Name) {
				return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance target identity changed")
			}
			if extra := rel.GetExtraInfo(); extra != nil && extra.ParentTableID != 0 && extra.ParentTableID != spec.BaseID {
				return nil, nil, moerr.NewInternalError(proc.Ctx, "index maintenance target parent changed")
			}
			accessProc = &process.Process{Base: proc.Base, Ctx: process.WithTrustedIndexRelationAccess(proc.Ctx)}
		}
		if err = process.CheckIndexRelationDefinitionAccess(accessProc, rel.GetTableName(), def); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(def.TableType, catalog.SystemViewRel) {
			views = append(views, operand.Name)
		}
	}
	return policy, views, nil
}
