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

package iscp

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/maintenance"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type initSQLMaintenance struct {
	statement int
	spec      *executor.IndexMaintenanceSpec
	validator executor.IndexMaintenanceValidator
}

func checkInitSQLSourceID(ctx context.Context, eng engine.Engine, txn client.TxnOperator, dbName, tableName string, expected uint64) error {
	if expected == 0 || dbName == "" || tableName == "" {
		return moerr.NewInternalError(ctx, "index initialization source identity is unavailable")
	}
	db, err := eng.Database(ctx, dbName, txn)
	if err != nil {
		return err
	}
	if db == nil {
		return moerr.NewInternalError(ctx, "index initialization database is unavailable")
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return err
	}
	if rel == nil || rel.GetTableID(ctx) != expected {
		return moerr.NewInternalError(ctx, "index initialization source identity changed")
	}
	return nil
}

// resolveInitSQLMaintenance recognizes only registered plugin-owned functions.
// Legacy non-TVF InitSQL keeps its existing behavior and never receives a spec.
func resolveInitSQLMaintenance(ctx context.Context, eng engine.Engine, txn client.TxnOperator, dbName, tableName, indexName string, expected []uint64, statements []string) (*initSQLMaintenance, error) {
	known := make(map[string]bool)
	for _, plugin := range indexplugin.All() {
		if d, ok := plugin.Compile().(compileplugin.IndexMaintenanceDescriptor); ok {
			for _, name := range d.IndexMaintenanceFunctions() {
				known[strings.ToLower(name)] = true
			}
		}
	}
	statement, function := -1, ""
	for i, sql := range statements {
		if strings.TrimSpace(sql) == "" {
			continue
		}
		parsed, err := parsers.Parse(ctx, dialect.MYSQL, sql, 0)
		if err != nil {
			return nil, err
		}
		if len(parsed) != 1 {
			for _, stmt := range parsed {
				stmt.Free()
			}
			return nil, moerr.NewInvalidInput(ctx, "index initialization requires one statement per entry")
		}
		names := compileplugin.IndexMaintenanceFunctionNames(parsed[0])
		parsed[0].Free()
		for _, name := range names {
			name = strings.ToLower(name)
			if !known[name] {
				continue
			}
			if statement >= 0 {
				return nil, moerr.NewInvalidInput(ctx, "multiple index maintenance functions in initialization")
			}
			statement, function = i, name
		}
	}
	if statement < 0 {
		return nil, nil
	}
	if len(expected) != 1 || expected[0] == 0 {
		return nil, moerr.NewInvalidInput(ctx, "index initialization source identity is unavailable")
	}
	if dbName == "" || tableName == "" || indexName == "" {
		return nil, moerr.NewInvalidInput(ctx, "index initialization owner is unavailable")
	}
	db, err := eng.Database(ctx, dbName, txn)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, moerr.NewInvalidInput(ctx, "index initialization database is unavailable")
	}
	base, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, moerr.NewInvalidInput(ctx, "index initialization source is unavailable")
	}
	baseID := base.GetTableID(ctx)
	if baseID == 0 || baseID != expected[0] {
		return nil, moerr.NewInvalidInput(ctx, "index initialization source identity changed")
	}
	def := base.CopyTableDef(ctx)
	if def == nil {
		return nil, moerr.NewInvalidInput(ctx, "index initialization source definition is unavailable")
	}
	group := make(map[string]*plan.IndexDef)
	var algo string
	for _, idx := range def.Indexes {
		if idx == nil || idx.IndexName != indexName {
			continue
		}
		if algo != "" && !strings.EqualFold(algo, idx.IndexAlgo) {
			return nil, moerr.NewInvalidInput(ctx, "index initialization owner is ambiguous")
		}
		algo = idx.IndexAlgo
		if group[idx.IndexAlgoTableType] != nil {
			return nil, moerr.NewInvalidInput(ctx, "duplicate index initialization role")
		}
		group[idx.IndexAlgoTableType] = idx
	}
	plugin, ok := indexplugin.Get(algo)
	if !ok {
		return nil, moerr.NewInvalidInput(ctx, "index initialization plugin is unavailable")
	}
	descriptor, ok := plugin.Compile().(compileplugin.IndexMaintenanceDescriptor)
	if !ok || !strings.EqualFold(descriptor.InitSQLMaintenanceFunction(), function) {
		return nil, moerr.NewInvalidInput(ctx, "index initialization function does not match its owner")
	}
	roles, err := descriptor.IndexMaintenanceRoles(function, group)
	if err != nil {
		return nil, err
	}
	defs := maintenance.SelectDefinitions(group, roles)
	if defs == nil {
		return nil, moerr.NewInvalidInput(ctx, "invalid index initialization role")
	}

	spec, err := compileplugin.ResolveIndexMaintenanceSpec(ctx, dbName, db, tableName, baseID, function, defs)
	if err != nil {
		return nil, err
	}
	return &initSQLMaintenance{statement: statement, spec: spec, validator: descriptor}, nil
}
