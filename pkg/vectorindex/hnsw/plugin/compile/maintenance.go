// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0. See the LICENSE file.

package compile

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// ValidateIndexMaintenance binds an owner-generated literal to the live group.
func (Hooks) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	if function != "hnsw_create" || len(args) < 2 {
		return moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	var cfg vectorindex.IndexTableConfig
	if err := json.Unmarshal([]byte(args[1]), &cfg); err != nil {
		return err
	}
	if cfg.SrcTable == "" {
		return moerr.NewInvalidInputNoCtx("index maintenance source is empty")
	}
	targets := map[string]string{catalog.Hnsw_TblType_Metadata: cfg.MetadataTable, catalog.Hnsw_TblType_Storage: cfg.IndexTable}
	if cfg.DictTable != "" {
		targets[catalog.Hnsw_TblType_Dict] = cfg.DictTable
	}
	if cfg.IgnoredTable != "" {
		targets[catalog.IndexIgnored_TblType] = cfg.IgnoredTable
	}
	return vectorindex.ValidateMaintenanceConfig(spec, function, cfg.DbName, cfg.SrcTable, targets)
}

func (Hooks) InitSQLMaintenanceFunction() string { return "" }

func (Hooks) IndexMaintenanceFunctions() []string { return []string{"hnsw_create"} }

func (Hooks) IndexMaintenanceRoles(function string, defs map[string]*plan.IndexDef) ([]string, error) {
	if function != "hnsw_create" {
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	roles, err := vectorindex.MaintenanceRoles(defs, catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage)
	if err != nil {
		return nil, err
	}
	if defs[catalog.Hnsw_TblType_Dict] != nil {
		roles = append(roles, catalog.Hnsw_TblType_Dict)
	}
	if defs[catalog.IndexIgnored_TblType] != nil {
		roles = append(roles, catalog.IndexIgnored_TblType)
	}
	return roles, nil
}
