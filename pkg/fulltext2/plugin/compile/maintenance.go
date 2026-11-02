// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0. See the LICENSE file.

package compile

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func (Hooks) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	var db, source, store, meta string
	switch function {
	case "fulltext2_create":
		if len(args) < 2 {
			return moerr.NewInvalidInputNoCtx("missing fulltext2 config")
		}
		var cfg fulltext2.TableConfig
		if err := json.Unmarshal([]byte(args[1]), &cfg); err != nil {
			return err
		}
		db, source, store, meta = cfg.DbName, cfg.SrcTable, cfg.IndexTable, cfg.MetadataTable
		if source == "" {
			return moerr.NewInvalidInputNoCtx("fulltext2 source is empty")
		}
	case "fulltext2_compact":
		if len(args) < 3 {
			return moerr.NewInvalidInputNoCtx("missing fulltext2 targets")
		}
		db, store, meta = args[0], args[1], args[2]
	default:
		return moerr.NewInvalidInputNoCtx("unexpected fulltext2 maintenance function")
	}
	return vectorindex.ValidateMaintenanceConfig(spec, function, db, source, map[string]string{
		catalog.FullText2Index_TblType_Storage:  store,
		catalog.FullText2Index_TblType_Metadata: meta,
	})
}
func (Hooks) InitSQLMaintenanceFunction() string { return "" }

func (Hooks) IndexMaintenanceFunctions() []string {
	return []string{"fulltext2_create", "fulltext2_compact"}
}

func (Hooks) IndexMaintenanceRoles(function string, defs map[string]*plan.IndexDef) ([]string, error) {
	if function != "fulltext2_create" && function != "fulltext2_compact" {
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	return vectorindex.MaintenanceRoles(defs, catalog.FullText2Index_TblType_Storage, catalog.FullText2Index_TblType_Metadata)
}
