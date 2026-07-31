// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler *versionHandle

func init() {
	Handler = &versionHandle{metadata: versions.Version{
		Version:           "4.0.6",
		MinUpgradeVersion: "4.0.5",
		UpgradeCluster:    versions.Yes,
		UpgradeTenant:     versions.Yes,
		VersionOffset:     uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
	}}
}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	if err := upgradeLegacyForeignKeyMetadata(ctx, tenantID, txn); err != nil {
		return err
	}

	for _, entry := range tenantUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(tenantID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error",
				zap.Error(err), zap.Int32("tenantId", tenantID), zap.String("version", v.metadata.Version), zap.String("upgrade entry", entry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete",
			zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.metadata.Version))
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade success", zap.Int32("tenantId", tenantID), zap.String("toVersion", v.metadata.Version))
	return nil
}

const legacyForeignKeyTableDefinitionsSQL = "SELECT DISTINCT fk.db_name, fk.table_name, tbl.rel_createsql " +
	"FROM mo_catalog.mo_foreign_keys fk " +
	"JOIN mo_catalog.mo_tables tbl ON tbl.reldatabase = fk.db_name AND tbl.relname = fk.table_name " +
	"WHERE fk.constraint_id = 0"

type legacyForeignKeyTableDefinition struct {
	database  string
	table     string
	createSQL string
}

// upgradeLegacyForeignKeyMetadata restores details that earlier catalog rows did
// not persist: composite column order and whether a RESTRICT action was omitted.
func upgradeLegacyForeignKeyMetadata(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	definitions, err := getLegacyForeignKeyTableDefinitions(tenantID, txn)
	if err != nil {
		return err
	}

	for _, definition := range definitions {
		updates, err := legacyForeignKeyMetadataUpdates(definition)
		if err != nil {
			return err
		}
		for _, sql := range updates {
			res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	return nil
}

func getLegacyForeignKeyTableDefinitions(tenantID int32, txn executor.TxnExecutor) ([]legacyForeignKeyTableDefinition, error) {
	res, err := txn.Exec(legacyForeignKeyTableDefinitionsSQL, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	definitions := make([]legacyForeignKeyTableDefinition, 0)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			definitions = append(definitions, legacyForeignKeyTableDefinition{
				database:  cols[0].GetStringAt(i),
				table:     cols[1].GetStringAt(i),
				createSQL: cols[2].GetStringAt(i),
			})
		}
		return true
	})
	return definitions, nil
}

func legacyForeignKeyMetadataUpdates(definition legacyForeignKeyTableDefinition) ([]string, error) {
	statements, err := mysql.Parse(context.Background(), definition.createSQL, 1)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("parse legacy foreign-key table %s.%s: %v", definition.database, definition.table, err)
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()

	if len(statements) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("legacy foreign-key table %s.%s has %d statements", definition.database, definition.table, len(statements))
	}
	createTable, ok := statements[0].(*tree.CreateTable)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("legacy foreign-key table %s.%s does not have a CREATE TABLE definition", definition.database, definition.table)
	}

	updates := make([]string, 0)
	for _, tableDef := range createTable.Defs {
		foreignKey, ok := tableDef.(*tree.ForeignKey)
		if !ok {
			continue
		}
		if foreignKey.ConstraintSymbol == "" || foreignKey.Refer == nil {
			return nil, moerr.NewInternalErrorNoCtxf("legacy foreign key in %s.%s has incomplete persisted definition", definition.database, definition.table)
		}
		for ordinal, keyPart := range foreignKey.KeyParts {
			updates = append(updates, fmt.Sprintf(
				"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = %d, on_delete = %s, on_update = %s "+
					"WHERE constraint_id = 0 AND db_name = %s AND table_name = %s AND constraint_name = %s AND column_name = %s",
				ordinal+1,
				quoteSQLStringLiteral(referenceActionName(foreignKey.Refer.OnDelete)),
				quoteSQLStringLiteral(referenceActionName(foreignKey.Refer.OnUpdate)),
				quoteSQLStringLiteral(definition.database),
				quoteSQLStringLiteral(definition.table),
				quoteSQLStringLiteral(foreignKey.ConstraintSymbol),
				quoteSQLStringLiteral(keyPart.ColName.ColName()),
			))
		}
	}
	return updates, nil
}

func referenceActionName(action tree.ReferenceOptionType) string {
	switch action {
	case tree.REFERENCE_OPTION_CASCADE:
		return "CASCADE"
	case tree.REFERENCE_OPTION_SET_NULL:
		return "SET_NULL"
	case tree.REFERENCE_OPTION_NO_ACTION:
		return "NO_ACTION"
	case tree.REFERENCE_OPTION_SET_DEFAULT:
		return "SET_DEFAULT"
	case tree.REFERENCE_OPTION_RESTRICT:
		return "RESTRICT"
	default:
		return "NO_ACTION"
	}
}

func quoteSQLStringLiteral(s string) string {
	return "'" + strings.NewReplacer(`\\`, `\\\\`, `'`, `''`).Replace(s) + "'"
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error",
				zap.Error(err), zap.String("version", v.metadata.Version), zap.String("upgrade entry", entry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.metadata.Version))
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade success", zap.String("toVersion", v.metadata.Version))
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
