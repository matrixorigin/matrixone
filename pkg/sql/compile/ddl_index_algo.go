// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) handleUniqueIndexTable(
	c *Compile,
	mainTableID uint64,
	mainExtra *api.SchemaExtra,
	dbSource engine.Database,
	indexDef *plan.IndexDef,
	qryDatabase string,
	originalTableDef *plan.TableDef,
	indexInfo *plan.CreateTable,
) error {
	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	if err := indexTableBuild(c, mainTableID, mainExtra, def, dbSource); err != nil {
		return err
	}
	// the logic of detecting whether the unique constraint is violated does not need to be done separately,
	// it will be processed when inserting into the hidden table.
	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) createAndInsertForUniqueOrRegularIndexTable(c *Compile, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	// Skip index data population for CCPR tables when this is a CCPR task transaction.
	// The index data will be synced via CCPR data synchronization instead.
	if c.isCCPRTaskTransaction() && isTableFromPublication(originalTableDef) {
		return nil
	}
	insertSQL := genInsertIndexTableSql(originalTableDef, indexDef, qryDatabase, indexDef.Unique)
	err := c.runSql(insertSQL)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) handleRegularSecondaryIndexTable(
	c *Compile,
	mainTableID uint64,
	mainExtra *api.SchemaExtra,
	dbSource engine.Database,
	indexDef *plan.IndexDef,
	qryDatabase string,
	originalTableDef *plan.TableDef,
	indexInfo *plan.CreateTable,
) error {

	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	if err := indexTableBuild(c, mainTableID, mainExtra, def, dbSource); err != nil {
		return err
	}

	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) handleMasterIndexTable(
	c *Compile,
	mainTableID uint64,
	mainExtra *api.SchemaExtra,
	dbSource engine.Database,
	indexDef *plan.IndexDef,
	qryDatabase string,
	originalTableDef *plan.TableDef,
	indexInfo *plan.CreateTable,
) error {
	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	err := indexTableBuild(c, mainTableID, mainExtra, def, dbSource)
	if err != nil {
		return err
	}

	// Skip index data population for CCPR tables when this is a CCPR task transaction.
	// The index data will be synced via CCPR data synchronization instead.
	if c.isCCPRTaskTransaction() && isTableFromPublication(originalTableDef) {
		return nil
	}

	insertSQLs := genInsertIndexTableSqlForMasterIndex(originalTableDef, indexDef, qryDatabase)
	for _, insertSQL := range insertSQLs {
		err = c.runSql(insertSQL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) isExperimentalEnabled(c *Compile, flag string) (bool, error) {
	if s.Magic == TableClone && isPluginExperimentalFlag(flag) {
		// A table-clone scope inherits the source table's index set,
		// which was already created (and gated) when the source went
		// in. Re-checking the experimental gate at clone time would
		// reject existing tables every time the operator demotes the
		// flag back to off — surprising, and not what the legacy
		// behaviour did. Allow any plugin-declared experimental flag
		// to skip the gate at clone; non-plugin flags fall through to
		// the normal resolve.
		return true, nil
	}

	val, err := resolveVariableOrDefault(c.proc, flag, true, false)
	if err != nil {
		return false, err
	}

	if val == nil {
		return false, nil
	}

	return fmt.Sprintf("%v", val) == "1", nil
}

// isPluginExperimentalFlag reports whether flag matches any registered
// plugin's catalog.Hooks.ExperimentalFlag() value. Derives the skip set
// from the plugin registry at call time so a new plugin that declares
// an experimental flag automatically participates in the table-clone
// bypass — no manual update to this file required. Plugins that return
// "" from ExperimentalFlag() (e.g. IVF-FLAT, fulltext) are naturally
// excluded.
func isPluginExperimentalFlag(flag string) bool {
	if flag == "" {
		return false
	}
	for _, p := range indexplugin.All() {
		if p.Catalog().ExperimentalFlag() == flag {
			return true
		}
	}
	return false
}
