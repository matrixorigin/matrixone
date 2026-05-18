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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	hnswIndexFlag  = "experimental_hnsw_index"
	cagraIndexFlag = "experimental_cagra_index"
	ivfpqIndexFlag = "experimental_ivfpq_index"
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
	if s.Magic == TableClone {
		skipFlags := []string{
			hnswIndexFlag,
			cagraIndexFlag,
			ivfpqIndexFlag,
		}

		// if the scope is a table clone means we are trying to
		// clone a table that has an experimental index type,
		// if the source table (we want clone) exists already, we can just skip the flag check.
		// (the source table existence check has done before this check, so skip at here is fine)
		if slices.Index(skipFlags, flag) != -1 {
			return true, nil
		}
	}

	val, err := c.proc.GetResolveVariableFunc()(flag, true, false)
	if err != nil {
		return false, err
	}

	if val == nil {
		return false, nil
	}

	return fmt.Sprintf("%v", val) == "1", nil
}
