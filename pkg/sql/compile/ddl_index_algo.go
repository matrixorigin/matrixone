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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	ivfFlatIndexFlag  = "experimental_ivf_index"
	fulltextIndexFlag = "experimental_fulltext_index"
	hnswIndexFlag     = "experimental_hnsw_index"
)

func (s *Scope) handleUniqueIndexTable(c *Compile, dbSource engine.Database,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	if err := indexTableBuild(c, def, dbSource); err != nil {
		return err
	}
	// the logic of detecting whether the unique constraint is violated does not need to be done separately,
	// it will be processed when inserting into the hidden table.
	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) createAndInsertForUniqueOrRegularIndexTable(c *Compile, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	insertSQL := genInsertIndexTableSql(originalTableDef, indexDef, qryDatabase, indexDef.Unique)
	err := c.runSql(insertSQL)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) handleRegularSecondaryIndexTable(c *Compile, dbSource engine.Database,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	if err := indexTableBuild(c, def, dbSource); err != nil {
		return err
	}

	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) handleMasterIndexTable(c *Compile, dbSource engine.Database,
	indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	err := indexTableBuild(c, def, dbSource)
	if err != nil {
		return err
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

func (s *Scope) handleFullTextIndexTable(c *Compile, dbSource engine.Database, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	if ok, err := s.isExperimentalEnabled(c, fulltextIndexFlag); err != nil {
		return err
	} else if !ok {
		return moerr.NewInternalErrorNoCtx("FullText index is not enabled")
	}
	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	err := indexTableBuild(c, def, dbSource)
	if err != nil {
		return err
	}

	insertSQLs := genInsertIndexTableSqlForFullTextIndex(originalTableDef, indexDef, qryDatabase)
	for _, insertSQL := range insertSQLs {
		err = c.runSql(insertSQL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) handleIndexColCount(c *Compile, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef) (int64, error) {

	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(`%s`) from `%s`.`%s`;",
		indexColumnName,
		qryDatabase,
		originalTableDef.Name)
	rs, err := c.runSqlWithResult(countTotalSql, NoAccountId)
	if err != nil {
		return 0, err
	}

	var totalCnt int64
	rs.ReadRows(func(_ int, cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		return false
	})
	rs.Close()

	return totalCnt, nil
}

func (s *Scope) handleIvfIndexMetaTable(c *Compile, indexDef *plan.IndexDef, qryDatabase string) error {

	/*
		The meta table will contain version number for now. In the future, it can contain `index progress` etc.
		The version number is incremented monotonically for each re-index.
		NOTE: We don't handle version number overflow as BIGINT has a large upper bound.

		Sample SQL:

		CREATE TABLE meta ( `key` VARCHAR(255), `value` VARCHAR(255), PRIMARY KEY (`key`));
		INSERT INTO meta (`key`, `value`) VALUES ('version', '0') ON DUPLICATE KEY UPDATE `value` = CAST( (cast(`value` AS BIGINT) + 1) AS CHAR);
	*/

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`) values('version', '0')"+
		"ON DUPLICATE KEY UPDATE `%s` = CAST( (CAST(`%s` AS BIGINT) + 1) AS CHAR);",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
	)

	err := c.runSql(insertSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexCentroidsTable(c *Compile, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, totalCnt int64, metadataTableName string) error {

	var cfg vectorindex.IndexTableConfig
	src_alias := "src"
	pkColName := src_alias + "." + originalTableDef.Pkey.PkeyColName

	cfg.MetadataTable = metadataTableName
	cfg.IndexTable = indexDef.IndexTableName
	cfg.DbName = qryDatabase
	cfg.SrcTable = originalTableDef.Name
	cfg.PKey = pkColName
	cfg.KeyPart = indexDef.Parts[0]
	cfg.DataSize = totalCnt

	// 1.a algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	centroidParamsLists, err := strconv.Atoi(params[catalog.IndexAlgoParamLists])
	if err != nil {
		return err
	}

	// 1.b init centroids table with default centroid, if centroids are not enough.
	// NOTE: we can run re-index to improve the centroid quality.
	if totalCnt == 0 || totalCnt < int64(centroidParamsLists) {
		initSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) "+
			"SELECT "+
			"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version'), "+
			"1, NULL;",
			qryDatabase,
			indexDef.IndexTableName,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,

			catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			qryDatabase,
			metadataTableName,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		)
		err := c.runSql(initSQL)
		if err != nil {
			return err
		}
		return nil
	}

	val, err := c.proc.GetResolveVariableFunc()("ivf_threads_build", true, false)
	if err != nil {
		return err
	}
	cfg.ThreadsBuild = val.(int64)

	params_str := indexDef.IndexAlgoParams

	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	part := src_alias + "." + indexDef.Parts[0]
	insertIntoIvfIndexTableFormat := "SELECT f.* from `%s`.`%s` AS %s CROSS APPLY ivf_create('%s', '%s', %s) AS f;"
	sql := fmt.Sprintf(insertIntoIvfIndexTableFormat,
		qryDatabase, originalTableDef.Name,
		src_alias,
		params_str,
		string(cfgbytes),
		part)

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "clustering_start")
	if err != nil {
		return err
	}

	err = c.runSql(sql)
	if err != nil {
		return err
	}

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "clustering_end")
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexEntriesTable(c *Compile, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef,
	metadataTableName string,
	centroidsTableName string) error {

	// 1.a algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	optype, ok := params[catalog.IndexAlgoParamOpType]
	if !ok {
		return moerr.NewInternalErrorNoCtx("vector optype not found")
	}

	// 1. Original table's pkey name and value
	var originalTblPkColsCommaSeperated string
	var originalTblPkColMaySerial string
	if originalTableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		for i, part := range originalTableDef.Pkey.Names {
			if i > 0 {
				originalTblPkColsCommaSeperated += ","
			}
			originalTblPkColsCommaSeperated += fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, part)
		}
		originalTblPkColMaySerial = fmt.Sprintf("serial(%s)", originalTblPkColsCommaSeperated)
	} else {
		originalTblPkColsCommaSeperated = fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, originalTableDef.Pkey.PkeyColName)
		originalTblPkColMaySerial = originalTblPkColsCommaSeperated
	}

	// 2. insert into entries table
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`, `%s`) ",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
	)

	// 3. centroids table with latest version
	centroidsTableForCurrentVersionSql := fmt.Sprintf("(select * from "+
		"`%s`.`%s` where `%s` = "+
		"(select CAST(%s as BIGINT) from `%s`.`%s` where `%s` = 'version'))  as `%s`",
		qryDatabase,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		centroidsTableName,
	)

	// 4. select * from table and cross join centroids;
	indexColumnName := indexDef.Parts[0]
	centroidsCrossL2JoinTbl := fmt.Sprintf("%s "+
		"SELECT `%s`, `%s`,  %s, `%s`"+
		" FROM `%s`.`%s` CENTROIDX ('%s') join %s "+
		" using (`%s`, `%s`) ",
		insertSQL,

		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		originalTblPkColMaySerial,
		indexColumnName,

		qryDatabase,
		originalTableDef.Name,
		optype,
		centroidsTableForCurrentVersionSql,

		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		indexColumnName,
	)

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "mapping_start")
	if err != nil {
		return err
	}

	err = c.runSql(centroidsCrossL2JoinTbl)
	if err != nil {
		return err
	}

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "mapping_end")
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) logTimestamp(c *Compile, qryDatabase, metadataTableName, metrics string) error {
	return c.runSql(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s, %s) "+
		" VALUES ('%s', NOW()) "+
		" ON DUPLICATE KEY UPDATE %s = NOW();",
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,

		metrics,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
	))
}

func (s *Scope) isExperimentalEnabled(c *Compile, flag string) (bool, error) {

	val, err := c.proc.GetResolveVariableFunc()(flag, true, false)
	if err != nil {
		return false, err
	}

	if val == nil {
		return false, nil
	}

	return fmt.Sprintf("%v", val) == "1", nil
}

func (s *Scope) handleIvfIndexDeleteOldEntries(c *Compile,
	metadataTableName string,
	centroidsTableName string,
	entriesTableName string,
	qryDatabase string) error {

	pruneCentroidsTbl := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` < "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version');",
		qryDatabase,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)

	pruneEntriesTbl := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` < "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version');",
		qryDatabase,
		entriesTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)

	err := s.logTimestamp(c, qryDatabase, metadataTableName, "pruning_start")
	if err != nil {
		return err
	}

	err = c.runSql(pruneCentroidsTbl)
	if err != nil {
		return err
	}

	err = c.runSql(pruneEntriesTbl)
	if err != nil {
		return err
	}

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "pruning_end")
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleVectorHnswIndex(c *Compile, dbSource engine.Database, indexDefs map[string]*plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	if ok, err := s.isExperimentalEnabled(c, hnswIndexFlag); err != nil {
		return err
	} else if !ok {
		return moerr.NewInternalErrorNoCtx("experimental_hnsw_index is not enabled")
	}

	// 1. static check
	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid hnsw index table definition")
	}
	if len(indexDefs[catalog.Hnsw_TblType_Metadata].Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid hnsw index part must be 1.")
	}

	// 2. create hidden tables
	if indexInfo != nil {
		for _, table := range indexInfo.GetIndexTables() {
			if err := indexTableBuild(c, table, dbSource); err != nil {
				return err
			}
		}
	}

	// clear the cache (it only work in standalone mode though)
	key := indexDefs[catalog.Hnsw_TblType_Storage].IndexTableName
	cache.Cache.Remove(key)

	// delete old data first
	{
		sqls, err := genDeleteHnswIndex(c.proc, indexDefs, qryDatabase, originalTableDef)
		if err != nil {
			return err
		}

		for _, sql := range sqls {
			err = c.runSql(sql)
			if err != nil {
				return err
			}
		}
	}

	// 3. build hnsw index
	sqls, err := genBuildHnswIndex(c.proc, indexDefs, qryDatabase, originalTableDef)
	if err != nil {
		return err
	}

	for _, sql := range sqls {
		err = c.runSql(sql)
		if err != nil {
			return err
		}
	}

	return nil
}
