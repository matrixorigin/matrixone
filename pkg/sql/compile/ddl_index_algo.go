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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func (s *Scope) handleUniqueIndexTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	err := genNewUniqueIndexDuplicateCheck(c, qryDatabase, originalTableDef.Name, partsToColsStr(indexDef.Parts))
	if err != nil {
		return err
	}

	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) handleRegularSecondaryIndexTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	return s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
}

func (s *Scope) createAndInsertForUniqueOrRegularIndexTable(c *Compile, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	createSQL := genCreateIndexTableSql(def, indexDef, qryDatabase)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	insertSQL := genInsertIndexTableSql(originalTableDef, indexDef, qryDatabase, indexDef.Unique)
	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}
	return nil
}
func (s *Scope) handleMasterIndexTable(c *Compile, indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	createSQL := genCreateIndexTableSql(def, indexDef, qryDatabase)
	err := c.runSql(createSQL)
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

func (s *Scope) handleIndexColCount(c *Compile, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef) (int64, error) {

	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(`%s`) from `%s`.`%s`;",
		indexColumnName,
		qryDatabase,
		originalTableDef.Name)
	rs, err := c.runSqlWithResult(countTotalSql)
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

	// 1.a algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	centroidParamsLists, err := strconv.Atoi(params[catalog.IndexAlgoParamLists])
	if err != nil {
		return err
	}
	centroidParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])
	kmeansInitType := "kmeansplusplus"
	kmeansNormalize := "true"

	// 1.b init centroids table with default centroid, if centroids are not enough.
	// NOTE: we can run re-index to improve the centroid quality.
	if totalCnt == 0 || totalCnt < int64(centroidParamsLists) {
		initSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`) VALUES(0,1,NULL);",
			qryDatabase,
			indexDef.IndexTableName,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		)
		err := c.runSql(initSQL)
		if err != nil {
			return err
		}
		return nil
	}

	// 2. Sampling SQL Logic
	sampleCnt := catalog.CalcSampleCount(int64(centroidParamsLists), totalCnt)
	indexColumnName := indexDef.Parts[0]
	sampleSQL := fmt.Sprintf("(select sample(`%s`, %d rows, 'row') as `%s` from `%s`.`%s`)",
		indexColumnName,
		sampleCnt,
		indexColumnName,
		qryDatabase,
		originalTableDef.Name,
	)

	// 3. Insert into centroids table
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`)",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid)

	/*
		Sample SQL:

		SELECT
		(SELECT CAST(`value` AS BIGINT) FROM meta WHERE `key` = 'version'),
		ROW_NUMBER() OVER(),
		cast(`__mo_index_unnest_cols`.`value` as VARCHAR)
		FROM
		(SELECT cluster_centers(`embedding` kmeans '2,vector_l2_ops') AS `__mo_index_centroids_string` FROM (select sample(embedding, 10 rows) as embedding from tbl) ) AS `__mo_index_centroids_tbl`
		CROSS JOIN
		UNNEST(`__mo_index_centroids_tbl`.`__mo_index_centroids_string`) AS `__mo_index_unnest_cols`;
	*/
	// 4. final SQL
	clusterCentersSQL := fmt.Sprintf("%s "+
		"SELECT "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s` WHERE `%s` = 'version'), "+
		"ROW_NUMBER() OVER(), "+
		"cast(`__mo_index_unnest_cols`.`value` as VARCHAR) "+
		"FROM "+
		"(SELECT cluster_centers(`%s` kmeans '%d,%s,%s,%s') AS `__mo_index_centroids_string` FROM %s ) AS `__mo_index_centroids_tbl` "+
		"CROSS JOIN "+
		"UNNEST(`__mo_index_centroids_tbl`.`__mo_index_centroids_string`) AS `__mo_index_unnest_cols`;",
		insertSQL,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,

		indexColumnName,
		centroidParamsLists,
		centroidParamsDistFn,
		kmeansInitType,
		kmeansNormalize,
		sampleSQL,
	)

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "clustering_start")
	if err != nil {
		return err
	}

	err = c.runSql(clusterCentersSQL)
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

	// 1. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	algoParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamOpType_l2] = "l2_distance"
	//ops[catalog.IndexAlgoParamOpType_ip] = "inner_product" //TODO: verify this is correct @arjun
	//ops[catalog.IndexAlgoParamOpType_cos] = "cosine_distance"
	algoParamsDistFn = ops[algoParamsDistFn]

	// 2. Original table's pkey name and value
	var originalTblPkColsCommaSeperated string
	var originalTblPkColMaySerial string
	var originalTblPkColMaySerialColNameAlias = "__mo_org_tbl_pk_may_serial_col"
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

	// 3. insert into entries table
	indexColumnName := indexDef.Parts[0]
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`) ",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)

	// 4. centroids table with latest version
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

	// 5. original table with normalized SK
	originalTableWithNormalizedSkSql := fmt.Sprintf("(select %s as `%s`, "+
		"normalize_l2(`%s`.`%s`) as `%s` from `%s`.`%s`) as `%s`",
		originalTblPkColMaySerial,
		originalTblPkColMaySerialColNameAlias,

		originalTableDef.Name,
		indexColumnName,
		indexColumnName,
		qryDatabase,
		originalTableDef.Name,
		originalTableDef.Name,
	)

	/*
		Sample SQL:
		INSERT INTO `a`.`entries_tbl`(`__mo_index_centroid_fk_version`, `__mo_index_centroid_fk_id`, `__mo_index_pri_col`)
		SELECT     `centroids_tbl`.`__mo_index_centroid_version`,
		           serial_extract( min( serial( l2_distance(`centroids_tbl`.`__mo_index_centroid`,`tbl`.embedding ), `centroids_tbl`.`__mo_index_centroid_id`)), 1 AS bigint),
		           `tbl`.`__mo_org_tbl_pk_may_serial_col`
		FROM
			(SELECT `tbl`.`id` AS `__mo_org_tbl_pk_may_serial_col`, normalize_l2(`tbl`.embedding) AS `embedding` FROM `a`.`tbl`) AS `tbl`
		CROSS JOIN
			(SELECT * FROM   `a`.`centroids_tbl` WHERE  `__mo_index_centroid_version` = 0) AS `centroids_tbl`
		GROUP BY   `tbl`.`__mo_org_tbl_pk_may_serial_col`;

	*/
	// 6. final SQL
	mappingSQL := fmt.Sprintf("%s "+
		"select `%s`.`__mo_index_centroid_version`, "+
		"serial_extract( min( serial_full( %s(`%s`.`%s`, `%s`.%s), `%s`.`%s`)), 1 as bigint), "+
		"%s "+
		"from %s CROSS JOIN %s group by %s;",
		insertSQL,

		centroidsTableName,

		algoParamsDistFn,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		originalTableDef.Name,
		indexColumnName,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,

		originalTblPkColMaySerialColNameAlias, // NOTE: no need to add tableName here, because it could be serial()

		originalTableWithNormalizedSkSql,
		centroidsTableForCurrentVersionSql,
		originalTblPkColMaySerialColNameAlias,
	)

	err = s.logTimestamp(c, qryDatabase, metadataTableName, "mapping_start")
	if err != nil {
		return err
	}

	err = c.runSql(mappingSQL)
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
