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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"strconv"
)

func (s *Scope) handleUniqueIndexTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	err := genNewUniqueIndexDuplicateCheck(c, qryDatabase, originalTableDef.Name, partsToColsStr(indexDef.Parts))
	if err != nil {
		return err
	}

	err = s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
	if err != nil {
		return err
	}

	return err
}

func (s *Scope) handleRegularSecondaryIndexTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	err := s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qryDatabase, originalTableDef, indexInfo)
	if err != nil {
		return err
	}

	return err
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

func (s *Scope) handleIvfIndexMetaTable(c executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string) error {

	/*
		CREATE TABLE meta ( `key` VARCHAR(255), `value` VARCHAR(255), PRIMARY KEY (`key`));

		INSERT INTO meta (`key`, `value`) VALUES ('version', '1') ON DUPLICATE KEY UPDATE `value` = CAST( (CAST(`value` AS UNSIGNED) + 1)%10 AS CHAR);
	*/

	maxVersions := 3
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`) values('version', '0')"+
		"ON DUPLICATE KEY UPDATE `%s` = CAST( (CAST(`%s` AS UNSIGNED) + 1)%% %d  AS CHAR);",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		maxVersions,
	)

	_, err := c.Exec(insertSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexDeleteOldEntries(c executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef,
	metadataTableName string,
	centroidsTableName string,
	entriesTableName string) error {

	deleteCentroidsSQL := fmt.Sprintf("	delete from `%s`.`%s` where `%s` = (select CAST(%s as BIGINT) from `%s`.`%s` where `%s` = 'version') ",
		qryDatabase,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)
	_, err := c.Exec(deleteCentroidsSQL)
	if err != nil {
		return err
	}

	deleteEntriesSQL := fmt.Sprintf("delete from `%s`.`%s` where `%s` = (select CAST(%s as BIGINT) from `%s`.`%s` where `%s` = 'version') ",
		qryDatabase,
		entriesTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)
	_, err = c.Exec(deleteEntriesSQL)
	if err != nil {
		return err
	}
	return nil

}

func (s *Scope) handleIvfIndexCentroidsTable(c executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef, metaTableName string) error {

	// 1. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	centroidParamsLists, err := strconv.Atoi(params[catalog.IndexAlgoParamLists])
	if err != nil {
		return err
	}

	centroidParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])

	// 2.a Number of records in the table
	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(%s) from `%s`.`%s`;", indexColumnName, qryDatabase, originalTableDef.Name)
	rs, err := c.Exec(countTotalSql)
	if err != nil {
		return err
	}
	var totalCnt int64
	rs.ReadRows(func(cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		return false
	})
	rs.Close()

	// 2.b Sampling Logic
	var sampleCnt = totalCnt
	if sampleCnt > int64(50*centroidParamsLists) {
		//NOTE: this is the sampling rule. Every list/k (ie cluster) will have 50 samples.
		// we will change it to 250 samples per list/k in the future.
		sampleCnt = int64(50 * centroidParamsLists)
	}
	sampledSQL := fmt.Sprintf("(select `%s` from `%s` order by rand() limit %d)", indexColumnName, originalTableDef.Name, sampleCnt)

	// 3. Insert into centroids table
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`)",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid)

	/*
		SELECT (SELECT CAST(`value` AS UNSIGNED) FROM meta WHERE `key` = 'version'), ROW_NUMBER() OVER (), cast(`__mo_index_unnest_cols`.`value` as VARCHAR) FROM
		(SELECT cluster_centers(`embedding` spherical_kmeans '2,vector_l2_ops') AS `__mo_index_centroids_string` FROM `tbl`) AS `__mo_index_centroids_tbl`
		CROSS JOIN
		UNNEST(`__mo_index_centroids_tbl`.`__mo_index_centroids_string`) AS `__mo_index_unnest_cols`;
	*/

	clusterCentersSQL := fmt.Sprintf("%s "+
		"SELECT "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s` WHERE `%s` = 'version'), "+
		"ROW_NUMBER() OVER(), "+
		"cast(`__mo_index_unnest_cols`.`value` as VARCHAR) "+
		"FROM "+
		"(SELECT cluster_centers(`%s` spherical_kmeans '%d,%s') AS `__mo_index_centroids_string` FROM %s ) AS `__mo_index_centroids_tbl` "+
		"CROSS JOIN "+
		"UNNEST(`__mo_index_centroids_tbl`.`__mo_index_centroids_string`) AS `__mo_index_unnest_cols`;",
		insertSQL,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		metaTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,

		indexColumnName,
		centroidParamsLists,
		centroidParamsDistFn,
		sampledSQL,
	)
	_, err = c.Exec(clusterCentersSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexEntriesTable(c executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef,
	metadataTableName string,
	centroidsTableName string) error {

	// 2. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	algoParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamOpType_ip] = "inner_product"
	ops[catalog.IndexAlgoParamOpType_l2] = "l2_distance"
	ops[catalog.IndexAlgoParamOpType_cos] = "cosine_distance"
	algoParamsDistFn = ops[algoParamsDistFn]

	// 3. Original table's pkey name and value
	var originalTblPkCols string
	if originalTableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		originalTblPkCols = "serial("
		for i, part := range originalTableDef.Pkey.Names {
			if i > 0 {
				originalTblPkCols += ","
			}
			originalTblPkCols += fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, part)
		}
		originalTblPkCols += ")"
	} else {
		originalTblPkCols = fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, originalTableDef.Pkey.PkeyColName)
	}

	// 4. insert into entries table
	indexColumnName := indexDef.Parts[0]
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (%s, %s, %s) ",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)

	// 4.a centroids table with latest version
	filteredCentroidsTableSql := fmt.Sprintf("(select * from "+
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

	mappingSQL := fmt.Sprintf("%s "+
		"SELECT `__mo_index_entries_tbl`.`__mo_index_centroid_version_fk`, `__mo_index_entries_tbl`.`__mo_index_centroid_id_fk`, `__mo_index_entries_tbl`.`__mo_index_table_pk` FROM "+
		"("+
		"SELECT "+
		"`%s`.`%s` as `__mo_index_centroid_version_fk`,  "+
		"`%s`.`%s` as `__mo_index_centroid_id_fk`, "+
		"%s as `__mo_index_table_pk`, "+
		"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s(`%s`.`%s`, normalize_l2(%s.%s))) as `__mo_index_rn` "+
		"FROM "+
		" `%s` CROSS JOIN %s "+
		") `__mo_index_entries_tbl` WHERE `__mo_index_entries_tbl`.`__mo_index_rn` = 1;",
		insertSQL,

		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		// NOTE: no need to add tableName here, because it could be serial()
		originalTblPkCols,

		originalTblPkCols,
		algoParamsDistFn,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		originalTableDef.Name,
		indexColumnName,

		originalTableDef.Name,
		filteredCentroidsTableSql,
	)

	_, err = c.Exec(mappingSQL)
	if err != nil {
		return err
	}

	return nil
}
