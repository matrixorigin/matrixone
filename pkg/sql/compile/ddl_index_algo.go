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

func (s *Scope) handleCount(txn executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef) (int64, error) {

	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(`%s`), count(`%s`) from `%s`.`%s`;",
		indexColumnName,
		originalTableDef.Pkey.PkeyColName,
		qryDatabase,
		originalTableDef.Name)
	rs, err := txn.Exec(countTotalSql)
	if err != nil {
		return 0, err
	}

	var totalCnt int64
	var pkeyCnt int64
	rs.ReadRows(func(cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		pkeyCnt = executor.GetFixedRows[int64](cols[1])[0]
		return false
	})
	rs.Close()

	if totalCnt != pkeyCnt {
		return 0, moerr.NewInvalidInputNoCtx("vecfxx column contains nulls.")
	}

	return totalCnt, nil
}

func (s *Scope) handleIvfIndexMetaTable(c executor.TxnExecutor, indexDef *plan.IndexDef, qryDatabase string) error {

	/*
		The meta table will contain version number for now. In the future, it can contain `index progress` etc.
		The version number is rotated and reused after maxVersions. This is to avoid version number overflow.
		If maxVersion = 3, then versions will be 0,1,2,0,1,2,0,1,2,0,1,2,0,1,2 etc.

		We delete old entries from centroids and entries table for the current version number. This is to ensure that
		1. we don't keep old/stale entries in the table
		2. we don't delete entries that are being used by a query.

		- If a query is referencing current version (say 1), then during reindex, we will increment current version to 2 (inside the txn),
		and then delete old entries of version 2.Then we insert new data for version 2. We commit the txn if everything succeeded.
		- When a new reindex is issued again, we will delete old entries of version 0 ( (2+1)%3 ), and insert new data for version 0.
		- When a new reindex is issued again, we reach back to version 1.
		If the query is still referencing version 1, then bad luck! We will get into an inconsistent state.
		To ensure that we don't get in this state, we need to have maxVersions based reindex frequency and avg query duration etc.
		For now, lets keep it as 10.

		Sample SQL:

		CREATE TABLE meta ( `key` VARCHAR(255), `value` VARCHAR(255), PRIMARY KEY (`key`));
		INSERT INTO meta (`key`, `value`) VALUES ('version', '0') ON DUPLICATE KEY UPDATE `value` = CAST( (cast(`value` AS BIGINT) + 1)%10 AS CHAR);
	*/

	maxVersions := 10
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`) values('version', '0')"+
		"ON DUPLICATE KEY UPDATE `%s` = CAST( (CAST(`%s` AS BIGINT) + 1)%% %d  AS CHAR);",
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

	/*
		Sample SQL:
		delete from a.centroids where version = (select CAST(`value` as BIGINT) from a.meta where `key` = 'version');
		delete from a.entries   where version = (select CAST(`value` as BIGINT) from a.meta where `key` = 'version');
	*/

	deleteCentroidsSQL := fmt.Sprintf("delete from `%s`.`%s` where `%s` = (select CAST(%s as BIGINT) from `%s`.`%s` where `%s` = 'version') ",
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

func (s *Scope) handleIvfIndexCentroidsTable(c executor.TxnExecutor, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef, totalCnt int64, metaTableName string) error {

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
	kmeansInitType := "kmeansplusplus"

	// 2. Sampling SQL Logic
	var sampleCnt = totalCnt
	entriesPerList := 50
	if sampleCnt > int64(entriesPerList*centroidParamsLists) {
		//NOTE: this is the sampling rule. Every list (ie cluster) will have 50 samples.
		// we will change it to 250 samples per list in the future.
		sampleCnt = int64(entriesPerList * centroidParamsLists)
	}
	indexColumnName := indexDef.Parts[0]
	sampleSQL := fmt.Sprintf("(select `%s` from `%s`.`%s` where `%s` is not null order by rand() limit %d)",
		indexColumnName,
		qryDatabase,
		originalTableDef.Name,
		indexColumnName,
		sampleCnt)

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
		ROW_NUMBER() OVER (),
		cast(`__mo_index_unnest_cols`.`value` as VARCHAR)
		FROM
		(SELECT cluster_centers(`embedding` spherical_kmeans '2,vector_l2_ops') AS `__mo_index_centroids_string` FROM `tbl`) AS `__mo_index_centroids_tbl`
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
		"(SELECT cluster_centers(`%s` spherical_kmeans '%d,%s,%s') AS `__mo_index_centroids_string` FROM %s ) AS `__mo_index_centroids_tbl` "+
		"CROSS JOIN "+
		"UNNEST(`__mo_index_centroids_tbl`.`__mo_index_centroids_string`) AS `__mo_index_unnest_cols`;",
		insertSQL,

		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		metaTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,

		indexColumnName,
		centroidParamsLists,
		centroidParamsDistFn,
		kmeansInitType,
		sampleSQL,
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

	// 1. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	algoParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamOpType_l2] = "l2_distance"
	ops[catalog.IndexAlgoParamOpType_ip] = "inner_product" //TODO: verify this is correct @arjun
	ops[catalog.IndexAlgoParamOpType_cos] = "cosine_distance"
	algoParamsDistFn = ops[algoParamsDistFn]

	// 2. Original table's pkey name and value
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

	// 5. non-null original table rows
	nonNullOriginalTableRowsSql := fmt.Sprintf("(select %s, %s from `%s`.`%s` where `%s` is not null) `%s`",
		originalTblPkCols,
		indexColumnName,
		qryDatabase,
		originalTableDef.Name,
		indexColumnName,
		originalTableDef.Name,
	)

	/*
		Sample SQL:
		SELECT `__mo_index_entries_tbl`.`__mo_index_centroid_version_fk`,
		`__mo_index_entries_tbl`.`__mo_index_centroid_id_fk`,
		`__mo_index_entries_tbl`.`__mo_index_table_pk` FROM
		(
		SELECT
		centroids.version as __mo_index_centroid_version_fk,
		centroids.id as __mo_index_centroid_id_fk,
		tbl.id as __mo_index_table_pk,
		ROW_NUMBER() OVER (PARTITION BY tbl.id ORDER BY l2_distance(centroids.centroid, normalize_l2(tbl.embedding)) ) as `__mo_index_rn`
		FROM
		tbl CROSS JOIN centroids
		)`__mo_index_entries_tbl` WHERE `__mo_index_entries_tbl`.`__mo_index_rn` = 1;
	*/
	// 5. final SQL
	mappingSQL := fmt.Sprintf("%s "+
		"SELECT `__mo_index_entries_tbl`.`__mo_index_centroid_version_fk`, "+
		"`__mo_index_entries_tbl`.`__mo_index_centroid_id_fk`, "+
		"`__mo_index_entries_tbl`.`__mo_index_table_pk` FROM "+
		"("+
		"SELECT "+
		"`%s`.`%s` as `__mo_index_centroid_version_fk`,  "+
		"`%s`.`%s` as `__mo_index_centroid_id_fk`, "+
		"%s as `__mo_index_table_pk`, "+
		"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s(`%s`.`%s`, normalize_l2(%s.%s))) as `__mo_index_rn` "+
		"FROM "+
		" %s CROSS JOIN %s "+
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

		nonNullOriginalTableRowsSql,
		centroidsTableForCurrentVersionSql,
	)

	_, err = c.Exec(mappingSQL)
	if err != nil {
		return err
	}

	return nil
}
