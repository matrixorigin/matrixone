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
	indexDef *plan.IndexDef, qry *plan.CreateIndex,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	err := genNewUniqueIndexDuplicateCheck(c, qry.Database, originalTableDef.Name, partsToColsStr(indexDef.Parts))
	if err != nil {
		return err
	}

	err = s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qry, originalTableDef, indexInfo)
	if err != nil {
		return err
	}

	return err
}

func (s *Scope) handleRegularSecondaryIndexTable(c *Compile,
	indexDef *plan.IndexDef, qry *plan.CreateIndex,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	err := s.createAndInsertForUniqueOrRegularIndexTable(c, indexDef, qry, originalTableDef, indexInfo)
	if err != nil {
		return err
	}

	return err
}

func (s *Scope) createAndInsertForUniqueOrRegularIndexTable(c *Compile, indexDef *plan.IndexDef,
	qry *plan.CreateIndex, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	if len(indexInfo.GetIndexTables()) != 1 {
		return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
	}

	def := indexInfo.GetIndexTables()[0]
	createSQL := genCreateIndexTableSql(def, indexDef, qry.Database)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	insertSQL := genInsertIndexTableSql(originalTableDef, indexDef, qry.Database, indexDef.Unique)
	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) handleIvfIndexMetaTable(c *Compile,
	indexDef *plan.IndexDef, qry *plan.CreateIndex,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	// 0. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[0], indexDef, qry.Database)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	// 1. insert version
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` values('version', '1');", qry.Database, indexDef.IndexTableName)
	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexCentroidsTable(c *Compile,
	indexDef *plan.IndexDef, qry *plan.CreateIndex,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	// 0. type
	//colType := types.T(indexInfo.GetIndexTables()[1].GetCols()[0].Typ.Id)
	//colTypeStr := fmt.Sprintf("%s(%d)", colType.String(), indexInfo.GetIndexTables()[1].GetCols()[0].Typ.Width)

	// 0. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[1], indexDef, qry.Database)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	// 1. k and vector ops
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	lists, err := strconv.Atoi(params[catalog.IndexAlgoParamLists])
	if err != nil {
		return err
	}

	vecOps := params[catalog.IndexAlgoParamSimilarityFn]
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamSimilarityFn_ip] = "vector_ip_ops"
	ops[catalog.IndexAlgoParamSimilarityFn_l2] = "vector_l2_ops"
	ops[catalog.IndexAlgoParamSimilarityFn_cos] = "vector_cosine_ops"
	vecOps = ops[vecOps]

	// 2. Number of records in the table & number of samples required
	countTotalSql := fmt.Sprintf("select count(%s) from `%s`.`%s`;", indexDef.Parts[0], qry.Database, originalTableDef.Name)
	rs, err := c.runSqlWithResult(countTotalSql)
	if err != nil {
		return err
	}
	var totalCnt int64
	rs.ReadRows(func(cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		return true
	})

	var sampleCnt = totalCnt
	if sampleCnt > int64(50*lists) {
		sampleCnt = int64(50 * lists)
	}

	// 3. Insert into centroids table
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s, %s, %s) "+
		"SELECT cast(value as VARCHAR), ROW_NUMBER() OVER () AS row_num, 1 FROM "+
		"(SELECT cluster_centers(%s spherical_kmeans '%d,%s') AS centers FROM (select `%s` from `%s` rand limit %d)) AS subquery "+
		"CROSS JOIN UNNEST(subquery.centers) AS u;",
		qry.Database,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		//colTypeStr,
		indexDef.Parts[0],
		lists,
		vecOps,
		indexDef.Parts[0],
		originalTableDef.Name,
		sampleCnt,
	)
	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexEntriesTable(c *Compile,
	indexDef *plan.IndexDef, qry *plan.CreateIndex,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable, centroidsTableName string) error {

	// 1. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[2], indexDef, qry.Database)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	// 2. vector ops
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	vecOps := params[catalog.IndexAlgoParamSimilarityFn]
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamSimilarityFn_ip] = "inner_product"
	ops[catalog.IndexAlgoParamSimilarityFn_l2] = "l2_distance"
	ops[catalog.IndexAlgoParamSimilarityFn_cos] = "l2_distance"
	vecOps = ops[vecOps]

	// 3. Original table's pkey name and value
	pkeyName := originalTableDef.Pkey.PkeyColName
	var originalTblPk string
	if pkeyName == catalog.CPrimaryKeyColName {
		originalTblPk = "serial("
		for i, part := range originalTableDef.Pkey.Names {
			if i == 0 {
				originalTblPk += part
			} else {
				originalTblPk += "," + part
			}
		}
		originalTblPk += ")"
	} else {
		originalTblPk = pkeyName
	}

	// 3. insert into entries table
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (%s,%s,%s) "+
		"SELECT sub.centroid_id_fk, sub.centroid_version_fk, sub.table_pk FROM "+
		"(SELECT c.%s as centroid_id_fk, c.%s as centroid_version_fk, %s as table_pk, %s(c.%s, normalize_l2(t.%s)) as distance, "+
		"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s(c.%s, normalize_l2(t.%s))) as rn "+
		"FROM %s t CROSS JOIN `%s` c) sub WHERE sub.rn = 1;",
		qry.Database,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,

		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		originalTblPk,
		vecOps,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		indexDef.Parts[0],
		originalTblPk,
		vecOps,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		indexDef.Parts[0],

		originalTableDef.Name,
		centroidsTableName,
	)

	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}

	return nil
}
