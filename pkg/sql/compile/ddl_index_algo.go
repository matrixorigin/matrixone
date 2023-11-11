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

	// 0. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[1], indexDef, qry.Database)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

	// 1. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	centroidParamsLists, err := strconv.Atoi(params[catalog.IndexAlgoParamLists])
	if err != nil {
		return err
	}

	centroidParamsDistFn := params[catalog.IndexAlgoParamSimilarityFn]
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamSimilarityFn_ip] = "vector_ip_ops"
	ops[catalog.IndexAlgoParamSimilarityFn_l2] = "vector_l2_ops"
	ops[catalog.IndexAlgoParamSimilarityFn_cos] = "vector_cosine_ops"
	centroidParamsDistFn = ops[centroidParamsDistFn]

	// 2.1 Number of records in the table
	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(%s) from `%s`.`%s`;", indexColumnName, qry.Database, originalTableDef.Name)
	rs, err := c.runSqlWithResult(countTotalSql)
	if err != nil {
		return err
	}
	var totalCnt int64
	rs.ReadRows(func(cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		return false
	})
	rs.Close()

	// 2.2 Sampling Logic
	var sampleCnt = totalCnt
	if sampleCnt > int64(50*centroidParamsLists) {
		//NOTE: this is the sampling rule. Every list/k (ie cluster) will have 50 samples.
		// we will change it to 250 samples per list/k in the future.
		sampleCnt = int64(50 * centroidParamsLists)
	}
	sampledSQL := fmt.Sprintf("(select `%s` from `%s` rand limit %d)", indexColumnName, originalTableDef.Name, sampleCnt)

	// 3. Insert into centroids table
	/* Example SQL:-

	-- Original table
	create table tbl(id int primary key, embedding vecf32(3));
	insert into tbl values(1, "[1,2,3]");
	insert into tbl values(2, "[1,2,4]");
	insert into tbl values(3, "[1,2.4,4]");
	insert into tbl values(4, "[1,2,5]");
	insert into tbl values(5, "[1,3,5]");
	insert into tbl values(6, "[100,44,50]");
	insert into tbl values(7, "[120,50,70]");
	insert into tbl values(8, "[130,40,90]");

	-- Centroid SQL query
	create table centroids(`centroid` vecf32(3), `id` int auto_increment, `version` int);

	insert into centroids(`centroid`, `id`, `version`)
	SELECT cast(value as VARCHAR),ROW_NUMBER() OVER () AS row_num, 1 FROM
	(SELECT cluster_centers(`embedding` spherical_kmeans '2,vector_cosine_ops') AS centers FROM
	(select `embedding` from `tbl` rand limit 10)) AS subquery
	CROSS JOIN UNNEST (subquery.centers) AS u;


	+-------------------------------------+---------+------+
	| cast(value as varchar)              | row_num | 1    |
	+-------------------------------------+---------+------+
	| [0.20933901, 0.46925306, 0.8542248] |       1 |    1 |
	| [0.8140391, 0.31670254, 0.4806907]  |       2 |    1 |
	+-------------------------------------+---------+------+

	mysql> select * from mo_catalog.mo_indexes where name = "idx4";
	+------+----------+-------------+------+----------+---------+-----------------+-------------------------------------------+------------+--------+---------+-------------+------------------+---------+-----------------------------------------------------------+
	| id   | table_id | database_id | name | type     | algo    | algo_table_type | algo_params                               | is_visible | hidden | comment | column_name | ordinal_position | options | index_table_name                                          |
	+------+----------+-------------+------+----------+---------+-----------------+-------------------------------------------+------------+--------+---------+-------------+------------------+---------+-----------------------------------------------------------+
	| 2501 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | metadata        | {"lists":"2","similarity_function":"cos"} |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0b1c-80ee-11ee-9b5a-723e89f7b974 |
	| 2502 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | centroids       | {"lists":"2","similarity_function":"cos"} |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0cde-80ee-11ee-9b5a-723e89f7b974 |
	| 2503 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | entries         | {"lists":"2","similarity_function":"cos"} |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0cfc-80ee-11ee-9b5a-723e89f7b974 |
	+------+----------+-------------+------+----------+---------+-----------------+-------------------------------------------+------------+--------+---------+-------------+------------------+---------+-----------------------------------------------------------+

	mysql> select * from `__mo_index_secondary_346d0cde-80ee-11ee-9b5a-723e89f7b974`;
	+-------------------------------------+-------------+---------+
	| centroid                            | centroid_id | version |
	+-------------------------------------+-------------+---------+
	| [0.20933901, 0.46925306, 0.8542248] |           1 |       1 |
	| [0.8140391, 0.31670254, 0.4806907]  |           2 |       1 |
	+-------------------------------------+-------------+---------+

	*/

	a := catalog.SystemSI_IVFFLAT_TblCol_Centroids_version
	fmt.Printf(a)

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`)",
		qry.Database,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version)

	clusterCentersSQL := fmt.Sprintf("%s SELECT cast(value as VARCHAR), ROW_NUMBER() OVER () AS row_num, 1 FROM "+
		"(SELECT cluster_centers(`%s` spherical_kmeans '%d,%s') AS centers FROM %s) AS subquery "+
		"CROSS JOIN UNNEST(subquery.centers) AS u;",
		insertSQL,
		indexColumnName,
		centroidParamsLists,
		centroidParamsDistFn,
		sampledSQL,
	)
	err = c.runSql(clusterCentersSQL)
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

	// 2. algo params
	params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	algoParamsDistFn := params[catalog.IndexAlgoParamSimilarityFn]
	ops := make(map[string]string)
	ops[catalog.IndexAlgoParamSimilarityFn_ip] = "inner_product"
	ops[catalog.IndexAlgoParamSimilarityFn_l2] = "l2_distance"
	ops[catalog.IndexAlgoParamSimilarityFn_cos] = "l2_distance"
	algoParamsDistFn = ops[algoParamsDistFn]

	// 3. Original table's pkey name and value
	_pkeyName := originalTableDef.Pkey.PkeyColName
	var originalTblPkCols string
	if _pkeyName == catalog.CPrimaryKeyColName {
		originalTblPkCols = "serial("
		for i, part := range originalTableDef.Pkey.Names {
			if i > 0 {
				originalTblPkCols += ","
			}
			originalTblPkCols += fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, part)
		}
		originalTblPkCols += ")"
	} else {
		originalTblPkCols = fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, _pkeyName)
	}

	// 3. insert into entries table
	/* Example SQL:-
	SELECT c.centroid_id as centroid_id_fk, c.version as centroid_version_fk, id as table_pk,
	l2_distance(c.centroid, tbl.embedding) as distance, c.centroid, tbl.embedding, normalize_l2(tbl.embedding),
	ROW_NUMBER() OVER (PARTITION BY id ORDER BY l2_distance(c.centroid, normalize_l2(tbl.embedding) ) ) as rn
	FROM tbl CROSS JOIN `__mo_index_secondary_centroids` c;

	*/
	indexColumnName := indexDef.Parts[0]

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (%s, %s, %s) ",
		qry.Database,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)

	mappingSQL := fmt.Sprintf("%s SELECT sub.centroid_id_fk, sub.centroid_version_fk, sub.table_pk FROM "+
		"(SELECT c.`%s` as centroid_id_fk, c.`%s` as centroid_version_fk, %s as table_pk, "+
		"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s(c.%s, normalize_l2(%s.%s))) as rn "+
		"FROM %s CROSS JOIN `%s` c) sub WHERE sub.rn = 1;",
		insertSQL,

		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		originalTblPkCols,

		originalTblPkCols,
		algoParamsDistFn,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		originalTableDef.Name,
		indexColumnName,

		originalTableDef.Name,
		centroidsTableName,
	)

	err = c.runSql(mappingSQL)
	if err != nil {
		return err
	}

	return nil
}
