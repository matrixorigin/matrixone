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

func (s *Scope) handleIvfIndexMetaTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	// -1. check if table contains any data
	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(%s) from `%s`.`%s`;", indexColumnName, qryDatabase, originalTableDef.Name)
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
	if totalCnt == 0 {
		return moerr.NewInternalErrorNoCtx("table should have some data")
	}

	// 0. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[0], indexDef, qryDatabase)
	err = c.runSql(createSQL)
	if err != nil {
		return err
	}

	// 1. insert version
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` values('version', '1');", qryDatabase, indexDef.IndexTableName)
	err = c.runSql(insertSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) handleIvfIndexCentroidsTable(c *Compile,
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {

	// 0. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[1], indexDef, qryDatabase)
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

	centroidParamsDistFn := catalog.ToLower(params[catalog.IndexAlgoParamOpType])

	// 2.1 Number of records in the table
	indexColumnName := indexDef.Parts[0]
	countTotalSql := fmt.Sprintf("select count(%s) from `%s`.`%s`;", indexColumnName, qryDatabase, originalTableDef.Name)
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

	create index idx1 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";

	select * from mo_catalog.mo_indexes where name="idx1";

	-- Centroid SQL query
	create table centroids(`centroid` vecf32(3), `id` int auto_increment, `version` int);

	insert into centroids(`centroid`, `id`, `version`)
	SELECT cast(value as VARCHAR),ROW_NUMBER() OVER () AS row_num, 1 FROM
	((SELECT cluster_centers(`embedding` spherical_kmeans '2,vector_l2_ops') AS centers FROM
	(select `embedding` from `tbl` rand limit 10)) AS subquery
	CROSS JOIN UNNEST(subquery.centers) AS u) WHERE EXISTS ( select  from tbl);


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
	| 2501 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | metadata        | {"lists":"2","op_type":"vec_l2_ops"}      |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0b1c-80ee-11ee-9b5a-723e89f7b974 |
	| 2502 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | centroids       | {"lists":"2","op_type":"vec_l2_ops"}      |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0cde-80ee-11ee-9b5a-723e89f7b974 |
	| 2503 |   705779 |      705778 | idx4 | MULTIPLE | ivfflat | entries         | {"lists":"2","op_type":"vec_l2_ops"}      |          1 |      0 |         | embedding   |                1 | NULL    | __mo_index_secondary_346d0cfc-80ee-11ee-9b5a-723e89f7b974 |
	+------+----------+-------------+------+----------+---------+-----------------+-------------------------------------------+------------+--------+---------+-------------+------------------+---------+-----------------------------------------------------------+

	mysql> select * from `__mo_index_secondary_346d0cde-80ee-11ee-9b5a-723e89f7b974`;
	+-------------------------------------+-------------+---------+
	| centroid                            | centroid_id | version |
	+-------------------------------------+-------------+---------+
	| [0.20933901, 0.46925306, 0.8542248] |           1 |       1 |
	| [0.8140391, 0.31670254, 0.4806907]  |           2 |       1 |
	+-------------------------------------+-------------+---------+

	*/
	//TODO: fix null json scenario.

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`)",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid)

	clusterCentersSQL := fmt.Sprintf("%s SELECT 1, ROW_NUMBER() OVER (), cast(value as VARCHAR) FROM "+
		"(SELECT cluster_centers(`%s` spherical_kmeans '%d,%s') AS `__mo_index_centroids` FROM %s ) AS `__mo_index_subquery` "+
		"CROSS JOIN UNNEST(`__mo_index_subquery`.`__mo_index_centroids`) AS u;",
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
	indexDef *plan.IndexDef, qryDatabase string,
	originalTableDef *plan.TableDef, indexInfo *plan.CreateTable, centroidsTableName string) error {

	// 1. create table
	createSQL := genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[2], indexDef, qryDatabase)
	err := c.runSql(createSQL)
	if err != nil {
		return err
	}

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

	// 3. insert into entries table
	/* Example SQL:-
	create table entries(`id` int, `version` int, `__mo_index_pri_col` int, primary key );


	SELECT c.id as centroid_id_fk, c.version as centroid_version_fk, pk as table_pk,
	l2_distance(c.centroid, tbl.embedding) as distance, c.centroid, tbl.embedding, normalize_l2(tbl.embedding),
	ROW_NUMBER() OVER (PARTITION BY id ORDER BY l2_distance(c.centroid, normalize_l2(tbl.embedding) ) ) as rn
	FROM tbl CROSS JOIN `centroids` c;
	+----------------+---------------------+----------+--------------------+-------------------------------------+---------------+--------------------------------------+------+
	| centroid_id_fk | centroid_version_fk | table_pk | distance           | centroid                            | embedding     | normalize_l2(tbl.embedding)          | rn   |
	+----------------+---------------------+----------+--------------------+-------------------------------------+---------------+--------------------------------------+------+
	|              1 |                   1 |        3 | 3.7747623116096736 | [0.20933901, 0.46925306, 0.8542248] | [1, 2.4, 4]   | [0.20961091, 0.5030662, 0.83844364]  |    1 |
	|              1 |                   1 |        2 |   3.58667430796965 | [0.20933901, 0.46925306, 0.8542248] | [1, 2, 4]     | [0.2182179, 0.4364358, 0.8728716]    |    2 |
	|              1 |                   1 |        5 |  4.921105248104198 | [0.20933901, 0.46925306, 0.8542248] | [1, 3, 5]     | [0.16903085, 0.50709254, 0.8451542]  |    3 |
	|              1 |                   1 |        1 | 2.7518506851892677 | [0.20933901, 0.46925306, 0.8542248] | [1, 2, 3]     | [0.26726124, 0.5345225, 0.80178374]  |    4 |
	|              1 |                   1 |        4 |  4.489519238105846 | [0.20933901, 0.46925306, 0.8542248] | [1, 2, 5]     | [0.18257418, 0.36514837, 0.91287094] |    5 |
	|              1 |                   1 |        8 | 162.34304687916105 | [0.20933901, 0.46925306, 0.8542248] | [130, 40, 90] | [0.7970811, 0.24525574, 0.5518254]   |    6 |
	|              1 |                   1 |        7 | 146.91574313511364 | [0.20933901, 0.46925306, 0.8542248] | [120, 50, 70] | [0.81274253, 0.33864272, 0.4740998]  |    7 |
	|              1 |                   1 |        6 | 119.45044650420134 | [0.20933901, 0.46925306, 0.8542248] | [100, 44, 50] | [0.8322936, 0.36620918, 0.4161468]   |    8 |
	|              2 |                   1 |        7 | 146.65148375064788 | [0.8140391, 0.31670254, 0.4806907]  | [120, 50, 70] | [0.81274253, 0.33864272, 0.4740998]  |    1 |
	|              2 |                   1 |        6 |  119.1563985221276 | [0.8140391, 0.31670254, 0.4806907]  | [100, 44, 50] | [0.8322936, 0.36620918, 0.4161468]   |    2 |
	|              2 |                   1 |        8 | 162.10331066150925 | [0.8140391, 0.31670254, 0.4806907]  | [130, 40, 90] | [0.7970811, 0.24525574, 0.5518254]   |    3 |
	|              2 |                   1 |        1 |  3.035620395454993 | [0.8140391, 0.31670254, 0.4806907]  | [1, 2, 3]     | [0.26726124, 0.5345225, 0.80178374]  |    4 |
	|              2 |                   1 |        2 |  3.905586999352682 | [0.8140391, 0.31670254, 0.4806907]  | [1, 2, 4]     | [0.2182179, 0.4364358, 0.8728716]    |    5 |
	|              2 |                   1 |        3 | 4.0939282078608565 | [0.8140391, 0.31670254, 0.4806907]  | [1, 2.4, 4]   | [0.20961091, 0.5030662, 0.83844364]  |    6 |
	|              2 |                   1 |        5 |  5.259165650970188 | [0.8140391, 0.31670254, 0.4806907]  | [1, 3, 5]     | [0.16903085, 0.50709254, 0.8451542]  |    7 |
	|              2 |                   1 |        4 |  4.826202278575054 | [0.8140391, 0.31670254, 0.4806907]  | [1, 2, 5]     | [0.18257418, 0.36514837, 0.91287094] |    8 |
	+----------------+---------------------+----------+--------------------+-------------------------------------+---------------+--------------------------------------+------+

	SELECT sub.centroid_id_fk, sub.centroid_version_fk, sub.table_pk FROM
	(SELECT c.id as centroid_id_fk, c.version as centroid_version_fk, pk as table_pk,
	l2_distance(c.centroid, tbl.embedding) as distance, c.centroid, tbl.embedding, normalize_l2(tbl.embedding),
	ROW_NUMBER() OVER (PARTITION BY id ORDER BY l2_distance(c.centroid, normalize_l2(tbl.embedding) ) ) as rn
	FROM tbl CROSS JOIN `centroids` c) sub WHERE sub.rn = 1;

	*/
	indexColumnName := indexDef.Parts[0]

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (%s, %s, %s) ",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)

	mappingSQL := fmt.Sprintf("%s SELECT sub.`__mo_index_centroid_version_fk`, sub.`__mo_index_centroid_id_fk`, sub.`__mo_index_table_pk` FROM "+
		"(SELECT  `%s`.`%s` as `__mo_index_centroid_version_fk`,  `%s`.`%s` as `__mo_index_centroid_id_fk`,%s as `__mo_index_table_pk`, "+
		"ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s(`%s`.`%s`, normalize_l2(%s.%s))) as `__mo_index_rn` "+
		"FROM %s CROSS JOIN `%s`) sub WHERE sub.`__mo_index_rn` = 1;",
		insertSQL,

		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		originalTblPkCols,

		originalTblPkCols,
		algoParamsDistFn,
		centroidsTableName,
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
