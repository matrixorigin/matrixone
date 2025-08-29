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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

/* CDC APIs */
func RegisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, pitr_name string, spec *iscp.JobSpec, job *iscp.JobID) (bool, error) {
	//dummyurl := "mysql://root:111@127.0.0.1:6001"
	// sql = fmt.Sprintf("CREATE CDC `%s` '%s' 'indexsync' '%s' '%s.%s' {'Level'='table'};", cdcname, dummyurl, dummyurl, qryDatabase, srctbl)
	return iscp.RegisterJob(ctx, cnUUID, txn, pitr_name, spec, job)
}

func UnregisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
	return iscp.UnregisterJob(ctx, cnUUID, txn, job)
}

/* start here */
func CreateCdcTask(c *Compile, pitr_name string, spec *iscp.JobSpec, job *iscp.JobID) (bool, error) {
	logutil.Infof("Create Index Task %v", spec)

	return RegisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), pitr_name, spec, job)
}

func DeleteCdcTask(c *Compile, job *iscp.JobID) (bool, error) {
	logutil.Infof("Delete Index Task %v", job)
	return UnregisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), job)
}

func getIndexPitrName(dbname string, tablename string) string {
	return fmt.Sprintf("__mo_idxpitr_%s_%s", dbname, tablename)
}

func CreateIndexPitr(c *Compile, dbname string, tablename string) (string, error) {
	var sql string
	pitr_name := getIndexPitrName(dbname, tablename)
	sql = fmt.Sprintf("CREATE PITR IF NOT EXISTS `%s` FOR TABLE `%s` `%s` range 2 'h' INTERNAL;", pitr_name, dbname, tablename)
	logutil.Infof("Create Index Pitr %s. sql: %s:", pitr_name, sql)
	err := c.runSql(sql)
	if err != nil {
		return pitr_name, err
	}

	return pitr_name, nil
}

func DeleteIndexPitr(c *Compile, dbname string, tablename string) error {
	pitr_name := getIndexPitrName(dbname, tablename)
	// remove pitr
	sql := fmt.Sprintf("DROP PITR IF EXISTS `%s` INTERNAL;", pitr_name)
	logutil.Infof("Delete Index Pitr %s: %s", pitr_name, sql)
	err := c.runSql(sql)
	if err != nil {
		return err
	}

	return nil
}

func checkValidIndexCdcByIndexdef(idx *plan.IndexDef) (bool, error) {
	var err error

	if idx.TableExist &&
		(catalog.IsHnswIndexAlgo(idx.IndexAlgo) ||
			catalog.IsIvfIndexAlgo(idx.IndexAlgo) ||
			catalog.IsFullTextIndexAlgo(idx.IndexAlgo)) {
		async := false
		if catalog.IsHnswIndexAlgo(idx.IndexAlgo) {
			// HNSW always async
			async = true
		} else {
			async, err = catalog.IsIndexAsync(idx.IndexAlgoParams)
			if err != nil {
				return false, err
			}
		}

		return async, nil
	}
	return false, nil
}

func checkValidIndexCdc(tableDef *plan.TableDef, indexname string) (bool, error) {
	for _, idx := range tableDef.Indexes {

		if idx.IndexName == indexname {
			valid, err := checkValidIndexCdcByIndexdef(idx)
			if err != nil {
				return false, err
			}
			if valid {
				return true, nil
			}
		}
	}
	return false, nil
}

// NOTE: CreateIndexCdcTask will create CDC task without any checking.  Original TableDef may be empty
func CreateIndexCdcTask(c *Compile, dbname string, tablename string, indexname string, sinker_type int8) error {
	var err error

	// create table pitr if not exists and return pitr_name
	pitr_name, err := CreateIndexPitr(c, dbname, tablename)
	if err != nil {
		return err
	}

	spec := &iscp.JobSpec{
		ConsumerInfo: iscp.ConsumerInfo{ConsumerType: sinker_type,
			DBName:    dbname,
			TableName: tablename,
			IndexName: indexname},
	}
	job := &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(indexname)}

	// create index cdc task
	ok, err := CreateCdcTask(c, pitr_name, spec, job)
	if err != nil {
		return err
	}

	if !ok {
		// cdc task already exist. ignore it.  IVFFLAT alter reindex will call CreateIndexCdcTask multiple times.
		logutil.Infof("index cdc task (%s, %s, %s) already exists", dbname, tablename, indexname)
		return nil
	}
	return nil
}

func genCdcTaskJobID(indexname string) string {
	return "index_" + indexname
}

func DropIndexCdcTask(c *Compile, tableDef *plan.TableDef, dbname string, tablename string, indexname string) error {
	var err error

	valid, err := checkValidIndexCdc(tableDef, indexname)
	if err != nil {
		return err
	}

	if !valid {
		// index name is not valid cdc task. ignore it
		return nil
	}

	// delete index cdc task
	_, err = DeleteCdcTask(c, &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(indexname)})
	if err != nil {
		return err
	}

	// remove pitr if no index uses the pitr
	hasCdcIndex := false
	for _, idx := range tableDef.Indexes {
		if idx.IndexName == indexname {
			// skip same index name. don't count
			continue
		}

		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}

		if valid {
			hasCdcIndex = true
			break
		}
	}

	if !hasCdcIndex {
		// remove pitr
		err = DeleteIndexPitr(c, dbname, tablename)
		if err != nil {
			return err
		}
	}

	return nil
}

// drop all cdc tasks according to tableDef
func DropAllIndexCdcTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) error {
	idxmap := make(map[string]bool)
	hasindex := false
	for _, idx := range tabledef.Indexes {

		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}

		if valid {
			idxmap[idx.IndexName] = true
			hasindex = true
			_, e := DeleteCdcTask(c, &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(idx.IndexName)})
			if e != nil {
				return e
			}
		}
	}

	// remove pitr
	if hasindex {
		return DeleteIndexPitr(c, dbname, tablename)
	}
	return nil
}

func getSinkerTypeFromAlgo(algo string) int8 {
	if catalog.IsHnswIndexAlgo(algo) {
		return int8(iscp.ConsumerType_IndexSync)
	} else if catalog.IsIvfIndexAlgo(algo) {
		return int8(iscp.ConsumerType_IndexSync)
	} else if catalog.IsFullTextIndexAlgo(algo) {
		return int8(iscp.ConsumerType_IndexSync)
	}
	panic("getSinkerTypeFromAlgo: invalid sinker type")
}

// NOTE: CreateAllIndexCdcTasks will create CDC task according to existing tableDef
func CreateAllIndexCdcTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string) error {
	idxmap := make(map[string]bool)
	for _, idx := range indexes {
		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}

		if valid {
			idxmap[idx.IndexName] = true
			sinker_type := getSinkerTypeFromAlgo(idx.IndexAlgo)
			e := CreateIndexCdcTask(c, dbname, tablename, idx.IndexName, sinker_type)
			if e != nil {
				return e
			}
		}
	}
	return nil
}
