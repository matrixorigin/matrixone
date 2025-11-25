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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var (
	iscpRegisterJobFunc   = iscp.RegisterJob
	iscpUnregisterJobFunc = iscp.UnregisterJob
)

/* CDC APIs */
func RegisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	//dummyurl := "mysql://root:111@127.0.0.1:6001"
	// sql = fmt.Sprintf("CREATE CDC `%s` '%s' 'indexsync' '%s' '%s.%s' {'Level'='table'};", cdcname, dummyurl, dummyurl, qryDatabase, srctbl)
	return iscpRegisterJobFunc(ctx, cnUUID, txn, spec, job, startFromNow)
}

func UnregisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
	return iscpUnregisterJobFunc(ctx, cnUUID, txn, job)
}

/* start here */
func CreateCdcTask(c *Compile, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	logutil.Infof("Create Index Task %v", spec)

	return RegisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), spec, job, startFromNow)
}

func DeleteCdcTask(c *Compile, job *iscp.JobID) (bool, error) {
	logutil.Infof("Delete Index Task %v", job)
	return UnregisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), job)
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
func CreateIndexCdcTask(c *Compile, dbname string, tablename string, indexname string, sinker_type int8, startFromNow bool, sql string) error {
	var err error

	spec := &iscp.JobSpec{
		ConsumerInfo: iscp.ConsumerInfo{ConsumerType: sinker_type,
			DBName:    dbname,
			TableName: tablename,
			IndexName: indexname,
			InitSQL:   sql},
	}
	job := &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(indexname)}

	// create index cdc task
	ok, err := CreateCdcTask(c, spec, job, startFromNow)
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

	return nil
}

// drop all cdc tasks according to tableDef
func DropAllIndexCdcTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) error {
	idxmap := make(map[string]bool)
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
			//hasindex = true
			_, e := DeleteCdcTask(c, &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(idx.IndexName)})
			if e != nil {
				return e
			}
		}
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
func CreateAllIndexCdcTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, startFromNow bool) error {
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
			e := CreateIndexCdcTask(c, dbname, tablename, idx.IndexName, sinker_type, startFromNow, "")
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func getIvfflatMetadata(c *Compile) (metadata []byte, frontend bool, err error) {
	var val any

	// only frontend has ivf_threads_search variable declared
	_, err = c.proc.GetResolveVariableFunc()("ivf_threads_search", true, false)
	if err == nil {
		frontend = true
	}

	// When Clone, variables are nil. Set variable to default value
	val, err = c.proc.GetResolveVariableFunc()("ivf_threads_build", true, false)
	if err != nil {
		return
	}
	threadsBuild := int64(0)
	if val != nil {
		threadsBuild = val.(int64)
	}

	val, err = c.proc.GetResolveVariableFunc()("kmeans_train_percent", true, false)
	if err != nil {
		return
	}
	kmeansTrainPercent := float64(10)
	if val != nil {
		kmeansTrainPercent = val.(float64)
	}

	val, err = c.proc.GetResolveVariableFunc()("kmeans_max_iteration", true, false)
	if err != nil {
		return
	}
	kmeansMaxIteration := int64(20)
	if val != nil {
		kmeansMaxIteration = val.(int64)
	}

	val, err = c.proc.GetResolveVariableFunc()("lower_case_table_names", true, false)
	if err != nil {
		return
	}
	lowerCaseTableNames := int64(1)
	if val != nil {
		lowerCaseTableNames = val.(int64)
	}

	val, err = c.proc.GetResolveVariableFunc()("experimental_ivf_index", true, false)
	if err != nil {
		return
	}
	experimentalIvfIndex := int8(1)
	if val != nil {
		experimentalIvfIndex = val.(int8)
	}

	w := sqlexec.NewMetadataWriter()
	w.AddInt("ivf_threads_build", threadsBuild)
	w.AddFloat("kmeans_train_percent", kmeansTrainPercent)
	w.AddInt("kmeans_max_iteration", kmeansMaxIteration)
	w.AddInt("lower_case_table_names", lowerCaseTableNames)
	w.AddInt8("experimental_ivf_index", experimentalIvfIndex)

	metadata, err = w.Marshal()
	if err != nil {
		return
	}

	return
}

func checkValidIndexUpdateByIndexdef(idx *plan.IndexDef) (bool, error) {
	var err error

	if idx.TableExist && catalog.IsIvfIndexAlgo(idx.IndexAlgo) {

		auto, err = catalog.IsAutoUpdate(idx.IndexAlgoParams)
		if err != nil {
			return false, err
		}

		return auto, nil
	}
	return false, nil
}

// idxcron function
func CreateAllIndexUpdateTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, tableid uint64) (err error) {
	var (
		ivf_metadata []byte
	)

	if c.proc.GetResolveVariableFunc() == nil {
		return
	}

	idxmap := make(map[string]bool)
	for _, idx := range indexes {
		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexUpdateByIndexdef(idx)
		if err != nil {
			return
		}
		if valid {
			idxmap[idx.IndexName] = true

			if len(idx.IndexName) == 0 {
				// skip empty index name because alter reindex sql don't support empty index name
				continue
			}

			if ivf_metadata == nil {
				ivf_metadata, _, err = getIvfflatMetadata(c)
				if err != nil {
					return
				}
			}

			err = idxcron.RegisterUpdate(c.proc.Ctx,
				c.proc.GetService(),
				c.proc.GetTxnOperator(),
				tableid,
				dbname,
				tablename,
				idx.IndexName,
				idxcron.Action_Ivfflat_Reindex,
				string(ivf_metadata))
			if err != nil {
				return
			}
		}
	}
	return
}

// drop all cdc tasks according to tableDef
func DropAllIndexUpdateTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) (err error) {
	idxmap := make(map[string]bool)
	for _, idx := range tabledef.Indexes {

		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexUpdateByIndexdef(idx)
		if err != nil {
			return
		}
		if valid {
			idxmap[idx.IndexName] = true
			//hasindex = true

			err = idxcron.UnregisterUpdate(c.proc.Ctx,
				c.proc.GetService(),
				c.proc.GetTxnOperator(),
				tabledef.TblId,
				idx.IndexName,
				idxcron.Action_Ivfflat_Reindex)
			if err != nil {
				return
			}
		}
	}
	return
}
