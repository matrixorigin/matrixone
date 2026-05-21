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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
)

var (
	iscpRegisterJobFunc   = iscp.RegisterJob
	iscpUnregisterJobFunc = iscp.UnregisterJob
	isTableInCCPRFunc     = isTableInCCPRImpl
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
	if !idx.TableExist {
		return false, nil
	}

	// Plugin-registered algorithms (vector + fulltext) describe their
	// CDC participation via SyncDescriptor().
	if p, ok := indexplugin.Get(idx.IndexAlgo); ok {
		d := p.Catalog().SyncDescriptor()
		if !d.UsesCDC {
			return false, nil
		}
		if d.AlwaysAsync {
			return true, nil
		}
		return catalog.IsIndexAsync(idx.IndexAlgoParams)
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

// isTableInCCPR checks if a table is managed by CCPR (in mo_ccpr_tables)
// Returns true if the table is in CCPR system, false otherwise
func isTableInCCPR(c *Compile, tableid uint64) bool {
	return isTableInCCPRFunc(c, tableid)
}

func isTableInCCPRImpl(c *Compile, tableid uint64) bool {
	// Check mo_ccpr_tables by tableid
	querySql := fmt.Sprintf(
		"SELECT tableid FROM `%s`.`%s` WHERE tableid = %d",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_TABLES,
		tableid,
	)

	res, err := c.runSqlWithResult(querySql, int32(catalog.System_Account))
	if err != nil {
		// If query fails, assume not in CCPR
		return false
	}
	defer res.Close()

	var found bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			found = true
		}
		return false
	})

	return found
}

// NOTE: CreateIndexCdcTask will create CDC task without any checking.  Original TableDef may be empty
func CreateIndexCdcTask(c *Compile, dbname string, tablename string, tableid uint64, indexname string, sinker_type int8, startFromNow bool, sql string, tableDef *plan.TableDef) error {
	var err error

	// Skip ISCP task creation if table is from CCPR subscription (from_publication = true)
	if isTableFromPublication(tableDef) {
		logutil.Infof("skip creating index cdc task for CCPR subscribed table (%s, %s, %s)", dbname, tablename, indexname)
		return nil
	}

	// Skip ISCP task creation if table is managed by CCPR
	if isTableInCCPR(c, tableid) {
		logutil.Infof("skip creating index cdc task for CCPR table (%s, %s, %s)", dbname, tablename, indexname)
		return nil
	}

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
	if p, ok := indexplugin.Get(algo); ok {
		if d := p.Catalog().SyncDescriptor(); d.UsesCDC {
			return d.SinkerType
		}
	}
	panic("getSinkerTypeFromAlgo: invalid sinker type")
}

// NOTE: CreateAllIndexCdcTasks will create CDC task according to existing tableDef
func CreateAllIndexCdcTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, tableid uint64, startFromNow bool, tableDef *plan.TableDef) error {
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
			e := CreateIndexCdcTask(c, dbname, tablename, tableid, idx.IndexName, sinker_type, startFromNow, "", tableDef)
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func checkValidIndexUpdateByIndexdef(idx *plan.IndexDef) (bool, error) {
	if !idx.TableExist {
		return false, nil
	}
	if p, ok := indexplugin.Get(idx.IndexAlgo); ok {
		return p.Catalog().SyncDescriptor().IdxcronAction != "", nil
	}
	return false, nil
}

// idxcron function
func CreateAllIndexUpdateTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, tableid uint64) (err error) {
	// Background re-entry (idxcron's own ALTER REINDEX, ProcessInitSQL,
	// or any internal-SQL caller whose proc has IsFrontend=false) must
	// not re-register idxcron tasks here — IdxcronMetadata returns
	// (nil,nil) in background, the resulting string(metadata) is "",
	// and the REPLACE INTO mo_index_update would fail when its JSON
	// column rejects the empty literal. Mirror the alter.go /
	// ddl.go::AlterTableInplace IsFrontend gates (commit 2c8a55957).
	if !c.proc.Base.IsFrontend {
		return
	}

	idxmap := make(map[string]bool)
	// cctx is loop-invariant (depends only on c) — lazy-init so we
	// don't allocate when no index reaches the metadata fetch.
	var cctx *pluginCompileCtx
	for _, idx := range indexes {
		if _, ok := idxmap[idx.IndexName]; ok {
			continue
		}
		if len(idx.IndexName) == 0 {
			// alter reindex SQL doesn't support empty index names; skip.
			continue
		}

		p, ok := indexplugin.Get(idx.IndexAlgo)
		if !ok {
			continue
		}
		d := p.Catalog().SyncDescriptor()
		if d.IdxcronAction == "" {
			continue
		}
		if cctx == nil {
			cctx = newPluginCompileCtxForSync(c)
		}
		metadata, mErr := p.Compile().IdxcronMetadata(cctx)
		if mErr != nil {
			err = mErr
			return
		}

		idxmap[idx.IndexName] = true
		err = idxcron.RegisterUpdate(c.proc.Ctx,
			c.proc.GetService(),
			c.proc.GetTxnOperator(),
			tableid,
			dbname,
			tablename,
			idx.IndexName,
			d.IdxcronAction,
			string(metadata))
		if err != nil {
			return
		}
	}
	return
}

// drop all cdc tasks according to tableDef
func DropAllIndexUpdateTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) (err error) {
	idxmap := make(map[string]bool)
	for _, idx := range tabledef.Indexes {
		if _, ok := idxmap[idx.IndexName]; ok {
			continue
		}

		p, ok := indexplugin.Get(idx.IndexAlgo)
		if !ok {
			continue
		}
		d := p.Catalog().SyncDescriptor()
		if d.IdxcronAction == "" {
			continue
		}
		action := d.IdxcronAction

		idxmap[idx.IndexName] = true
		err = idxcron.UnregisterUpdate(c.proc.Ctx,
			c.proc.GetService(),
			c.proc.GetTxnOperator(),
			tabledef.TblId,
			idx.IndexName,
			action)
		if err != nil {
			return
		}
	}
	return
}
