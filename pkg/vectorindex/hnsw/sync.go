// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CdcSync is the main function to update hnsw index via CDC.  SQL function hnsw_cdc_update() will call this function.

const (
	catalogsql = "select index_table_name, algo_table_type, algo_params, column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s' and account_id = %d) and algo='hnsw';"
)

var runTxn = sqlexec.RunTxn
var runCatalogSql = sqlexec.RunSql

type HnswSync[T types.RealNumbers] struct {
	indexes []*HnswModel[T]
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	uid     string
	ts      int64
	ninsert atomic.Int32
	ndelete atomic.Int32
	nupdate atomic.Int32
	current *HnswModel[T]
	last    *HnswModel[T]
}

func (s *HnswSync[T]) RunOnce(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[T]) (err error) {

	defer s.Destroy()
	err = s.Update(sqlproc, cdc)
	if err != nil {
		return err
	}

	err = s.Save(sqlproc)
	if err != nil {
		return err
	}

	return nil
}

func NewHnswSync[T types.RealNumbers](sqlproc *sqlexec.SqlProcess,
	db string,
	tbl string,
	vectype int32,
	dimension int32) (*HnswSync[T], error) {
	var err error

	accountId := uint32(0)
	if sqlproc.Proc != nil {
		accountId, err = defines.GetAccountId(sqlproc.GetContext())
		if err != nil {
			return nil, err
		}
	} else {
		accountId = sqlproc.SqlCtx.AccountId
	}

	// get index catalog
	sql := fmt.Sprintf(catalogsql, tbl, db, accountId)
	res, err := runCatalogSql(sqlproc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	//os.Stderr.WriteString(sql)

	if len(res.Batches) == 0 {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("hnsw cdc sync: no secondary index tables found with accountID %d, table %s and db %s",
			accountId, tbl, db))
	}

	bat := res.Batches[0]

	idxtblvec := bat.Vecs[0]
	algotypevec := bat.Vecs[1]
	paramvec := bat.Vecs[2]
	colvec := bat.Vecs[3]

	var idxtblcfg vectorindex.IndexTableConfig
	var param vectorindex.HnswParam

	idxtblcfg.DbName = db
	idxtblcfg.SrcTable = tbl

	// GetResolveVariableFunc() is nil because of internal SQL proc don't have ResolveVariableFunc().
	if sqlproc.GetResolveVariableFunc() != nil {
		val, err := sqlproc.GetResolveVariableFunc()("hnsw_threads_build", true, false)
		if err != nil {
			return nil, err
		}
		idxtblcfg.ThreadsBuild = vectorindex.GetConcurrencyForBuild(val.(int64))

		idxcap, err := sqlproc.GetResolveVariableFunc()("hnsw_max_index_capacity", true, false)
		if err != nil {
			return nil, err
		}
		idxtblcfg.IndexCapacity = idxcap.(int64)
	} else {

		idxtblcfg.ThreadsBuild = vectorindex.GetConcurrencyForBuild(0)
		idxtblcfg.IndexCapacity = 1000000
	}

	for i := 0; i < bat.RowCount(); i++ {

		idxtbl := idxtblvec.UnsafeGetStringAt(i)
		algotyp := algotypevec.UnsafeGetStringAt(i)

		if i == 0 {
			paramstr := paramvec.UnsafeGetStringAt(i)
			cname := colvec.UnsafeGetStringAt(i)
			//os.Stderr.WriteString(fmt.Sprintf("idxtbl %s, type %s, param %s, cname %s\n", idxtbl, algotyp, paramstr, cname))
			idxtblcfg.KeyPart = cname
			if len(paramstr) > 0 {
				err := json.Unmarshal([]byte(paramstr), &param)
				if err != nil {
					return nil, err
				}
			}
		}

		if algotyp == catalog.Hnsw_TblType_Metadata {
			idxtblcfg.MetadataTable = idxtbl

		} else if algotyp == catalog.Hnsw_TblType_Storage {
			idxtblcfg.IndexTable = idxtbl

		}
	}

	var idxcfg vectorindex.IndexConfig
	idxcfg.Type = "hnsw"

	idxcfg.Usearch.Dimensions = uint(dimension)

	idxcfg.Usearch.Quantization, err = QuantizationToUsearch(vectype)
	if err != nil {
		return nil, err
	}

	if len(param.M) > 0 {
		val, err := strconv.Atoi(param.M)
		if err != nil {
			return nil, err
		}
		idxcfg.Usearch.Connectivity = uint(val)
	}

	// default L2Sq
	metrictype, ok := metric.OpTypeToUsearchMetric[param.OpType]
	if !ok {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), "Invalid op_type")
	}
	idxcfg.Usearch.Metric = metrictype

	if len(param.EfConstruction) > 0 {
		val, err := strconv.Atoi(param.EfConstruction)
		if err != nil {
			return nil, err
		}
		idxcfg.Usearch.ExpansionAdd = uint(val)
	}
	// ef_search
	if len(param.EfSearch) > 0 {
		val, err := strconv.Atoi(param.EfSearch)
		if err != nil {
			return nil, err
		}
		idxcfg.Usearch.ExpansionSearch = uint(val)
	}

	//os.Stderr.WriteString(fmt.Sprintf("idxtblcfg: %v\n", idxtblcfg))
	//os.Stderr.WriteString(fmt.Sprintf("idxcfg: %v\n", idxcfg))

	// load metadata
	indexes, err := LoadMetadata[T](sqlproc, idxtblcfg.DbName, idxtblcfg.MetadataTable)
	if err != nil {
		return nil, err
	}

	// assume CDC run in single thread
	// model id for CDC is cdc:1:0:timestamp
	uid := fmt.Sprintf("%s:%d:%d", "cdc", 1, 0)
	ts := time.Now().Unix()
	sync := &HnswSync[T]{indexes: indexes, idxcfg: idxcfg, tblcfg: idxtblcfg, uid: uid, ts: ts}

	// save all model to local by LoadIndex and Unload
	err = sync.DownloadAll(sqlproc)
	if err != nil {
		return nil, err
	}

	return sync, nil
}

func (s *HnswSync[T]) Destroy() {
	for _, m := range s.indexes {
		m.Destroy()
	}
	s.indexes = nil
}

func (s *HnswSync[T]) DownloadAll(sqlproc *sqlexec.SqlProcess) (err error) {

	for _, m := range s.indexes {
		err = m.LoadIndex(sqlproc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, false)
		if err != nil {
			return
		}
		err = m.Unload()
		if err != nil {
			return
		}
	}

	return
}

func (s *HnswSync[T]) checkContains(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[T]) (maxcap uint, midx []int, err error) {
	err_chan := make(chan error, s.tblcfg.ThreadsBuild)

	// try to find index cap
	cdclen := len(cdc.Data)

	midx = make([]int, cdclen)
	// reset idx to -1
	for i := range midx {
		midx[i] = -1
	}

	ninsert := int32(0)
	nupdate := int32(0)
	ndelete := int32(0)
	for _, row := range cdc.Data {
		switch row.Type {
		case vectorindex.CDC_INSERT:
			ninsert++
		case vectorindex.CDC_UPSERT:
			nupdate++
		case vectorindex.CDC_DELETE:
			ndelete++
		}
	}

	s.ninsert.Store(ninsert)
	s.nupdate.Store(nupdate)
	s.ndelete.Store(ndelete)

	// update max capacity from indexes
	maxcap = uint(s.tblcfg.IndexCapacity)
	for _, m := range s.indexes {
		if maxcap < m.MaxCapacity {
			maxcap = m.MaxCapacity
		}
	}

	// skip check when all cdc are inserts
	if ninsert == int32(len(cdc.Data)) {
		// all insert
		return maxcap, midx, nil
	}

	// find corresponding indexes
	for i, m := range s.indexes {
		err = m.LoadIndex(sqlproc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, false)
		if err != nil {
			return 0, nil, err
		}

		var wg sync.WaitGroup

		nthread := int(s.tblcfg.ThreadsBuild)
		for k := 0; k < nthread; k++ {
			wg.Add(1)
			go func(tid int) {
				defer wg.Done()
				for j, row := range cdc.Data {

					if j%nthread != tid {
						continue
					}

					if row.Type == vectorindex.CDC_INSERT {
						continue
					}

					// IMPORTANT: always check key exists even with INSERT.  Even it is INSERT, key may exist in model
					if midx[j] == -1 {
						found, err := m.Contains(row.PKey)
						if err != nil {
							err_chan <- err
							return
						}
						if found {
							//os.Stderr.WriteString(fmt.Sprintf("searching... found model %d row %d\n", i, j))
							midx[j] = i
						}
					}

				}
			}(k)
		}

		wg.Wait()
		if len(err_chan) > 0 {
			return 0, nil, <-err_chan
		}

		err = m.Unload()
		if err != nil {
			return 0, nil, err
		}
	}

	return maxcap, midx, nil
}

func (s *HnswSync[T]) insertAllInParallel(sqlproc *sqlexec.SqlProcess, maxcap uint, midx []int, cdc *vectorindex.VectorIndexCdc[T]) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	err_chan := make(chan error, s.tblcfg.ThreadsBuild)

	nthread := int(s.tblcfg.ThreadsBuild)
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()

			for j, row := range cdc.Data {

				if j%nthread != tid {
					continue
				}

				// skip delete with key not found in model
				if row.Type == vectorindex.CDC_DELETE {
					continue
				}

				// Only INSERT or UPSERT with midx[i] == -1 need to add to model
				if midx[i] != -1 {
					// key exists and ignore
					continue
				}

				// make sure last model won't unload when full and return full
				// don't unload any model here. Quite dangerous and There is no harm not to unload because
				// cdc size max is 8192.  Model will eventually unload when save.
				last, _, err := s.getLastModelAndIncrForSync(sqlproc, maxcap, &mu)
				if err != nil {
					err_chan <- err
					return
				}

				// Len counter already incremented.  Just add to last model
				last.AddWithoutIncr(row.PKey, row.Vec)
			}
		}(i)
	}

	wg.Wait()

	if len(err_chan) > 0 {
		return <-err_chan
	}

	return nil
}

func (s *HnswSync[T]) setupModel(sqlproc *sqlexec.SqlProcess, maxcap uint) error {

	s.current = (*HnswModel[T])(nil)
	s.last = (*HnswModel[T])(nil)
	if len(s.indexes) == 0 {
		// create a new model and do insert
		id := s.getModelId()
		newmodel, err := NewHnswModelForBuild[T](id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return err
		}
		s.indexes = append(s.indexes, newmodel)
		s.last = newmodel
	} else {
		s.last = s.indexes[len(s.indexes)-1]
		// last model not load yet so check the last.Len instead of Full()
		idxlen := uint(s.last.Len.Load())
		if idxlen >= s.last.MaxCapacity {
			//os.Stderr.WriteString(fmt.Sprintf("full len %d, cap %d\n", idxlen, last.MaxCapacity))
			id := s.getModelId()
			// model is already full, create a new model for insert
			newmodel, err := NewHnswModelForBuild[T](id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
			if err != nil {
				return err
			}
			s.indexes = append(s.indexes, newmodel)
			s.last = newmodel

		} else {
			//os.Stderr.WriteString(fmt.Sprintf("load model with index %d\n", len(s.indexes)-1))
			// load last
			s.last.LoadIndex(sqlproc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, false)

		}
	}

	return nil
}

func (s *HnswSync[T]) sequentialUpdate(sqlproc *sqlexec.SqlProcess, maxcap uint, midx []int, cdc *vectorindex.VectorIndexCdc[T]) error {

	for i, row := range cdc.Data {

		switch row.Type {
		case vectorindex.CDC_UPSERT:
			if midx[i] == -1 {
				// cannot find key from existing model. simple insert
				last, err := s.getLastModel(sqlproc, maxcap)
				if err != nil {
					return err
				}
				// insert
				err = last.Add(row.PKey, row.Vec)
				if err != nil {
					return err
				}

				break

			}
			current, err := s.getCurrentModel(sqlproc, midx[i])
			if err != nil {
				return err
			}

			// update
			err = current.Remove(row.PKey)
			if err != nil {
				return err
			}

			err = current.Add(row.PKey, row.Vec)
			if err != nil {
				return err
			}

		case vectorindex.CDC_DELETE:
			if midx[i] == -1 {
				// cannot find key from existing models. ignore it
				//os.Stderr.WriteString("DELETE NOT FOUND\n")
				continue
			}

			current, err := s.getCurrentModel(sqlproc, midx[i])
			if err != nil {
				return err
			}

			// delete
			err = current.Remove(row.PKey)
			if err != nil {
				return err
			}

		case vectorindex.CDC_INSERT:
			last, err := s.getLastModel(sqlproc, maxcap)
			if err != nil {
				return err
			}

			// insert
			err = last.Add(row.PKey, row.Vec)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (s *HnswSync[T]) Update(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[T]) error {
	var err error

	start := time.Now()

	// check contains and find the correspoding index id
	maxcap, midx, err := s.checkContains(sqlproc, cdc)
	if err != nil {
		return err
	}

	t := time.Now()

	checkidxElapsed := t.Sub(start)

	// setup s.last and s.current model. s.late will point to the last model in metadata and s.current is nil
	err = s.setupModel(sqlproc, maxcap)
	if err != nil {
		return err
	}

	logutil.Infof("hnsw_cdc_update[%p]: db=%s, table=%s, cdc: len=%d, ninsert = %d, ndelete = %d, nupdate = %d\n",
		s,
		s.tblcfg.DbName, s.tblcfg.SrcTable,
		len(cdc.Data), s.ninsert.Load(), s.ndelete.Load(), s.nupdate.Load())

	if len(cdc.Data) == int(s.ninsert.Load()) {
		// pure insert and insert into parallel
		err = s.insertAllInParallel(sqlproc, maxcap, midx, cdc)
		if err != nil {
			return err
		}

	} else {
		// perform sequential update in single thread
		err = s.sequentialUpdate(sqlproc, maxcap, midx, cdc)
		if err != nil {
			return err
		}
	}

	// Unload models which are full
	for _, m := range s.indexes {
		if m.Index != nil {
			full, err := m.Full()
			if err != nil {
				return err
			}
			if full {
				err = m.Unload()
				if err != nil {
					return err
				}
			}
		}
	}

	t2 := time.Now()
	updateElapsed := t2.Sub(t)
	logutil.Infof("hnsw_cdc_update[%p]: time elapsed: checkidx %d ms, update %d ms",
		s, checkidxElapsed.Milliseconds(), updateElapsed.Milliseconds())
	return nil
}

func (s *HnswSync[T]) Save(sqlproc *sqlexec.SqlProcess) error {
	// save to files and then save to database
	s.ts = time.Now().Unix()
	sqls, err := s.ToSql(s.ts)
	if err != nil {
		return err
	}

	if len(sqls) == 0 {
		return nil
	}

	err = s.runSqls(sqlproc, sqls)
	if err != nil {
		return err
	}

	// clear the cache (it only work in standalone mode though)
	veccache.Cache.Remove(s.tblcfg.IndexTable)

	return nil
}

func (s *HnswSync[T]) runSqls(sqlproc *sqlexec.SqlProcess, sqls []string) error {
	/*
		for _, s := range sqls {
			os.Stderr.WriteString(fmt.Sprintf("sql : %s\n", s))
		}
	*/
	opts := executor.Options{}
	err := runTxn(sqlproc, func(exec executor.TxnExecutor) error {
		for _, sql := range sqls {
			res, err := exec.Exec(sql, opts.StatementOption())
			if err != nil {
				return err
			}
			res.Close()
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *HnswSync[T]) getModelId() string {
	id := fmt.Sprintf("%s:%d", s.uid, s.ts)
	s.ts++
	return id
}

func (s *HnswSync[T]) getCurrentModel(sqlproc *sqlexec.SqlProcess, idx int) (*HnswModel[T], error) {
	m := s.indexes[idx]
	if s.current != m {
		// check current == last, if not, safe to unload
		if s.current != nil && s.current != s.last {
			s.current.Unload()
		}
		m.LoadIndex(sqlproc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, false)
		s.current = m
	}
	return s.current, nil
}

func (s *HnswSync[T]) getLastModel(sqlproc *sqlexec.SqlProcess, maxcap uint) (*HnswModel[T], error) {

	full, err := s.last.Full()
	if err != nil {
		return nil, err
	}

	if full {
		// check current == last, if not, safe to unload
		if s.current != s.last {
			s.last.Unload()
		}

		id := s.getModelId()
		// model is already full, create a new model for insert
		newmodel, err := NewHnswModelForBuild[T](id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return nil, err
		}
		s.indexes = append(s.indexes, newmodel)
		s.last = newmodel

	}
	//os.Stderr.WriteString(fmt.Sprintf("getlast model full %v id = %s\n", full, last.Id))
	return s.last, nil
}

func (s *HnswSync[T]) getLastModelAndIncrForSync(sqlproc *sqlexec.SqlProcess, maxcap uint, mu *sync.Mutex) (*HnswModel[T], bool, error) {

	mu.Lock()
	defer mu.Unlock()

	full := (s.last.Len.Load() >= int64(s.last.MaxCapacity))
	if full {
		id := s.getModelId()
		// model is already full, create a new model for insert
		newmodel, err := NewHnswModelForBuild[T](id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return nil, false, err
		}
		s.indexes = append(s.indexes, newmodel)
		s.last = newmodel

	}
	//os.Stderr.WriteString(fmt.Sprintf("getlast model full %v id = %s\n", full, last.Id))

	// pre-occupy this model by increment a Len counter and do Add() outside the mutex
	// make sure only one call can get full = true
	idxlen := s.last.Len.Add(1)
	full = (idxlen >= int64(s.last.MaxCapacity))
	return s.last, full, nil
}

// generate SQL to update the secondary index tables
// 1. sync the metadata table
// 2. sync the index file to index table
func (s *HnswSync[T]) ToSql(ts int64) ([]string, error) {

	if len(s.indexes) == 0 {
		return []string{}, nil
	}

	sqls := make([]string, 0, len(s.indexes)+1)

	metas := make([]string, 0, len(s.indexes))
	for _, idx := range s.indexes {
		// check Dirty.  Only update when Dirty is true
		if !idx.Dirty.Load() {
			continue
		}

		// delete sql
		deletesqls, err := idx.ToDeleteSql(s.tblcfg)
		if err != nil {
			return nil, err
		}
		if len(deletesqls) > 0 {
			sqls = append(sqls, deletesqls...)
		}

		// insert sql
		indexsqls, err := idx.ToSql(s.tblcfg)
		if err != nil {
			return nil, err
		}

		// skip when sqls is empty which means the index is empty
		if len(indexsqls) == 0 {
			continue
		}

		sqls = append(sqls, indexsqls...)

		//os.Stderr.WriteString(fmt.Sprintf("Sql: %s\n", sql))
		chksum, err := vectorindex.CheckSum(idx.Path)
		if err != nil {
			return nil, err
		}

		finfo, err := os.Stat(idx.Path)
		if err != nil {
			return nil, err
		}
		fs := finfo.Size()

		metas = append(metas, fmt.Sprintf("('%s', '%s', %d, %d)", idx.Id, chksum, ts, fs))
		ts++
	}

	if len(metas) > 0 {
		metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", s.tblcfg.DbName, s.tblcfg.MetadataTable, strings.Join(metas, ", "))
		sqls = append(sqls, metasql)
	}
	return sqls, nil
}
