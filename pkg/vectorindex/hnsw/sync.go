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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	catalogsql = "select index_table_name, algo_table_type, algo_params, column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s') and algo='hnsw';"
)

var runTxn = sqlexec.RunTxn

func CdcSync(proc *process.Process, db string, tbl string, dimension int32, cdc *vectorindex.VectorIndexCdc[float32]) error {

	sql := fmt.Sprintf(catalogsql, tbl, db)
	res, err := runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	os.Stderr.WriteString(sql)
	os.Stderr.WriteString(fmt.Sprintf("\nnumber of batch = %d\n", len(res.Batches)))

	if len(res.Batches) == 0 {
		return nil
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
	if proc.GetResolveVariableFunc() != nil {
		val, err := proc.GetResolveVariableFunc()("hnsw_threads_build", true, false)
		if err != nil {
			return err
		}
		idxtblcfg.ThreadsBuild = val.(int64)

		idxcap, err := proc.GetResolveVariableFunc()("hnsw_max_index_capacity", true, false)
		if err != nil {
			return err
		}
		idxtblcfg.IndexCapacity = idxcap.(int64)
	} else {

		idxtblcfg.ThreadsBuild = 0
		idxtblcfg.IndexCapacity = 1000000
	}

	for i := 0; i < bat.RowCount(); i++ {

		idxtbl := idxtblvec.UnsafeGetStringAt(i)
		algotyp := algotypevec.UnsafeGetStringAt(i)

		if i == 0 {
			paramstr := paramvec.UnsafeGetStringAt(i)
			cname := colvec.UnsafeGetStringAt(i)
			os.Stderr.WriteString(fmt.Sprintf("idxtbl %s, type %s, param %s, cname %s\n", idxtbl, algotyp, param, cname))
			idxtblcfg.KeyPart = cname
			if len(paramstr) > 0 {
				err := json.Unmarshal([]byte(paramstr), &param)
				if err != nil {
					return err
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

	if len(param.Quantization) > 0 {
		var ok bool
		idxcfg.Usearch.Quantization, ok = vectorindex.QuantizationValid(param.Quantization)
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "Invalid quantization value")

		}
	}

	if len(param.M) > 0 {
		val, err := strconv.Atoi(param.M)
		if err != nil {
			return err
		}
		idxcfg.Usearch.Connectivity = uint(val)
	}

	// default L2Sq
	metrictype, ok := metric.OpTypeToUsearchMetric[param.OpType]
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "Invalid op_type")
	}
	idxcfg.Usearch.Metric = metrictype

	if len(param.EfConstruction) > 0 {
		val, err := strconv.Atoi(param.EfConstruction)
		if err != nil {
			return err
		}
		idxcfg.Usearch.ExpansionAdd = uint(val)
	}
	// ef_search
	if len(param.EfSearch) > 0 {
		val, err := strconv.Atoi(param.EfSearch)
		if err != nil {
			return err
		}
		idxcfg.Usearch.ExpansionSearch = uint(val)
	}

	os.Stderr.WriteString(fmt.Sprintf("idxtblcfg: %v\n", idxtblcfg))
	os.Stderr.WriteString(fmt.Sprintf("idxcfg: %v\n", idxcfg))

	// load metadata
	indexes, err := LoadMetadata(proc, idxtblcfg.DbName, idxtblcfg.MetadataTable)
	if err != nil {
		return err
	}

	os.Stderr.WriteString(fmt.Sprintf("meta: %v\n", indexes))

	// assume CDC run in single thread
	// model id for CDC is cdc:1:0:timestamp
	uid := fmt.Sprintf("%s:%d:%d", "cdc", 1, 0)
	ts := time.Now().Unix()
	sync := &HnswSync{indexes: indexes, idxcfg: idxcfg, tblcfg: idxtblcfg, cdc: cdc, uid: uid, ts: ts}
	return sync.run(proc)
}

type HnswSync struct {
	indexes []*HnswModel
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	cdc     *vectorindex.VectorIndexCdc[float32]
	uid     string
	ts      int64
}

func (s *HnswSync) destroy() {
	for _, m := range s.indexes {
		m.Destroy()
	}
	s.indexes = nil
}

func (s *HnswSync) run(proc *process.Process) error {
	var err error

	defer s.destroy()

	maxcap := uint(s.tblcfg.IndexCapacity)

	// try to find index cap
	cdclen := len(s.cdc.Data)
	midx := make([]int, cdclen)
	// reset idx to -1
	for i := range midx {
		midx[i] = -1
	}

	// find corresponding indexes
	for i, m := range s.indexes {
		err = m.LoadIndex(proc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, true)
		if err != nil {
			return err
		}

		capacity, err := m.Index.Capacity()
		if err != nil {
			return err
		}
		m.MaxCapacity = capacity
		mlen, err := m.Index.Len()
		if err != nil {
			return err
		}
		m.Len = mlen

		if maxcap < capacity {
			maxcap = capacity
		}

		for j, row := range s.cdc.Data {
			switch row.Type {
			case vectorindex.CDC_UPSERT, vectorindex.CDC_DELETE:
				if midx[j] == -1 {
					found, err := m.Contains(row.PKey)
					if err != nil {
						return err
					}
					if found {
						midx[j] = i
					}
				}
			}
		}

		m.Unload()
	}

	current := (*HnswModel)(nil)
	last := (*HnswModel)(nil)
	if len(s.indexes) == 0 {
		// create a new model and do insert
		id := s.getModelId()
		newmodel, err := NewHnswModelForBuild(id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return err
		}
		newmodel.InsertMeta = true
		s.indexes = append(s.indexes, newmodel)
		last = newmodel
	} else {
		last = s.indexes[len(s.indexes)-1]
		// last model not load yet so check the last.Len instead of Full()
		if last.Len >= last.MaxCapacity {
			id := s.getModelId()
			// model is already full, create a new model for insert
			newmodel, err := NewHnswModelForBuild(id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
			if err != nil {
				return err
			}
			newmodel.InsertMeta = true
			s.indexes = append(s.indexes, newmodel)
			last = newmodel

		} else {
			// load last
			last.LoadIndex(proc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, true)

		}
	}

	for i, row := range s.cdc.Data {

		switch row.Type {
		case vectorindex.CDC_UPSERT:
			if midx[i] == -1 {
				// cannot find key from existing model. simple insert
				last, err = s.getLastModel(proc, last, maxcap)
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
			current, err := s.getCurrentModel(proc, current, midx[i])
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
				continue
			}

			current, err := s.getCurrentModel(proc, current, midx[i])
			if err != nil {
				return err
			}

			// delete
			err = current.Remove(row.PKey)
			if err != nil {
				return err
			}

		case vectorindex.CDC_INSERT:
			last, err = s.getLastModel(proc, last, maxcap)
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

	// save to files and then save to database
	sqls, err := s.ToSql(s.ts)
	if err != nil {
		return err
	}

	if len(sqls) == 0 {
		return nil
	}

	return s.runSqls(proc, sqls)
}

func (s *HnswSync) runSqls(proc *process.Process, sqls []string) error {
	for _, s := range sqls {
		os.Stderr.WriteString(fmt.Sprintf("sql : %s\n", s))
	}
	opts := executor.Options{}
	err := runTxn(proc, func(exec executor.TxnExecutor) error {
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

func (s *HnswSync) getModelId() string {
	id := fmt.Sprintf("%s:%d", s.uid, s.ts)
	s.ts++
	return id
}

func (s *HnswSync) getCurrentModel(proc *process.Process, current *HnswModel, idx int) (*HnswModel, error) {
	m := s.indexes[idx]
	if current != m {
		if current != nil {
			current.Unload()
		}
		m.LoadIndex(proc, s.idxcfg, s.tblcfg, s.tblcfg.ThreadsBuild, true)
		current = m
	}
	return current, nil
}

func (s *HnswSync) getLastModel(proc *process.Process, last *HnswModel, maxcap uint) (*HnswModel, error) {

	full, err := last.Full()
	if err != nil {
		return nil, err
	}

	if full {
		id := s.getModelId()
		// model is already full, create a new model for insert
		newmodel, err := NewHnswModelForBuild(id, s.idxcfg, int(s.tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return nil, err
		}
		newmodel.InsertMeta = true
		s.indexes = append(s.indexes, newmodel)
		last = newmodel

	}
	return last, nil
}

// generate SQL to update the secondary index tables
// 1. sync the metadata table
// 2. sync the index file to index table
func (s *HnswSync) ToSql(ts int64) ([]string, error) {

	if len(s.indexes) == 0 {
		return []string{}, nil
	}

	sqls := make([]string, 0, len(s.indexes)+1)

	metas := make([]string, 0, len(s.indexes))
	for _, idx := range s.indexes {
		// check Dirty.  Only update when Dirty is true
		if !idx.Dirty {
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
	}

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", s.tblcfg.DbName, s.tblcfg.MetadataTable, strings.Join(metas, ", "))

	sqls = append(sqls, metasql)
	return sqls, nil
}
