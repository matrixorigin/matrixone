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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	catalogsql = "select index_table_name, algo_table_type, algo_params, column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s') and algo='hnsw';"
)

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

	return startsync(proc, indexes, idxcfg, idxtblcfg, cdc)
}

func startsync(proc *process.Process, indexes []*HnswModel, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, cdc *vectorindex.VectorIndexCdc[float32]) error {
	var err error

	defer func() {
		for _, m := range indexes {
			m.Destroy()
		}
	}()

	maxcap := uint(0)

	// try to find index cap
	cdclen := len(cdc.Data)
	midx := make([]int, cdclen)
	// reset idx to -1
	for i := range midx {
		midx[i] = -1
	}

	// find corresponding indexes
	for i, m := range indexes {
		err = m.LoadIndex(proc, idxcfg, tblcfg, tblcfg.ThreadsBuild, true)
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

		for j, row := range cdc.Data {
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
	if len(indexes) == 0 {
		// create a new model and do insert
		newmodel, err := NewHnswModelForBuild("", idxcfg, int(tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return err
		}
		indexes = append(indexes, newmodel)
		last = newmodel
	} else {
		last = indexes[len(indexes)-1]
	}
	// last model not load yet so check the last.Len instead of Full()
	if last.Len >= last.MaxCapacity {
		// model is already full, create a new model for insert
		newmodel, err := NewHnswModelForBuild("", idxcfg, int(tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return err
		}
		indexes = append(indexes, newmodel)
		last = newmodel

	} else {
		// load last
		last.LoadIndex(proc, idxcfg, tblcfg, tblcfg.ThreadsBuild, true)

	}

	for i, row := range cdc.Data {

		switch row.Type {
		case vectorindex.CDC_UPSERT:
			if midx[i] == -1 {
				// cannot find key from existing model. simple insert
				last, err = getLastModel(proc, idxcfg, tblcfg, indexes, last, maxcap)
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
			current, err := getCurrentModel(proc, idxcfg, tblcfg, indexes, current, midx[i])
			if err != nil {
				return err
			}

			// update
			_ = current

		case vectorindex.CDC_DELETE:
			if midx[i] == -1 {
				// cannot find key from existing models. ignore it
				continue
			}

			current, err := getCurrentModel(proc, idxcfg, tblcfg, indexes, current, midx[i])
			if err != nil {
				return err
			}

			// delete
			err = current.Remove(row.PKey)
			if err != nil {
				return err
			}

		case vectorindex.CDC_INSERT:
			last, err = getLastModel(proc, idxcfg, tblcfg, indexes, last, maxcap)
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

	// save to files

	// save to database

	return nil
}

func getCurrentModel(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, indexes []*HnswModel, current *HnswModel, idx int) (*HnswModel, error) {
	m := indexes[idx]
	if current != m {
		if current != nil {
			current.Unload()
		}
		m.LoadIndex(proc, idxcfg, tblcfg, tblcfg.ThreadsBuild, true)
		current = m
	}
	return current, nil
}

func getLastModel(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, indexes []*HnswModel, last *HnswModel, maxcap uint) (*HnswModel, error) {

	full, err := last.Full()
	if err != nil {
		return nil, err
	}

	if full {
		// model is already full, create a new model for insert
		newmodel, err := NewHnswModelForBuild("", idxcfg, int(tblcfg.ThreadsBuild), maxcap)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, newmodel)
		last = newmodel

	}
	return last, nil
}
