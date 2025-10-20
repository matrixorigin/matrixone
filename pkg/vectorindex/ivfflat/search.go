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

package ivfflat

import (
	"container/heap"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runSql = sqlexec.RunSql

type Centroid[T types.RealNumbers] struct {
	Id  int64
	Vec []T
}

// Ivf search index struct to hold the usearch index
type IvfflatSearchIndex[T types.RealNumbers] struct {
	Version   int64
	Centroids []Centroid[T]
}

// This is the Ivf search implementation that implement VectorIndexSearchIf interface
type IvfflatSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Index         *IvfflatSearchIndex[T]
	ThreadsSearch int64
}

func (idx *IvfflatSearchIndex[T]) LoadIndex(proc *sqlexec.SqlProcess, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

	idx.Version = idxcfg.Ivfflat.Version
	sql := fmt.Sprintf(
		"SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = %d",
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		tblcfg.DbName, tblcfg.IndexTable,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		idxcfg.Ivfflat.Version,
	)

	//os.Stderr.WriteString(fmt.Sprintf("Load Index SQL = %s\n", sql))
	res, err := runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return nil
	}

	idx.Centroids = make([]Centroid[T], 0, idxcfg.Ivfflat.Lists)
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		faVec := bat.Vecs[1]
		ids := vector.MustFixedColNoTypeCheck[int64](idVec)
		hasNull := faVec.HasNull()
		for i, id := range ids {
			if hasNull && faVec.IsNull(uint64(i)) {
				//os.Stderr.WriteString("Centroid is NULL\n")
				continue
			}
			val := faVec.GetStringAt(i)
			vec := types.BytesToArray[T](util.UnsafeStringToBytes(val))
			idx.Centroids = append(idx.Centroids, Centroid[T]{Id: id, Vec: vec})
		}
	}

	//os.Stderr.WriteString(fmt.Sprintf("%d centroids loaded... lists = %d, centroid %v\n", len(idx.Centroids), idxcfg.Ivfflat.Lists, idx.Centroids))
	return nil
}

func (idx *IvfflatSearchIndex[T]) findCentroids(sqlproc *sqlexec.SqlProcess, query []T, distfn metric.DistanceFunction[T], _ vectorindex.IndexConfig, probe uint, _ int64) ([]int64, error) {

	if len(idx.Centroids) == 0 {
		// empty index has id = 1
		return []int64{1}, nil
	}

	hp := make(vectorindex.SearchResultMaxHeap, 0, int(probe))
	for _, c := range idx.Centroids {
		dist, err := distfn(query, c.Vec)
		if err != nil {
			return nil, err
		}
		dist64 := float64(dist)

		if len(hp) >= int(probe) {
			if hp[0].GetDistance() > dist64 {
				hp[0] = &vectorindex.SearchResult{Id: c.Id, Distance: dist64}
				heap.Fix(&hp, 0)
			}
		} else {
			hp.Push(&vectorindex.SearchResult{Id: c.Id, Distance: dist64})
		}
	}

	n := hp.Len()
	res := make([]int64, 0, n)
	for range n {
		srif := hp.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, moerr.NewInternalError(sqlproc.GetContext(), "findCentroids: heap return key is not int64")
		}
		res = append(res, sr.Id)
	}

	//os.Stderr.WriteString(fmt.Sprintf("probe %d... return centroid ids %v\n", probe, res))
	return res, nil
}

// Call usearch.Search
func (idx *IvfflatSearchIndex[T]) Search(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	rt vectorindex.RuntimeConfig,
	nthread int64,
) (keys any, distances []float64, err error) {

	distfn, err := metric.ResolveDistanceFn[T](metric.MetricType(idxcfg.Ivfflat.Metric))
	if err != nil {
		return
	}

	centroids_ids, err := idx.findCentroids(sqlproc, query, distfn, idxcfg, rt.Probe, nthread)
	if err != nil {
		return
	}

	var instr string
	for i, c := range centroids_ids {
		if i > 0 {
			instr += ","
		}
		instr += strconv.FormatInt(c, 10)
	}

	sql := fmt.Sprintf(
		"SELECT `%s`, %s(`%s`, '%s') as vec_dist FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s) ORDER BY vec_dist LIMIT %d",
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
		types.ArrayToString(query),
		tblcfg.DbName, tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		idx.Version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		instr,
		rt.Limit,
	)

	//fmt.Println("IVFFlat SQL: ", sql)
	//os.Stderr.WriteString(sql)

	res, err := runSql(sqlproc, sql)
	if err != nil {
		return
	}
	if len(res.Batches) == 0 {
		return nil, nil, moerr.NewEmptyVector(sqlproc.GetContext())
	}

	if len(rt.BackgroundQueries) > 0 {
		rt.BackgroundQueries[0] = res.LogicalPlan
	}

	distances = make([]float64, 0, rt.Limit)
	resid := make([]any, 0, rt.Limit)
	bat := res.Batches[0]

	for i := 0; i < bat.RowCount(); i++ {
		pk := vector.GetAny(bat.Vecs[0], i, true)
		resid = append(resid, pk)

		dist := vector.GetFixedAtNoTypeCheck[float64](bat.Vecs[1], i)
		dist = metric.DistanceTransformIvfflat(dist, idxcfg.OpType, metric.MetricType(idxcfg.Ivfflat.Metric))
		distances = append(distances, dist)
	}

	res.Close()

	return resid, distances, nil
}

func (idx *IvfflatSearchIndex[T]) Destroy() {
	idx.Centroids = nil
}

func NewIvfflatSearch[T types.RealNumbers](
	idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig,
) *IvfflatSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	s := &IvfflatSearch[T]{Idxcfg: idxcfg, Tblcfg: tblcfg, ThreadsSearch: nthread}
	return s
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *IvfflatSearch[T]) Search(
	sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig,
) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfSearch: query not match with index type")
	}

	return s.Index.Search(sqlproc, s.Idxcfg, s.Tblcfg, query, rt, s.ThreadsSearch)
}

func (s *IvfflatSearch[T]) Contains(key int64) (bool, error) {
	return true, nil
}

// Destroy IvfflatSearch (implement VectorIndexSearch.Destroy)
func (s *IvfflatSearch[T]) Destroy() {
	// destroy index
	if s.Index != nil {
		s.Index.Destroy()
	}
	s.Index = nil
}

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *IvfflatSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {

	idx := &IvfflatSearchIndex[T]{}
	// load index model
	err := idx.LoadIndex(sqlproc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch)
	if err != nil {
		return err
	}
	s.Index = idx
	return nil
}

// check config and update some parameters such as ef_search
func (s *IvfflatSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {

	return nil
}
