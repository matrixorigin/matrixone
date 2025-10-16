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
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

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

func (idx *IvfflatSearchIndex[T]) LoadIndex(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

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

func (idx *IvfflatSearchIndex[T]) findCentroids(proc *process.Process, query []T, distfn metric.DistanceFunction[T], idxcfg vectorindex.IndexConfig, probe uint, nthread int64) ([]int64, error) {

	if len(idx.Centroids) == 0 {
		// empty index has id = 1
		return []int64{1}, nil
	}

	nworker := int(nthread)
	ncentroids := int(len(idx.Centroids))
	if ncentroids < nworker {
		nworker = ncentroids
	}
	errs := make(chan error, nworker)

	heap := vectorindex.NewSearchResultSafeHeap(int(probe))
	var wg sync.WaitGroup
	for i := 0; i < nworker; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for i, c := range idx.Centroids {
				if i%nworker != tid {
					continue
				}
				dist, err := distfn(query, c.Vec)
				if err != nil {
					errs <- err
					return
				}
				heap.Push(&vectorindex.SearchResult{Id: c.Id, Distance: float64(dist)})
			}
		}(i)
	}

	wg.Wait()

	if len(errs) > 0 {
		return nil, <-errs
	}

	res := make([]int64, 0, probe)
	n := heap.Len()
	for range n {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "findCentroids: heap return key is not int64")
		}
		res = append(res, sr.Id)
	}

	//os.Stderr.WriteString(fmt.Sprintf("probe %d... return centroid ids %v\n", probe, res))
	return res, nil
}

// Call usearch.Search
func (idx *IvfflatSearchIndex[T]) Search(
	proc *process.Process,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	rt vectorindex.RuntimeConfig,
	nthread int64,
) (keys any, distances []float64, err error) {

	stream_chan := make(chan executor.Result, nthread)
	error_chan := make(chan error, nthread)

	distfn, err := metric.ResolveDistanceFn[T](metric.MetricType(idxcfg.Ivfflat.Metric))
	if err != nil {
		return nil, nil, err
	}

	centroids_ids, err := idx.findCentroids(proc, query, distfn, idxcfg, rt.Probe, nthread)
	if err != nil {
		return nil, nil, err
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

	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancelCause(proc.GetTopContext())
	)
	defer cancel(nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err2 := runSql_streaming(ctx, proc, sql, stream_chan, error_chan)
		if err2 != nil {
			// consumer notify the producer to stop the sql streaming
			cancel(err2)
			return
		}

		rt.BackgroundQueries[0] = res.LogicalPlan
	}()

	distances = make([]float64, 0, rt.Limit)
	resid := make([]any, 0, rt.Limit)

loop:
	for {
		select {
		case res, ok := <-stream_chan:
			if !ok {
				break loop
			}

			bat := res.Batches[0]
			defer res.Close()

			for i := 0; i < bat.RowCount(); i++ {
				pk := vector.GetAny(bat.Vecs[0], i, true)
				resid = append(resid, pk)

				dist := vector.GetFixedAtNoTypeCheck[float64](bat.Vecs[1], i)
				dist = metric.DistanceTransformIvfflat(dist, idxcfg.OpType, metric.MetricType(idxcfg.Ivfflat.Metric))
				distances = append(distances, dist)
			}

		case err = <-error_chan:
			return nil, nil, err
		case <-proc.Ctx.Done():
			return nil, nil, proc.Ctx.Err()
		case <-ctx.Done():
			// local context cancelled. something went wrong with other threads
			return nil, nil, context.Cause(ctx)
		}
	}

	wg.Wait()

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
	proc *process.Process, anyquery any, rt vectorindex.RuntimeConfig,
) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfSearch: query not match with index type")
	}

	return s.Index.Search(proc, s.Idxcfg, s.Tblcfg, query, rt, s.ThreadsSearch)
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
func (s *IvfflatSearch[T]) Load(proc *process.Process) error {

	idx := &IvfflatSearchIndex[T]{}
	// load index model
	err := idx.LoadIndex(proc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch)
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
