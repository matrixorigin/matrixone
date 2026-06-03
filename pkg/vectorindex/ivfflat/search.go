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
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/gpumode"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// exactPkFilterThreshold controls when WaitUniqueJoinKeys converts the received
// unique join keys into an exact "pk IN (...)" filter instead of building a
// bloom filter. For very small PK sets, bloom filter false positives interact
// poorly with centroid pruning in IVF pre mode. Keeping this threshold small
// avoids overly large IN lists while preserving the bloom filter performance
// path for larger sets. Adjust this number if future workloads show a better cutoff.
const exactPkFilterThreshold = 100

var runSql = sqlexec.RunSql

// Ivf search index struct to hold the usearch index
type IvfflatSearchIndex[T types.RealNumbers] struct {
	Version   int64
	Centroids cache.VectorIndexSearchIf
}

// This is the Ivf search implementation that implement VectorIndexSearchIf interface
type IvfflatSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Index         *IvfflatSearchIndex[T]
	ThreadsSearch int64
}

func (idx *IvfflatSearchIndex[T]) LoadCentroids(proc *sqlexec.SqlProcess, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

	logutil.Infof("IVFFLAT START: Load Centroids")
	defer logutil.Infof("IVFFLAT END: Load Centroids")
	// load centroids
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

	ncenters := 0
	centroids := make([][]T, idxcfg.Ivfflat.Lists)
	elemsz := res.Batches[0].Vecs[1].GetType().GetArrayElementSize()
	for _, bat := range res.Batches {
		faVec := bat.Vecs[1]
		idVec := bat.Vecs[0]
		ids := vector.MustFixedColNoTypeCheck[int64](idVec)
		hasNull := faVec.HasNull()
		for i, id := range ids {
			if hasNull && faVec.IsNull(uint64(i)) {
				continue
			}
			val := faVec.GetStringAt(i)
			vec := types.BytesToArray[T](util.UnsafeStringToBytes(val))
			centroids[id] = vec
			ncenters += 1
		}
	}

	if ncenters == 0 {
		return nil
	}

	if uint(ncenters) != idxcfg.Ivfflat.Lists {
		return moerr.NewInternalErrorNoCtx("number of centroids in db != Nlist")
	}

	gpuMode := gpumode.EffectiveGpuMode(proc.GetResolveVariableFunc())
	bfidx, err := brute_force.NewBruteForceIndex[T](centroids, idxcfg.Ivfflat.Dimensions, metric.MetricType(idxcfg.Ivfflat.Metric), uint(elemsz), uint(nthread), gpuMode)
	if err != nil {
		return err
	}
	err = bfidx.Load(proc)
	if err != nil {
		return err
	}

	idx.Centroids = bfidx

	return nil
}

func (idx *IvfflatSearchIndex[T]) LoadIndex(proc *sqlexec.SqlProcess, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) (err error) {

	idx.Version = idxcfg.Ivfflat.Version

	err = idx.LoadCentroids(proc, idxcfg, tblcfg, nthread)
	if err != nil {
		return err
	}

	return nil
}

func (idx *IvfflatSearchIndex[T]) findCentroids(sqlproc *sqlexec.SqlProcess, query []T, distfn metric.DistanceFunction[T], idxcfg vectorindex.IndexConfig, probe uint, _ int64) ([]int64, error) {

	if idx.Centroids == nil {
		// empty index has id = 1
		return []int64{1}, nil
	}

	if probe > idxcfg.Ivfflat.Lists {
		probe = idxcfg.Ivfflat.Lists
	}

	rtprobe := probe
	queries := [][]T{query}
	rt := vectorindex.RuntimeConfig{Limit: rtprobe, NThreads: 1}
	keys, _, err := idx.Centroids.Search(sqlproc, queries, rt)
	if err != nil {
		return nil, err
	}
	return keys.([]int64), nil
}

/*
prepare the runtime doc_id pushdown filter for pre-filtering.

1. get the unique join keys from hashbuild
2. for a very small key set, emit an exact "pk IN (...)" SQL filter and return
3. otherwise build an exact doc_id membership filter directly from the keys

There is no need to first intersect the keys with the selected centroids'
entries. The reader only scans entries within the selected centroids, and an
entry passes iff its PK is in the key set — so an exact filter built over the
full key set yields the identical result as the (former) centroid-narrowed set.
The old centroid-bloom narrowing only mattered to keep an approximate bloom
filter small; with docfilter's exact bitset (cbitmap / CRoaring) it is a no-op,
so the per-centroid bloom build/merge (and its preload path) has been removed.
*/
func (idx *IvfflatSearchIndex[T]) getBloomFilter(sqlproc *sqlexec.SqlProcess) (err error) {

	if sqlproc.Proc == nil {
		return
	}

	if len(sqlproc.RuntimeFilterSpecs) == 0 {
		return
	}
	spec := sqlproc.RuntimeFilterSpecs[0]
	if !spec.UseMembershipFilter {
		return
	}

	// Get raw unique join key bytes from the build side.
	vecbytes, err := sqlexec.WaitUniqueJoinKeys(sqlproc)
	if err != nil {
		return
	}
	if len(vecbytes) == 0 {
		return
	}

	// Deserialize the unique join keys.
	keyvec := new(vector.Vector)
	err = keyvec.UnmarshalBinary(vecbytes)
	if err != nil {
		return
	}
	// Free the deserialized key vector once the filter is built; it is only read
	// here to produce the exact pk-IN list or the docfilter payload (the fulltext
	// path frees its keyvec the same way).
	defer keyvec.Free(sqlproc.Proc.Mp())

	// Small PK set: build an exact "pk IN (...)" SQL filter instead of a
	// pushdown doc_id filter. For very small sets the IN-list is cheaper and
	// more selective at the scan than a runtime membership filter.
	if exactPkFilterThreshold > 0 && keyvec.Length() <= exactPkFilterThreshold {
		exactPk, buildErr := sqlexec.BuildExactPkFilter(sqlproc.GetContext(), keyvec)
		if buildErr != nil {
			err = buildErr
			return
		}
		if exactPk != "" {
			sqlproc.ExactPkFilter = exactPk
		}
		return
	}

	// Build the doc_id pushdown filter directly from the unique join keys.
	// docfilter picks the structure: an exact bitset (cbitmap / CRoaring) for
	// integer PKs — no false positives — or a CBloomFilter otherwise. The
	// reader's docfilter.New reconstructs it from the tag.
	payload, err := docfilter.Build(keyvec)
	if err != nil {
		return err
	}
	sqlproc.IvfMembershipFilter = payload
	return nil
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

	err = idx.getBloomFilter(sqlproc)
	if err != nil {
		return
	}

	var sql string
	// Encode query vector as base64 of raw bytes — ~22x faster and ~48% smaller
	// than text format [0.123, ...]. Uses vecf32_from_base64/vecf64_from_base64
	// to decode back to the vector type inside the SQL engine.
	queryB64 := types.ArrayToBase64(query)
	var vecFromB64Fn string
	switch any(query).(type) {
	case []float32:
		vecFromB64Fn = "vecf32_from_base64"
	case []float64:
		vecFromB64Fn = "vecf64_from_base64"
	}

	if sqlproc != nil && sqlproc.ExactPkFilter != "" {
		// Exact PK path: WaitUniqueJoinKeys converted small key set into ExactPkFilter.
		// Query entries directly by pk list, skip centroid-based filtering.
		sql = fmt.Sprintf(
			"SELECT `%s`, %s(`%s`, %s('%s')) as vec_dist FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s) ORDER BY vec_dist LIMIT %d",
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			vecFromB64Fn,
			queryB64,
			tblcfg.DbName, tblcfg.EntriesTable,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			idx.Version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			sqlproc.ExactPkFilter,
			rt.Limit,
		)
	} else {
		// Standard centroid-based path with optional CBloomFilter pre-filtering.
		sql = fmt.Sprintf(
			"SELECT `%s`, %s(`%s`, %s('%s')) as vec_dist FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s) ORDER BY vec_dist LIMIT %d",
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			vecFromB64Fn,
			queryB64,
			tblcfg.DbName, tblcfg.EntriesTable,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			idx.Version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			instr,
			rt.Limit,
		)
	}

	//fmt.Println("IVFFlat SQL: ", sql)
	//os.Stderr.WriteString(sql)
	//os.Stderr.WriteString("\n")

	res, err := runSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	if len(rt.BackgroundQueries) > 0 {
		rt.BackgroundQueries[0] = res.LogicalPlan
	}

	distances = make([]float64, 0, rt.Limit)
	resid := make([]any, 0, rt.Limit)

	if len(res.Batches) == 0 {
		return resid, distances, nil
	}

	var rowCount int64
	for _, bat := range res.Batches {
		rowCount += int64(bat.RowCount())
		distVec := bat.Vecs[1]
		pkVec := bat.Vecs[0]

		for i := 0; i < bat.RowCount(); i++ {
			if distVec.IsNull(uint64(i)) {
				continue
			}

			pk := vector.GetAny(pkVec, i, true)
			resid = append(resid, pk)

			dist := vector.GetFixedAtNoTypeCheck[float64](distVec, i)
			dist = metric.DistanceTransformIvfflat(dist, metric.DistFuncNameToMetricType[rt.OrigFuncName], metric.MetricType(idxcfg.Ivfflat.Metric))
			distances = append(distances, dist)
		}
	}

	return resid, distances, nil
}

func (idx *IvfflatSearchIndex[T]) Destroy() {
	if idx.Centroids != nil {
		idx.Centroids.Destroy()
		idx.Centroids = nil
	}
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
func (s *IvfflatSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, dists, err := s.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	switch ks := keys.(type) {
	case []int64:
		copy(outKeys, ks)
	case []any:
		for i, k := range ks {
			outKeys[i] = k.(int64)
		}
	default:
		return moerr.NewInternalErrorNoCtx("IvfSearch: unknown keys type")
	}
	for i, d := range dists {
		outDists[i] = float32(d)
	}
	return nil
}

func (s *IvfflatSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
