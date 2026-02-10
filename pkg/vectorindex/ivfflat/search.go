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
	"math/rand/v2"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const bfProbability = 0.001

// exactPkFilterThreshold controls when WaitBloomFilter converts the received
// unique join keys into an exact "pk IN (...)" filter instead of building a
// bloom filter. For very small PK sets, bloom filter false positives interact
// poorly with centroid pruning in IVF pre mode. Keeping this threshold small
// avoids overly large IN lists while preserving the bloom filter performance
// path for larger sets. Adjust this number if future workloads show a better cutoff.
const exactPkFilterThreshold = 100

var runSql = sqlexec.RunSql

// Ivf search index struct to hold the usearch index
type IvfflatSearchIndex[T types.RealNumbers] struct {
	Version      int64
	Centroids    cache.VectorIndexSearchIf
	BloomFilters []*bloomfilter.CBloomFilter
	Meta         IvfflatMeta
}

// This is the Ivf search implementation that implement VectorIndexSearchIf interface
type IvfflatSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Index         *IvfflatSearchIndex[T]
	ThreadsSearch int64
}

type IvfflatMeta struct {
	CenterStats          map[int64]int64
	Nbits                uint64
	K                    uint32
	Seed                 uint64
	SmallCenterThreshold int64
}

// LoadStats get the number of entries per centroid
func (idx *IvfflatSearchIndex[T]) LoadStats(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64) error {

	idx.Meta.SmallCenterThreshold = int64(0)
	if sqlproc.GetResolveVariableFunc() != nil {
		val, err := sqlproc.GetResolveVariableFunc()("ivf_small_centroid_threshold", true, false)
		if err != nil {
			return err
		}
		idx.Meta.SmallCenterThreshold = val.(int64)
	}

	stats := make(map[int64]int64)

	sql := fmt.Sprintf("SELECT `%s`, COUNT(`%s`) FROM `%s`.`%s` WHERE `%s` = %d GROUP BY `%s`",
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		tblcfg.DbName, tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		idx.Version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
	)

	res, err := runSql(sqlproc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	for _, bat := range res.Batches {
		cntvec := bat.Vecs[1]
		idvec := bat.Vecs[0]

		for i := 0; i < bat.RowCount(); i++ {
			cid := vector.GetFixedAtNoTypeCheck[int64](idvec, i)
			cnt := vector.GetFixedAtNoTypeCheck[int64](cntvec, i)
			stats[cid] = cnt
		}
	}

	idx.Meta.CenterStats = stats
	return nil
}

// load all entries primary key per centroid and build bloomfilter per centroids
// make sure bloomfilters MUST share the same nbits, k and seed so that
// they can be merged together to form centroid bloomfilter
func (idx *IvfflatSearchIndex[T]) LoadBloomFilters(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64) (err error) {

	if idx.Centroids == nil {
		// no centroid
		return
	}

	// calculate the row count for bloomfilter
	if idx.Meta.CenterStats == nil {
		// no stats
		return
	}

	maxv := int64(0)
	for _, v := range idx.Meta.CenterStats {
		if v > maxv {
			maxv = v
		}
	}

	if maxv == 0 {
		// no entries found
		return
	}

	nprobe := int64(5)
	if sqlproc.GetResolveVariableFunc() != nil {
		val, err := sqlproc.GetResolveVariableFunc()("probe_limit", true, false)
		if err != nil {
			return err
		}
		nprobe = val.(int64)
	}

	// set the size of bloomfilter to max centroid size * probe so that the final
	// centroid bloomfilter have enough room after merge with nprobe centroids
	idx.Meta.Nbits, idx.Meta.K = bloomfilter.ComputeMemAndHashCountC(maxv*nprobe, bfProbability)
	idx.Meta.Seed = rand.Uint64()

	bloomfilters := make([]*bloomfilter.CBloomFilter, idxcfg.Ivfflat.Lists)
	for i := 0; i < int(idxcfg.Ivfflat.Lists); i++ {
		bf := bloomfilter.NewCBloomFilterWithSeed(idx.Meta.Nbits, idx.Meta.K, idx.Meta.Seed)
		bloomfilters[i] = bf
	}
	idx.BloomFilters = bloomfilters

	defer func() {
		if err != nil {
			if idx.BloomFilters != nil {
				for i := range idx.BloomFilters {
					if idx.BloomFilters[i] != nil {
						idx.BloomFilters[i].Free()
					}
				}
				idx.BloomFilters = nil
			}
		}
	}()

	for i := 0; i < int(idxcfg.Ivfflat.Lists); i++ {
		err = func() error {
			bf := bloomfilters[i]
			sql := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` = %d AND `%s` = %d",
				catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
				tblcfg.DbName, tblcfg.EntriesTable,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
				idx.Version,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
				i,
			)

			res, err2 := runSql(sqlproc, sql)
			if err2 != nil {
				return err2
			}
			defer res.Close()

			for _, bat := range res.Batches {
				bf.AddVector(bat.Vecs[0])
			}
			return nil
		}()

		if err != nil {
			return
		}
	}

	return
}

func (idx *IvfflatSearchIndex[T]) LoadCentroids(proc *sqlexec.SqlProcess, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

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

	bfidx, err := brute_force.NewBruteForceIndex[T](centroids, idxcfg.Ivfflat.Dimensions, metric.MetricType(idxcfg.Ivfflat.Metric), uint(elemsz))
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

	// load stats
	err = idx.LoadStats(proc, idxcfg, tblcfg, nthread)
	if err != nil {
		return err
	}

	err = idx.LoadCentroids(proc, idxcfg, tblcfg, nthread)
	if err != nil {
		return err
	}

	if proc.GetResolveVariableFunc() != nil {
		val, err := proc.GetResolveVariableFunc()("ivf_preload_entries", true, false)
		if err != nil {
			return err
		}

		preload := val.(int8)
		if preload != 0 {
			err = idx.LoadBloomFilters(proc, idxcfg, tblcfg, nthread)
			if err != nil {
				return err
			}
		}
	}
	//os.Stderr.WriteString(fmt.Sprintf("%d centroids loaded... lists = %d, centroid %v\n", len(idx.Centroids), idxcfg.Ivfflat.Lists, idx.Centroids))
	return nil
}

func (idx *IvfflatSearchIndex[T]) getCentroidsSum(centroids_ids []int64) uint64 {
	total := uint64(0)

	if idx.Meta.CenterStats == nil {
		return total
	}

	for _, k := range centroids_ids {
		cnt, ok := idx.Meta.CenterStats[k]
		if ok {
			total += uint64(cnt)
		}
	}
	return total
}

// merge the small centroids
func (idx *IvfflatSearchIndex[T]) findMergedCentroids(sqlproc *sqlexec.SqlProcess, centroids_ids []int64, idxcfg vectorindex.IndexConfig, probe uint) ([]int64, error) {
	n := 0
	nprobe := uint(0)

	for _, k := range centroids_ids {
		n++
		nprobe++
		cnt, ok := idx.Meta.CenterStats[k]
		if ok && cnt < idx.Meta.SmallCenterThreshold {
			nprobe--
		}
		if nprobe == probe {
			break
		}

	}
	return centroids_ids[:n], nil
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
	if idx.Meta.CenterStats != nil && idx.Meta.SmallCenterThreshold > 0 {
		rtprobe = probe * 2
		if rtprobe > idxcfg.Ivfflat.Lists {
			rtprobe = idxcfg.Ivfflat.Lists
		}
	}

	queries := [][]T{query}
	rt := vectorindex.RuntimeConfig{Limit: rtprobe, NThreads: 1}
	keys, _, err := idx.Centroids.Search(sqlproc, queries, rt)
	if err != nil {
		return nil, err
	}

	if idx.Meta.CenterStats != nil && idx.Meta.SmallCenterThreshold > 0 {
		return idx.findMergedCentroids(sqlproc, keys.([]int64), idxcfg, probe)
	}
	return keys.([]int64), nil
}

/*
prepare runtime bloomfilter for pre-filtering
Centroids are C1, C2,...CN (Lists)
Preload centroid bloomfilter are BF1, BF2, ..., BFN
Selected centroids lists is [C1, C2,..., Cj]

1.   get the unique join keys from hashbuild
2.1  if cache centroid is nil then
2.2     build bloomfilter with unique join keys and return
2.3  endif
3.1  if there is no pre-loaded centroid bloomfilters then
3.2.    get the entries primary keys from selected centroids by SQL (Slow)
3.3.    build the centroid bloomfilter (CBJ) with the entries primary keys
3.4. else
3.5.    generate the centroid bloomfilter (CBJ) by merge the centroid bloomfilter with bitmap_pr(BF1, BF2,.., BFj)  (Very fast)
3.5. end
4.   Test the unqiue join keys with CBJ. i.e. UniqueJoinKeys JOIN Selected Centroids
5.   Build the final bloomfilter with the exists key from 3.
*/
func (idx *IvfflatSearchIndex[T]) getBloomFilter(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	centroids_ids []int64) (err error) {

	if sqlproc.Proc == nil {
		return
	}

	if len(sqlproc.RuntimeFilterSpecs) == 0 {
		return
	}
	spec := sqlproc.RuntimeFilterSpecs[0]
	if !spec.UseBloomFilter {
		return
	}

	// Get raw unique join key bytes from the build side.
	vecbytes, err := sqlexec.WaitBloomFilter(sqlproc)
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

	// Small PK set: build an exact "pk IN (...)" filter instead of bloom filter.
	// This avoids bloom filter false positives that interact poorly with centroid
	// pruning in IVF pre mode.
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

	buildBloomFilterWithUniqueJoinKeys := func(vec *vector.Vector) error {
		// sometimes user create index with empty data and lead to have single NIL centroid
		// all entries will have centroid_id = 1. i.e. whole table scan
		// In this case, build bloomfilter with unique join keys

		ukeybf := bloomfilter.NewCBloomFilterWithProbability(int64(vec.Length()), bfProbability)
		defer ukeybf.Free()
		ukeybf.AddVector(vec)

		ukeybfbytes, err := ukeybf.Marshal()
		if err != nil {
			return err
		}
		sqlproc.BloomFilter = ukeybfbytes
		return nil
	}

	if idx.Centroids == nil {
		return buildBloomFilterWithUniqueJoinKeys(keyvec)
	}

	var bf *bloomfilter.CBloomFilter
	defer func() {
		if bf != nil {
			bf.Free()
		}
	}()

	if len(idx.BloomFilters) == 0 {

		sum := idx.getCentroidsSum(centroids_ids)
		if uint64(keyvec.Length()) < sum {
			// unique join keys size is smaller than entries in centroids
			return buildBloomFilterWithUniqueJoinKeys(keyvec)
		}

		// get centroid ids on the fly
		var instr string
		for i, c := range centroids_ids {
			if i > 0 {
				instr += ","
			}
			instr += strconv.FormatInt(c, 10)
		}

		sql := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			tblcfg.DbName, tblcfg.EntriesTable,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			idx.Version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			instr,
		)

		//os.Stderr.WriteString(sql)
		//os.Stderr.WriteString("\n")

		res, err := runSql(sqlproc, sql)
		if err != nil {
			return err
		}
		defer res.Close()

		if len(res.Batches) == 0 {
			return nil
		}

		var rowCount int64
		for _, bat := range res.Batches {
			rowCount += int64(bat.RowCount())
		}

		bf = bloomfilter.NewCBloomFilterWithProbability(rowCount, bfProbability)
		for _, bat := range res.Batches {
			bf.AddVector(bat.Vecs[0])
		}

	} else {
		// use preload bloomfilter
		bf = bloomfilter.NewCBloomFilterWithSeed(idx.Meta.Nbits, idx.Meta.K, idx.Meta.Seed)
		for _, c := range centroids_ids {
			err = bf.Merge(idx.BloomFilters[c])
			if err != nil {
				return
			}
		}
	}

	exists := bf.TestVector(keyvec, nil)

	if len(exists) != keyvec.Length() {
		return moerr.NewInternalError(sqlproc.GetContext(), "result from bloomfilter size not match with input key vector")
	}

	nexist := 0
	for _, e := range exists {
		if e != 0 {
			nexist++
		}
	}

	bf2 := bloomfilter.NewCBloomFilterWithProbability(int64(nexist), bfProbability)
	defer func() {
		if bf2 != nil {
			bf2.Free()
		}
	}()

	// Add filtered key to bloomfilter
	if nexist > 0 {
		for i, e := range exists {
			if e != 0 {
				bf2.Add(keyvec.GetRawBytesAt(i))
			}
		}
	}
	bfbytes, err := bf2.Marshal()
	if err != nil {
		return err
	}
	sqlproc.BloomFilter = bfbytes

	//os.Stderr.WriteString(fmt.Sprintf("IVF BloomFilter Build: #Entries for selected centers =  %d, #UniqueKey = %d, #ExistAfterFilter = %d,  Built Time = %v\n", rowCount, keyvec.Length(), nexist, elapsed))

	return
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

	err = idx.getBloomFilter(sqlproc, idxcfg, tblcfg, centroids_ids)
	if err != nil {
		return
	}

	var sql string
	if sqlproc != nil && sqlproc.ExactPkFilter != "" {
		// Exact PK path: WaitBloomFilter converted small key set into ExactPkFilter.
		// Query entries directly by pk list, skip centroid-based filtering.
		sql = fmt.Sprintf(
			"SELECT `%s`, %s(`%s`, '%s') as vec_dist FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			types.ArrayToString(query),
			tblcfg.DbName, tblcfg.EntriesTable,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			idx.Version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			sqlproc.ExactPkFilter,
		)
	} else {
		// Standard centroid-based path with optional CBloomFilter pre-filtering.
		sql = fmt.Sprintf(
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
	}

	//fmt.Println("IVFFlat SQL: ", sql)
	//os.Stderr.WriteString(sql)
	//os.Stderr.WriteString("\n")

	res, err := runSql(sqlproc, sql)
	if err != nil {
		return
	}

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

	res.Close()

	return resid, distances, nil
}

func (idx *IvfflatSearchIndex[T]) Destroy() {
	if idx.Centroids != nil {
		idx.Centroids.Destroy()
		idx.Centroids = nil
	}

	if idx.BloomFilters != nil {
		for _, bf := range idx.BloomFilters {
			if bf != nil {
				bf.Free()
			}
		}
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
func (s *IvfflatSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
