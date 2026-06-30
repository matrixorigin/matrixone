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
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

func (idx *IvfflatSearchIndex[T]) rankCentroids(sqlproc *sqlexec.SqlProcess, query []T, idxcfg vectorindex.IndexConfig) ([]int64, error) {
	if idx.Centroids == nil {
		// empty index has id = 1
		return []int64{1}, nil
	}

	limit := idxcfg.Ivfflat.Lists
	if limit == 0 {
		limit = 1
	}
	queries := [][]T{query}
	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}
	keys, _, err := idx.Centroids.Search(sqlproc, queries, rt)
	if err != nil {
		return nil, err
	}

	ranked, ok := keys.([]int64)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("ivfflat: ranked centroid ids are not []int64")
	}
	return ranked, nil
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

	if sqlproc == nil || sqlproc.Proc == nil {
		return
	}

	if len(sqlproc.IvfRuntimeFilterData) == 0 {
		if len(sqlproc.RuntimeFilterSpecs) == 0 {
			return
		}
		spec := sqlproc.RuntimeFilterSpecs[0]
		if !spec.UseMembershipFilter {
			return
		}
	}

	sqlproc.ExactPkFilter = ""
	sqlproc.IvfMembershipFilter = nil

	var vecbytes []byte
	if len(sqlproc.IvfRuntimeFilterData) > 0 {
		vecbytes = sqlproc.IvfRuntimeFilterData
	} else {
		vecbytes, err = sqlexec.WaitUniqueJoinKeys(sqlproc)
		if err != nil {
			return
		}
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
	// No keyvec.Free here on purpose: UnmarshalBinary aliases vecbytes (it sets
	// cantFreeData/cantFreeArea), so keyvec owns no mpool memory — the struct and
	// the aliased bytes are reclaimed by GC. Calling Free(mp) would be a no-op for
	// this zero-copy path, and tying its release to a specific mpool would be a
	// cross-pool free hazard if the deserialization ever became owning.

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

func filterRequestedIncludeColumns(requested []string, configured []string) []string {
	if len(requested) == 0 || len(configured) == 0 {
		return nil
	}

	allowed := make(map[string]struct{}, len(configured))
	for _, col := range configured {
		allowed[col] = struct{}{}
	}

	filtered := make([]string, 0, len(requested))
	for _, col := range requested {
		if _, ok := allowed[col]; ok {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

func buildActiveCentroidIDs(cursor *vectorindex.IvfSearchCursor, probe uint) []int64 {
	if cursor == nil {
		return nil
	}

	total := uint(len(cursor.RankedCentroidIDs))
	if total == 0 {
		cursor.Exhausted = true
		return nil
	}

	if cursor.Round == 0 && cursor.CurrentBucketCount == 0 {
		cursor.NextBucketOffset = 0
		cursor.CurrentBucketCount = probe
		if cursor.CurrentBucketCount == 0 {
			cursor.CurrentBucketCount = 1
		}
	}

	start := cursor.NextBucketOffset
	if start >= total || cursor.CurrentBucketCount == 0 {
		cursor.Exhausted = true
		return nil
	}

	end := start + cursor.CurrentBucketCount
	if end > total {
		end = total
	}
	cursor.CurrentBucketCount = end - start
	cursor.Exhausted = end >= total

	return cursor.RankedCentroidIDs[start:end]
}

func buildSearchRoundSQL[T types.RealNumbers](
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	activeCentroidIDs []int64,
	version int64,
	includeCols []string,
	pushdownFilterSQL string,
	roundLimit uint,
) string {
	inValues := make([]string, 0, len(activeCentroidIDs))
	for _, c := range activeCentroidIDs {
		inValues = append(inValues, strconv.FormatInt(c, 10))
	}

	queryB64 := types.ArrayToBase64(query)
	var distExpr string
	switch any(query).(type) {
	case []float32:
		distExpr = fmt.Sprintf("%s(`%s`, vecf32_from_base64('%s')) as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			queryB64,
		)
	case []float64:
		distExpr = fmt.Sprintf("%s(`%s`, vecf64_from_base64('%s')) as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			queryB64,
		)
	default:
		distExpr = fmt.Sprintf("%s(`%s`, '%s') as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			types.ArrayToString(query),
		)
	}

	selectCols := []string{
		fmt.Sprintf("`%s`", catalog.SystemSI_IVFFLAT_TblCol_Entries_pk),
		distExpr,
	}
	for _, col := range includeCols {
		selectCols = append(selectCols, fmt.Sprintf("`%s%s`", catalog.SystemSI_IVFFLAT_IncludeColPrefix, col))
	}

	sql := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
		strings.Join(selectCols, ", "),
		tblcfg.DbName,
		tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		strings.Join(inValues, ","),
	)
	if pushdownFilterSQL != "" {
		// pushdownFilterSQL is produced by the optimizer's AST deparse path after
		// column remap and validation, so this concatenation only stitches in
		// controlled SQL emitted from bound plan expressions.
		sql += " AND " + pushdownFilterSQL
	}
	sql += fmt.Sprintf(" ORDER BY vec_dist LIMIT %d", roundLimit)

	return sql
}

func buildExactSearchSQL[T types.RealNumbers](
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	version int64,
	exactPkFilter string,
	includeCols []string,
	pushdownFilterSQL string,
) string {
	queryB64 := types.ArrayToBase64(query)
	var distExpr string
	switch any(query).(type) {
	case []float32:
		distExpr = fmt.Sprintf("%s(`%s`, vecf32_from_base64('%s')) as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			queryB64,
		)
	case []float64:
		distExpr = fmt.Sprintf("%s(`%s`, vecf64_from_base64('%s')) as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			queryB64,
		)
	default:
		distExpr = fmt.Sprintf("%s(`%s`, '%s') as vec_dist",
			metric.MetricTypeToDistFuncName[metric.MetricType(idxcfg.Ivfflat.Metric)],
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			types.ArrayToString(query),
		)
	}

	selectCols := []string{
		fmt.Sprintf("`%s`", catalog.SystemSI_IVFFLAT_TblCol_Entries_pk),
		distExpr,
	}
	for _, col := range includeCols {
		selectCols = append(selectCols, fmt.Sprintf("`%s%s`", catalog.SystemSI_IVFFLAT_IncludeColPrefix, col))
	}

	sql := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
		strings.Join(selectCols, ", "),
		tblcfg.DbName,
		tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		exactPkFilter,
	)
	if pushdownFilterSQL != "" {
		sql += " AND " + pushdownFilterSQL
	}
	return sql
}

func sortAndLimitExactResults(
	keys []any,
	distances []float64,
	includeCols []string,
	includeData map[string][]any,
	includeNulls map[string][]bool,
	limit uint,
) ([]any, []float64, map[string][]any, map[string][]bool) {
	if len(keys) <= 1 {
		if limit > 0 && len(keys) > int(limit) {
			keys = keys[:limit]
			distances = distances[:limit]
			if includeData != nil {
				for _, col := range includeCols {
					includeData[col] = includeData[col][:limit]
				}
			}
			if includeNulls != nil {
				for _, col := range includeCols {
					includeNulls[col] = includeNulls[col][:limit]
				}
			}
		}
		return keys, distances, includeData, includeNulls
	}

	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		return distances[order[i]] < distances[order[j]]
	})

	if limit == 0 || int(limit) > len(order) {
		limit = uint(len(order))
	}

	sortedKeys := make([]any, 0, limit)
	sortedDistances := make([]float64, 0, limit)
	var sortedInclude map[string][]any
	if includeData != nil {
		sortedInclude = make(map[string][]any, len(includeCols))
		for _, col := range includeCols {
			sortedInclude[col] = make([]any, 0, limit)
		}
	}
	var sortedNulls map[string][]bool
	if includeNulls != nil {
		sortedNulls = make(map[string][]bool, len(includeCols))
		for _, col := range includeCols {
			sortedNulls[col] = make([]bool, 0, limit)
		}
	}

	for _, idx := range order[:limit] {
		sortedKeys = append(sortedKeys, keys[idx])
		sortedDistances = append(sortedDistances, distances[idx])
		if sortedInclude != nil {
			for _, col := range includeCols {
				sortedInclude[col] = append(sortedInclude[col], includeData[col][idx])
			}
		}
		if sortedNulls != nil {
			for _, col := range includeCols {
				sortedNulls[col] = append(sortedNulls[col], includeNulls[col][idx])
			}
		}
	}

	return sortedKeys, sortedDistances, sortedInclude, sortedNulls
}

func exactResultLimit(sqlproc *sqlexec.SqlProcess, fallback uint) uint {
	limit := fallback
	if sqlproc == nil || sqlproc.IndexReaderParam == nil || sqlproc.IndexReaderParam.GetLimit() == nil {
		return limit
	}
	lit := sqlproc.IndexReaderParam.GetLimit().GetLit()
	if lit == nil {
		return limit
	}
	readerLimit := uint(lit.GetU64Val())
	if readerLimit > limit {
		limit = readerLimit
	}
	return limit
}

// Call usearch.Search
func (idx *IvfflatSearchIndex[T]) Search(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	query []T,
	rt vectorindex.RuntimeConfig,
	_ int64,
) (keys any, distances []float64, err error) {

	distfn, err := metric.ResolveDistanceFn[T](metric.MetricType(idxcfg.Ivfflat.Metric))
	if err != nil {
		return
	}

	if sqlproc != nil {
		prevRuntimeFilterData := sqlproc.IvfRuntimeFilterData
		prevMembershipFilter := sqlproc.IvfMembershipFilter
		prevExactPkFilter := sqlproc.ExactPkFilter
		sqlproc.IvfRuntimeFilterData = rt.RuntimeFilterData
		sqlproc.IvfMembershipFilter = nil
		sqlproc.ExactPkFilter = ""
		defer func() {
			sqlproc.IvfRuntimeFilterData = prevRuntimeFilterData
			sqlproc.IvfMembershipFilter = prevMembershipFilter
			sqlproc.ExactPkFilter = prevExactPkFilter
		}()
	}

	includeMode := rt.SearchCursor != nil ||
		rt.IncludeResult != nil ||
		len(rt.RequestedIncludeColumns) > 0 ||
		rt.PushdownFilterSQL != "" ||
		rt.SearchRoundLimit > 0

	if includeMode {
		cursor := rt.SearchCursor
		if cursor == nil {
			cursor = &vectorindex.IvfSearchCursor{}
		}
		if len(cursor.RankedCentroidIDs) == 0 {
			cursor.RankedCentroidIDs, err = idx.rankCentroids(sqlproc, query, idxcfg)
			if err != nil {
				return nil, nil, err
			}
		}

		activeCentroidIDs := buildActiveCentroidIDs(cursor, rt.Probe)
		if len(activeCentroidIDs) == 0 {
			return []any{}, []float64{}, nil
		}
		cursor.Round++

		roundLimit := rt.SearchRoundLimit
		if roundLimit == 0 {
			roundLimit = rt.Limit
		}
		if roundLimit == 0 {
			roundLimit = 1
		}

		includeCols := filterRequestedIncludeColumns(rt.RequestedIncludeColumns, tblcfg.IncludeColumns)
		if rt.IncludeResult != nil {
			rt.IncludeResult.ColNames = append(rt.IncludeResult.ColNames[:0], includeCols...)
			rt.IncludeResult.Data = make(map[string][]any, len(includeCols))
			rt.IncludeResult.Nulls = make(map[string][]bool, len(includeCols))
			for _, col := range includeCols {
				rt.IncludeResult.Data[col] = make([]any, 0, roundLimit)
				rt.IncludeResult.Nulls[col] = make([]bool, 0, roundLimit)
			}
		}

		if err = idx.getBloomFilter(sqlproc); err != nil {
			return nil, nil, err
		}

		sql := buildSearchRoundSQL(idxcfg, tblcfg, query, activeCentroidIDs, idx.Version, includeCols, rt.PushdownFilterSQL, roundLimit)
		if sqlproc != nil && sqlproc.ExactPkFilter != "" {
			sql = buildExactSearchSQL(
				idxcfg,
				tblcfg,
				query,
				idx.Version,
				sqlproc.ExactPkFilter,
				includeCols,
				rt.PushdownFilterSQL,
			)
			if cursor != nil {
				cursor.Exhausted = true
			}
		}

		res, runErr := runSql(sqlproc, sql)
		if runErr != nil {
			return nil, nil, runErr
		}
		defer res.Close()

		if len(rt.BackgroundQueries) > 0 {
			if len(res.LogicalPlan.Nodes) > 0 && len(res.LogicalPlan.Steps) > 0 {
				rootID := res.LogicalPlan.Steps[0]
				if int(rootID) < len(res.LogicalPlan.Nodes) && res.LogicalPlan.Nodes[rootID] != nil {
					if res.LogicalPlan.Nodes[rootID].Stats == nil {
						res.LogicalPlan.Nodes[rootID].Stats = &plan.Stats{}
					}
					res.LogicalPlan.Nodes[rootID].Stats.Sql = sql
				}
			}
			rt.BackgroundQueries[0] = res.LogicalPlan
		}

		if len(res.Batches) == 0 {
			return []any{}, []float64{}, nil
		}

		resultCap := 0
		for _, bat := range res.Batches {
			resultCap += bat.RowCount()
		}
		distances = make([]float64, 0, resultCap)
		resid := make([]any, 0, resultCap)
		for _, bat := range res.Batches {
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

				if rt.IncludeResult != nil {
					for j, col := range includeCols {
						includeVec := bat.Vecs[2+j]
						isNull := includeVec.IsNull(uint64(i))
						rt.IncludeResult.Nulls[col] = append(rt.IncludeResult.Nulls[col], isNull)
						if isNull {
							rt.IncludeResult.Data[col] = append(rt.IncludeResult.Data[col], nil)
							continue
						}
						rt.IncludeResult.Data[col] = append(rt.IncludeResult.Data[col], vector.GetAny(includeVec, i, true))
					}
				}
			}
		}

		if sqlproc != nil && sqlproc.ExactPkFilter != "" {
			var sortedInclude map[string][]any
			var sortedNulls map[string][]bool
			exactLimit := exactResultLimit(sqlproc, roundLimit)
			var includeData map[string][]any
			var includeNulls map[string][]bool
			if rt.IncludeResult != nil {
				includeData = rt.IncludeResult.Data
				includeNulls = rt.IncludeResult.Nulls
			}
			resid, distances, sortedInclude, sortedNulls = sortAndLimitExactResults(resid, distances, includeCols, includeData, includeNulls, exactLimit)
			if rt.IncludeResult != nil {
				rt.IncludeResult.Data = sortedInclude
				rt.IncludeResult.Nulls = sortedNulls
			}
		}

		return resid, distances, nil
	}

	centroidsIDs, err := idx.findCentroids(sqlproc, query, distfn, idxcfg, rt.Probe, 0)
	if err != nil {
		return
	}

	var instr string
	for i, c := range centroidsIDs {
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
		//
		// Do NOT add ORDER BY vec_dist / LIMIT here. Adding "ORDER BY vec_dist LIMIT k"
		// makes the planner push the sort+limit INTO this entries Table Scan (EXPLAIN
		// shows "Index Reader Param: Sort Key ... Limit: k" on the scan), which applies
		// the LIMIT *before* the "pk IN (...)" / prefix_eq filter -- i.e. it turns our
		// intended pre-filter into a POST-filter: it takes the global top-k by distance
		// over ALL entries, then keeps only the candidates, so fewer than k matching rows
		// survive (regressed vector_ivf_mode.sql). With no ORDER BY/LIMIT the scan stays
		// a plain filtered read that returns the full candidate set; the downstream
		// Node_SORT + LIMIT k does the ranking and truncation.
		sql = fmt.Sprintf(
			"SELECT `%s`, %s(`%s`, %s('%s')) as vec_dist FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
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
		)
	} else {
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

	res, err := runSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	if len(rt.BackgroundQueries) > 0 {
		if len(res.LogicalPlan.Nodes) > 0 && len(res.LogicalPlan.Steps) > 0 {
			rootID := res.LogicalPlan.Steps[0]
			if int(rootID) < len(res.LogicalPlan.Nodes) && res.LogicalPlan.Nodes[rootID] != nil {
				if res.LogicalPlan.Nodes[rootID].Stats == nil {
					res.LogicalPlan.Nodes[rootID].Stats = &plan.Stats{}
				}
				res.LogicalPlan.Nodes[rootID].Stats.Sql = sql
			}
		}
		rt.BackgroundQueries[0] = res.LogicalPlan
	}

	distances = make([]float64, 0, rt.Limit)
	resid := make([]any, 0, rt.Limit)
	if len(res.Batches) == 0 {
		return resid, distances, nil
	}

	for _, bat := range res.Batches {
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

	if sqlproc != nil && sqlproc.ExactPkFilter != "" {
		exactLimit := exactResultLimit(sqlproc, rt.Limit)
		resid, distances, _, _ = sortAndLimitExactResults(resid, distances, nil, nil, nil, exactLimit)
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
