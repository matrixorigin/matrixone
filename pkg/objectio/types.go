// Copyright 2021 Matrix Origin
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

package objectio

import (
	"context"
	"math/bits"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type WriteType int8

const (
	WriteTS WriteType = iota
)

type ZoneMap = index.ZM
type StaticFilter = index.StaticFilter

var NewZM = index.NewZM
var BuildZM = index.BuildZM

type ColumnMetaFetcher interface {
	MustGetColumn(seqnum uint16) ColumnMeta
}

type ReadFilterSearchFuncType func(containers.Vectors) []int64

type BlockReadFilter struct {
	HasFakePK          bool
	Valid              bool
	SortedSearchFunc   ReadFilterSearchFuncType
	UnSortedSearchFunc ReadFilterSearchFuncType
	Cleanup            func() // Cleanup function to release resources (e.g., reusableTempVec)
}

func (f BlockReadFilter) DecideSearchFunc(isSortedBlk bool) ReadFilterSearchFuncType {
	if (f.HasFakePK || !isSortedBlk) && f.UnSortedSearchFunc != nil {
		return f.UnSortedSearchFunc
	}

	if isSortedBlk && f.SortedSearchFunc != nil {
		return f.SortedSearchFunc
	}

	return nil
}

type Float64Heap []float64

func (h Float64Heap) Len() int           { return len(h) }
func (h Float64Heap) Less(i, j int) bool { return h[i] > h[j] }
func (h Float64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Float64Heap) Push(x any) {
	*h = append(*h, x.(float64))
}

func (h *Float64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type IndexReaderTopOp struct {
	Typ          types.T
	MetricType   metric.MetricType
	ColPos       int32
	NumVec       []byte
	Limit        uint64
	OrderedLimit bool
	Desc         bool

	LowerBoundType plan.BoundType
	UpperBoundType plan.BoundType
	LowerBound     float64
	UpperBound     float64

	DistHeap Float64Heap

	// --- Reader-driven IVF widening (ivfflat_widening prototype) ---
	// When RankedCentroids is non-empty the entries-table scan is an IVF
	// widening search: blocks are visited in centroid-rank order (nearest
	// centroid first) and the scan early-stops once the top-K heap is full and
	// the nearest nprobe centroids have been covered. See ShouldStopWidening.
	// This mirrors the original multi-round IVF search ("probe nprobe buckets,
	// fill K, widen only if under-filled") in a single scan -- approximate, no
	// exact distance bound (IVF is approximate anyway).
	//
	// RankedCentroids holds centroid ids ordered nearest->farthest from the query.
	RankedCentroids []int64
	CentroidColPos  int32 // position of the centroid_id column in the scan
	Nprobe          int   // base probe budget (centroid batch size); widen past it only while under-filled
}

// WideningEnabled reports whether reader-driven IVF widening is active for this
// scan (i.e. the planner supplied a ranked centroid list).
func (op *IndexReaderTopOp) WideningEnabled() bool {
	return op != nil && len(op.RankedCentroids) > 0
}

// ShouldStopWidening reports whether the rank-ordered, batched bucket walk can
// stop before yielding the next block. nextBlockRank is the best (nearest)
// centroid rank that block covers (see IvfWideningParam.BlockBestRank).
//
// It stops when both hold:
//  1. the top-K heap is full (we already have K candidates), and
//  2. the next block's nearest centroid is beyond the base nprobe batch
//     (nextBlockRank >= Nprobe) -- i.e. the nearest nprobe buckets are covered.
//
// This is the approximate "fill K then stop" rule of the original multi-round
// IVF search: probe at least the nearest nprobe buckets; if a selective filter
// left the heap under-filled, keep widening into farther buckets until it fills.
// No exact distance/radius bound -- recall matches standard nprobe IVF.
func (op *IndexReaderTopOp) ShouldStopWidening(nextBlockRank int) bool {
	if int(op.Limit) <= 0 || len(op.DistHeap) < int(op.Limit) {
		return false // heap not yet full to K -> keep widening
	}
	return nextBlockRank >= op.Nprobe
}

// IvfWideningParam is the side-channel payload carrying reader-driven IVF
// widening parameters from ivf_search (Go runtime) down to the entries-table
// scan, attached to the internal-SQL context under defines.IvfWidening{}.
// It avoids a plan.IndexReaderParam (proto) change for these runtime values.
type IvfWideningParam struct {
	// RankedCentroids: centroid ids ordered nearest->farthest from the query.
	RankedCentroids []int64
	// Nprobe: base probe budget (centroid batch size). Widen past it only while
	// the top-K heap is under-filled (e.g. a selective filter emptied the near
	// buckets).
	Nprobe int
	// CentroidColName: the entries centroid_id column, used to locate its
	// per-block zonemap when ordering blocks by centroid rank.
	CentroidColName string
}

// How "sort blocks by centroid rank" works -- a worked example.
//
// The input is RankedCentroids: centroid ids ordered nearest->farthest from the
// query. Suppose 8 centroids sit at these 1-D positions and the query q is at 88
// (distance is |pos-88|; real vectors are multi-dim, the idea is identical):
//
//	centroid id:  1    2    3    4    5    6    7    8
//	position:     10   2    50   80   1    60   90   6
//	dist to q=88: 78   86   38   8    87   28   2    82
//
// Sorting centroid ids by that distance (nearest first) gives:
//
//	RankedCentroids = [7, 4, 6, 3, 1, 8, 2, 5]   // id 7 is nearest, id 5 farthest
//
// CentroidRank() inverts this into id -> rank ("which place did this id come"):
//
//	{7:0, 4:1, 6:2, 3:3, 1:4, 8:5, 2:6, 5:7}     // id 7 = rank 0 (nearest)
//
// The entries table is physically sorted by centroid_id, so each block holds a
// CONTIGUOUS id range (its centroid_id zonemap [min,max]). Three blocks:
//
//	block A: ids 1,2   -> ranks {4,6}     -> BlockBestRank = 4
//	block B: ids 3,4,5 -> ranks {3,1,7}   -> BlockBestRank = 1
//	block C: ids 6,7,8 -> ranks {2,0,5}   -> BlockBestRank = 0
//
// BlockBestRank = the smallest (nearest) rank a block can contain. Sorting the
// blocks by it gives the visit order C(0), B(1), A(4) -- block C first because it
// holds the nearest centroid (id 7), even though it is physically last on disk.
// Note the centroid ids are never sorted; they are only lookup keys into the
// rank map. The reader walks blocks in this order and early-stops (batched,
// approximate) once the top-K heap is full and the next block's nearest centroid
// is beyond the base nprobe batch -- the same "fill K then stop" behavior as the
// original multi-round IVF search.

// CentroidRank returns a map from centroid id to its rank index (0 = nearest).
// e.g. RankedCentroids [7,4,6,...] -> {7:0, 4:1, 6:2, ...}.
func (p *IvfWideningParam) CentroidRank() map[int64]int {
	m := make(map[int64]int, len(p.RankedCentroids))
	for i, c := range p.RankedCentroids {
		m[c] = i
	}
	return m
}

// BlockBestRank returns the best (smallest) centroid rank among centroid ids in
// the inclusive range [minCID, maxCID] (a block's centroid_id zonemap), or
// len(RankedCentroids) if none of the block's centroids are ranked (sorts last).
// Because the entries table is clustered by centroid_id, ordering blocks by this
// value yields a rank-ordered (nearest-centroid-first) block walk.
//
// Example (see the block table above): a block with zonemap [6,8] and rank map
// {7:0,4:1,6:2,3:3,1:4,8:5,2:6,5:7} contains ids 6,7,8 -> ranks 2,0,5 -> returns 0.
func (p *IvfWideningParam) BlockBestRank(minCID, maxCID int64, rank map[int64]int) int {
	best := len(p.RankedCentroids)
	for c, r := range rank {
		if c >= minCID && c <= maxCID && r < best {
			best = r
		}
	}
	return best
}

// CentroidRankIndex is the O(log L)-query form of BlockBestRank for the hot path
// (the per-block loop in the entries scan). The naive BlockBestRank scans the
// whole rank map per block -> O(B*L), which at scale (e.g. 88M rows: ~10k blocks
// x several-k centroids) is 10M-100M map probes = 0.3-3s per query. This builds,
// once, the centroids sorted by id plus a sparse table for range-min over their
// ranks, so each block's best rank is a couple of binary searches + an O(1)
// range-min: total O(L log L) build + O(B log L) queries (sub-ms at 88M).
type CentroidRankIndex struct {
	ids    []int64 // centroid ids, ascending
	sparse [][]int // sparse[k][i] = min rank over ranks[i : i+2^k]
	nFar   int     // = len(RankedCentroids): "no ranked centroid in range" sentinel (sorts last)
}

// NewCentroidRankIndex builds the range-min index from RankedCentroids. Returns
// nil if there are no centroids (caller should fall back / treat as no-widening).
func (p *IvfWideningParam) NewCentroidRankIndex() *CentroidRankIndex {
	n := len(p.RankedCentroids)
	if n == 0 {
		return nil
	}
	type cr struct {
		id   int64
		rank int
	}
	arr := make([]cr, n)
	for i, c := range p.RankedCentroids {
		arr[i] = cr{id: c, rank: i} // rank == position in RankedCentroids (0 = nearest)
	}
	sort.Slice(arr, func(a, b int) bool { return arr[a].id < arr[b].id })
	ids := make([]int64, n)
	ranks := make([]int, n)
	for i := range arr {
		ids[i] = arr[i].id
		ranks[i] = arr[i].rank
	}
	K := bits.Len(uint(n)) // levels: 2^(K-1) <= n
	sparse := make([][]int, K)
	sparse[0] = ranks
	for k := 1; k < K; k++ {
		span := 1 << k
		size := n - span + 1
		sparse[k] = make([]int, size)
		for i := 0; i < size; i++ {
			sparse[k][i] = min(sparse[k-1][i], sparse[k-1][i+(span>>1)])
		}
	}
	return &CentroidRankIndex{ids: ids, sparse: sparse, nFar: n}
}

// BestRank returns the smallest centroid rank among ids in [minCID, maxCID], or
// nFar (= len(RankedCentroids)) if none -- identical result to BlockBestRank, in
// O(log L). The entries table is clustered by centroid_id so a block's zonemap
// is a contiguous [min,max] range; ids are sorted, so that range maps to a
// contiguous slice [lo,hi) over which we take the range-min of ranks.
func (idx *CentroidRankIndex) BestRank(minCID, maxCID int64) int {
	lo := sort.Search(len(idx.ids), func(i int) bool { return idx.ids[i] >= minCID })
	hi := sort.Search(len(idx.ids), func(i int) bool { return idx.ids[i] > maxCID })
	if lo >= hi {
		return idx.nFar
	}
	k := bits.Len(uint(hi-lo)) - 1 // largest power of two <= (hi-lo)
	return min(idx.sparse[k][lo], idx.sparse[k][hi-(1<<k)])
}

type WriteOptions struct {
	Type WriteType
	Val  any
}

type ReadBlockOptions struct {
	Id       uint16
	DataType uint16
	Idxes    map[uint16]bool
}

// Writer is to virtualize batches into multiple blocks
// and write them into filefservice at one time
type Writer interface {
	// Write writes one batch to the Buffer at a time,
	// one batch corresponds to a virtual block,
	// and returns the handle of the block.
	Write(batch *batch.Batch) (BlockObject, error)

	// Write metadata for every column of all blocks
	WriteObjectMeta(ctx context.Context, totalRow uint32, metas []ColumnMeta)

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error)
}

// Reader is to read data from fileservice
type Reader interface {
	// Read is to read columns data of a block from fileservice at one time
	// extent is location of the block meta
	// idxs is the column serial number of the data to be read
	Read(ctx context.Context,
		extent *Extent, idxs []uint16,
		id uint32,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	ReadAll(
		ctx context.Context,
		extent *Extent,
		idxs []uint16,
		m *mpool.MPool,
		readFunc CacheConstructorFactory,
	) (*fileservice.IOVector, error)

	ReadBlocks(ctx context.Context,
		extent *Extent,
		ids map[uint32]*ReadBlockOptions,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	// ReadMeta is the meta that reads a block
	// extent is location of the block meta
	ReadMeta(ctx context.Context, extent *Extent, m *mpool.MPool) (ObjectDataMeta, error)

	// ReadAllMeta is read the meta of all blocks in an object
	ReadAllMeta(ctx context.Context, m *mpool.MPool) (ObjectDataMeta, error)

	GetObject() *Object
}
