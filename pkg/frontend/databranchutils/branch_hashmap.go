// Copyright 2025 Matrix Origin
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

package databranchutils

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
)

// BranchHashmap exposes the operations supported by the adaptive hashmap.
type BranchHashmap interface {
	// PutByVectors stores the provided vectors. Each row across the vectors is
	// treated as a single record and the columns referenced by keyCols build the key.
	PutByVectors(vecs []*vector.Vector, keyCols []int) error
	// GetByVectors probes the map using the supplied key vectors and returns one
	// GetResult per probed row, preserving the order of the original vectors. The
	// returned rows reference the internal storage and must be treated as read-only.
	GetByVectors(keyVecs []*vector.Vector) ([]GetResult, error)
	// GetByEncodedKey retrieves rows using a pre-encoded key such as one obtained
	// from ForEachShardParallel. Returned rows reference internal storage and must
	// be treated as read-only.
	GetByEncodedKey(encodedKey []byte) (GetResult, error)
	// PopByVectors behaves like GetByVectors but removes matching rows from the
	// hashmap. When removeAll is true, all rows associated with a key are removed.
	// When removeAll is false, only a single row is removed. Returned rows are
	// detached copies because the underlying entries have been removed.
	PopByVectors(keyVecs []*vector.Vector, removeAll bool) ([]GetResult, error)
	// PopByVectorsStream removes rows like PopByVectors but streams each matched
	// row to the callback. The callback receives the original key row index plus
	// the encoded key/value; the slices are detached copies and safe to keep.
	// The returned integer counts removed rows.
	PopByVectorsStream(keyVecs []*vector.Vector, removeAll bool, fn func(idx int, key []byte, row []byte) error) (int, error)
	// PopByEncodedKey removes rows using a pre-encoded key such as one obtained
	// from ForEachShardParallel. It mirrors PopByVectors semantics.
	PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error)
	// PopByEncodedKeyValue removes rows matching both the encoded key and encoded
	// value. When removeAll is true it removes all matching rows, otherwise it
	// removes a single row. It returns the number of rows removed.
	PopByEncodedKeyValue(encodedKey []byte, encodedValue []byte, removeAll bool) (int, error)
	// PopByEncodedFullValue PopByEncodedFullValues removes rows by reconstructing the key from a full
	// encoded row payload. The full row must match the value encoding used by
	// PutByVectors. It mirrors PopByEncodedKey semantics.
	PopByEncodedFullValue(encodedValue []byte, removeAll bool) (GetResult, error)
	// PopByEncodedFullValueExact removes rows by matching the full encoded row
	// payload, including the value bytes. It mirrors PopByEncodedKeyValue semantics.
	PopByEncodedFullValueExact(encodedValue []byte, removeAll bool) (int, error)
	// ForEachShardParallel provides exclusive access to each shard. The callback
	// receives a cursor offering read-only iteration plus mutation helpers that
	// avoid blocking other shards. parallelism <= 0 selects the default value:
	// min(runtime.NumCPU(), shardCount), clamped to [1, shardCount].
	ForEachShardParallel(fn func(cursor ShardCursor) error, parallelism int) error
	// Project rebuilds a new hashmap using the provided keyCols from the current
	// rows. parallelism controls shard-level fan-out; see ForEachShardParallel
	// for the clamping rules. The returned hashmap owns its own storage and is
	// independent from the source.
	Project(keyCols []int, parallelism int) (BranchHashmap, error)
	// Migrate rebuilds a new hashmap using the provided keyCols while freeing
	// rows from the current map during the process. This reduces peak memory
	// when Project would duplicate data. parallelism follows ForEachShardParallel
	// semantics.
	Migrate(keyCols []int, parallelism int) (BranchHashmap, error)
	// ItemCount reports the number of rows currently stored in the hashmap.
	ItemCount() int64
	// DecodeRow turns the encoded row emitted by Put/Get/Pop/ForEach back into a
	// tuple of column values in the same order that was originally supplied.
	DecodeRow(data []byte) (types.Tuple, []types.Type, error)
	Close() error
}

// ShardCursor exposes shard-scoped helpers when iterating via ForEachShardParallel.
type ShardCursor interface {
	// ShardID returns the zero-based shard identifier.
	ShardID() int
	// ForEach visits every encoded key stored inside the shard, including rows
	// that have spilled to disk. The callback is invoked per entry with a single
	// payload row. The provided slices reference internal buffers (or scratch
	// copies for spilled data) and are only valid inside the callback.
	ForEach(fn func(key []byte, row []byte) error) error
	// GetByEncodedKey retrieves rows using a pre-encoded key while the caller
	// still owns the iteration lock.
	GetByEncodedKey(encodedKey []byte) (GetResult, error)
	// PopByEncodedKey removes entries from the shard while the caller still owns
	// the iteration lock, ensuring In-shard mutations stay non-blocking.
	PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error)
	// PopByEncodedKeyValue removes entries matching both key and value while the
	// caller still owns the iteration lock.
	PopByEncodedKeyValue(encodedKey []byte, encodedValue []byte, removeAll bool) (int, error)
}

// GetResult bundles the original rows associated with a probed key.
type GetResult struct {
	// Exists reports whether at least one row matched the probed key.
	Exists bool
	// Rows contains the encoded payloads for every row that matched the key.
	// When obtained via GetByVectors the slices alias the internal storage and
	// must be treated as read-only views. Results returned by Pop* are copies.
	Rows [][]byte
}

type keyProbe struct {
	idx    int
	key    []byte
	keyStr string
	hash   uint64
	plan   *removalPlan
}

type probeGroup struct {
	indices []int
	plans   []*removalPlan
	next    int
	values  [][]byte
}

const (
	branchHashSeed             = 0x9e3779b97f4a7c15
	minShardCount              = 4
	maxShardCount              = 128
	putBatchSize               = 8192
	defaultMemIndexSize        = 1024
	defaultSpillBucketCount    = 1024
	defaultSpillSegmentMaxSize = 128 * mpool.MB
	spillEntryHeaderSize       = 16
)

// branchHashmap is an adaptive hash map that accepts vector columns and
// automatically spills to disk when the provided allocator cannot satisfy
// further allocations. Internally the dataset is sharded to guarantee that all
// exported APIs are safe for concurrent use.
type branchHashmap struct {
	allocator malloc.Allocator

	valueTypes []types.Type
	keyTypes   []types.Type
	keyCols    []int

	spillRoot string
	// spillBucketCount must be a power of two.
	spillBucketCount uint32
	// spillSegmentMaxBytes controls the maximum size of a spill file segment.
	spillSegmentMaxBytes uint64

	packerPool     sync.Pool
	entryBatchPool sync.Pool
	shardCount     int
	shards         []*hashShard

	metaMu sync.RWMutex
	closed bool
}

// BranchHashmapOption configures a branchHashmap instance.
type BranchHashmapOption func(*branchHashmap)

// WithBranchHashmapAllocator overrides the allocator used by branchHashmap.
func WithBranchHashmapAllocator(allocator malloc.Allocator) BranchHashmapOption {
	return func(bh *branchHashmap) {
		bh.allocator = allocator
	}
}

// WithBranchHashmapSpillRoot sets the root directory where spill files will be created.
func WithBranchHashmapSpillRoot(root string) BranchHashmapOption {
	return func(bh *branchHashmap) {
		bh.spillRoot = root
	}
}

// WithBranchHashmapSpillBucketCount sets the number of spill buckets per shard.
// The value is rounded up to the next power of two.
func WithBranchHashmapSpillBucketCount(bucketCount int) BranchHashmapOption {
	return func(bh *branchHashmap) {
		if bucketCount > 0 {
			bh.spillBucketCount = uint32(bucketCount)
		}
	}
}

// WithBranchHashmapSpillSegmentMaxBytes caps the size of each spill file segment.
func WithBranchHashmapSpillSegmentMaxBytes(maxBytes uint64) BranchHashmapOption {
	return func(bh *branchHashmap) {
		if maxBytes > 0 {
			bh.spillSegmentMaxBytes = maxBytes
		}
	}
}

// WithBranchHashmapShardCount sets the shard count. Values outside [4, 64] are clamped.
func WithBranchHashmapShardCount(shards int) BranchHashmapOption {
	return func(bh *branchHashmap) {
		bh.shardCount = shards
	}
}

// NewBranchHashmap constructs a new branchHashmap.
func NewBranchHashmap(opts ...BranchHashmapOption) (BranchHashmap, error) {
	bh := &branchHashmap{
		allocator:            malloc.GetDefault(nil),
		spillBucketCount:     defaultSpillBucketCount,
		spillSegmentMaxBytes: defaultSpillSegmentMaxSize,
	}
	for _, opt := range opts {
		opt(bh)
	}
	if bh.spillBucketCount == 0 {
		bh.spillBucketCount = defaultSpillBucketCount
	}
	bh.spillBucketCount = normalizeBucketCount(bh.spillBucketCount)
	if bh.spillSegmentMaxBytes == 0 {
		bh.spillSegmentMaxBytes = defaultSpillSegmentMaxSize
	}
	if bh.allocator == nil {
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap requires a non-nil allocator")
	}
	if bh.shardCount <= 0 {
		cpu := runtime.NumCPU() * 4
		bh.shardCount = cpu
	}
	if bh.shardCount < minShardCount {
		bh.shardCount = minShardCount
	}
	if bh.shardCount > maxShardCount {
		bh.shardCount = maxShardCount
	}
	bh.shards = make([]*hashShard, bh.shardCount)
	for i := range bh.shards {
		bh.shards[i] = newHashShard(i, bh.spillRoot, bh.spillBucketCount, bh.spillSegmentMaxBytes)
	}
	return bh, nil
}
func (bh *branchHashmap) ensurePutTypes(vecs []*vector.Vector, keyCols []int) error {
	bh.metaMu.Lock()
	defer bh.metaMu.Unlock()
	if bh.closed {
		return moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	if len(bh.valueTypes) == 0 {
		bh.valueTypes = cloneTypes(vecs)
		bh.keyTypes = make([]types.Type, len(keyCols))
		bh.keyCols = cloneInts(keyCols)
		seen := make(map[int]struct{}, len(keyCols))
		for i, idx := range keyCols {
			if idx < 0 || idx >= len(vecs) {
				return moerr.NewInvalidInputNoCtxf("key column index %d out of range", idx)
			}
			if _, ok := seen[idx]; ok {
				return moerr.NewInvalidInputNoCtx("duplicate key column index")
			}
			seen[idx] = struct{}{}
			bh.keyTypes[i] = *vecs[idx].GetType()
		}
		return nil
	}
	return bh.validatePutTypesLocked(vecs, keyCols)
}
func (bh *branchHashmap) PutByVectors(vecs []*vector.Vector, keyCols []int) error {
	if len(vecs) == 0 {
		return nil
	}
	if len(keyCols) == 0 {
		return moerr.NewInvalidInputNoCtx("branchHashmap requires at least one key column")
	}
	if err := bh.ensurePutTypes(vecs, keyCols); err != nil {
		return err
	}

	rowCount := vecs[0].Length()
	for i := 1; i < len(vecs); i++ {
		if vecs[i].Length() != rowCount {
			return moerr.NewInvalidInputNoCtxf("vector length mismatch at column %d", i)
		}
	}
	if rowCount == 0 {
		return nil
	}

	valueCols := make([]int, len(vecs))
	for i := range vecs {
		valueCols[i] = i
	}

	keyPacker := bh.getPacker()
	valuePacker := bh.getPacker()
	defer bh.putPacker(keyPacker)
	defer bh.putPacker(valuePacker)

	batchSize := putBatchSize
	if batchSize > rowCount {
		batchSize = rowCount
	}
	shardEntries := make([][]int, bh.shardCount)
	for batchStart := 0; batchStart < rowCount; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > rowCount {
			batchEnd = rowCount
		}
		chunk := bh.getPreparedEntryBatch(batchEnd - batchStart)
		for idx := batchStart; idx < batchEnd; idx++ {
			keyPacker.Reset()
			if err := encodeRow(keyPacker, vecs, keyCols, idx); err != nil {
				bh.putPreparedEntryBatch(chunk)
				return err
			}
			valuePacker.Reset()
			if err := encodeRow(valuePacker, vecs, valueCols, idx); err != nil {
				bh.putPreparedEntryBatch(chunk)
				return err
			}
			chunk = append(chunk, preparedEntry{
				key:   keyPacker.Bytes(),
				value: valuePacker.Bytes(),
			})
		}
		if err := bh.flushPreparedEntries(shardEntries, chunk); err != nil {
			bh.putPreparedEntryBatch(chunk)
			return err
		}
		bh.putPreparedEntryBatch(chunk)
	}

	return nil
}

func (bh *branchHashmap) flushPreparedEntries(shardEntries [][]int, chunk []preparedEntry) error {
	if len(chunk) == 0 {
		return nil
	}

	totalBytes := 0
	for i := range chunk {
		totalBytes += len(chunk[i].key) + len(chunk[i].value)
	}

	var (
		buf         []byte
		deallocator malloc.Deallocator
		err         error
	)
	if totalBytes > 0 {
		buf, deallocator, err = bh.allocateBuffer(uint64(totalBytes))
		if err != nil {
			if len(chunk) <= 1 {
				// Fallback to Go heap for single entry when allocator is exhausted.
				buf = make([]byte, totalBytes)
			} else {
				// Current batch does not fit into the allocator even after spilling.
				// Split the chunk so smaller batches can still make progress.
				mid := len(chunk) / 2
				if mid == 0 {
					mid = 1
				}
				if err := bh.flushPreparedEntries(shardEntries, chunk[:mid]); err != nil {
					return err
				}
				return bh.flushPreparedEntries(shardEntries, chunk[mid:])
			}
		}
	}

	block := &entryBlock{
		buf:         buf,
		deallocator: deallocator,
	}
	block.remaining.Store(int32(len(chunk)))
	offset := 0
	entries := make([]memEntry, len(chunk))
	for i := range chunk {
		prepared := &chunk[i]
		entrySize := len(prepared.key) + len(prepared.value)
		if entrySize > 0 {
			entryBuf := buf[offset : offset+entrySize]
			copy(entryBuf[:len(prepared.key)], prepared.key)
			copy(entryBuf[len(prepared.key):], prepared.value)
		}
		hash := hashKey(prepared.key)
		entries[i] = memEntry{
			hash:    hash,
			keyOff:  uint32(offset),
			keyLen:  uint32(len(prepared.key)),
			valOff:  uint32(offset + len(prepared.key)),
			valLen:  uint32(len(prepared.value)),
			block:   block,
			slot:    -1,
			lruPrev: -1,
			lruNext: -1,
		}
		offset += entrySize
		shardIdx := int(hash % uint64(bh.shardCount))
		shardEntries[shardIdx] = append(shardEntries[shardIdx], i)
	}
	for idx, entryIdxs := range shardEntries {
		if len(entryIdxs) == 0 {
			continue
		}
		shard := bh.shards[idx]
		shard.lock()
		for _, entryIdx := range entryIdxs {
			shard.insertEntryLocked(entries[entryIdx])
		}
		shard.unlock()
		shardEntries[idx] = entryIdxs[:0]
	}
	return nil
}
func (bh *branchHashmap) GetByVectors(keyVecs []*vector.Vector) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, nil)
}

func (bh *branchHashmap) GetByEncodedKey(encodedKey []byte) (GetResult, error) {
	var result GetResult

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return result, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	if len(bh.keyTypes) == 0 {
		bh.metaMu.RUnlock()
		return result, nil
	}
	shardCount := bh.shardCount
	bh.metaMu.RUnlock()

	if shardCount == 0 {
		return result, nil
	}

	hash := hashKey(encodedKey)
	shard := bh.shards[int(hash%uint64(shardCount))]
	rows, err := shard.getRows(hash, encodedKey)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	result.Exists = len(rows) > 0
	return result, nil
}

func (bh *branchHashmap) PopByVectors(keyVecs []*vector.Vector, removeAll bool) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, &removeAll)
}

func (bh *branchHashmap) PopByVectorsStream(keyVecs []*vector.Vector, removeAll bool, fn func(idx int, key []byte, row []byte) error) (int, error) {
	if len(keyVecs) == 0 {
		return 0, nil
	}

	rowCount := keyVecs[0].Length()

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return 0, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	keyTypes := bh.keyTypes
	shardCount := bh.shardCount
	bh.metaMu.RUnlock()

	if len(keyTypes) == 0 {
		return 0, nil
	}
	if len(keyVecs) != len(keyTypes) {
		return 0, moerr.NewInvalidInputNoCtxf("expected %d key vectors, got %d", len(keyTypes), len(keyVecs))
	}
	for i := 1; i < len(keyVecs); i++ {
		if keyVecs[i].Length() != rowCount {
			return 0, moerr.NewInvalidInputNoCtxf("key vector length mismatch at column %d", i)
		}
	}
	for i, vec := range keyVecs {
		if *vec.GetType() != keyTypes[i] {
			return 0, moerr.NewInvalidInputNoCtxf("key vector type mismatch at column %d", i)
		}
	}

	keyIndexes := make([]int, len(keyVecs))
	for i := range keyVecs {
		keyIndexes[i] = i
	}

	packer := bh.getPacker()
	defer bh.putPacker(packer)
	probesByShard := make(map[*hashShard][]*keyProbe, shardCount)
	for idx := 0; idx < rowCount; idx++ {
		packer.Reset()
		if err := encodeRow(packer, keyVecs, keyIndexes, idx); err != nil {
			return 0, err
		}
		keyBytes := packer.Bytes()
		keyCopy := make([]byte, len(keyBytes))
		copy(keyCopy, keyBytes)
		hash := hashKey(keyCopy)
		shard := bh.shards[int(hash%uint64(shardCount))]
		probe := &keyProbe{
			idx:    idx,
			key:    keyCopy,
			keyStr: string(keyCopy),
			hash:   hash,
			plan:   newRemovalPlan(removeAll),
		}
		probesByShard[shard] = append(probesByShard[shard], probe)
	}

	var totalRemoved int64
	collectValues := fn != nil
	for shard, probes := range probesByShard {
		if shard == nil {
			continue
		}
		shard.lock()
		var removedTotal int64
		for _, probe := range probes {
			rows, removedBytes, removedCount := shard.mem.collect(probe.hash, probe.key, probe.plan, true, collectValues)
			if removedBytes > 0 {
				shard.memInUse -= removedBytes
			}
			if removedCount > 0 {
				removedTotal += int64(removedCount)
			}
			if collectValues {
				for _, row := range rows {
					if err := fn(probe.idx, probe.key, row); err != nil {
						shard.unlock()
						return int(totalRemoved), err
					}
				}
			}
		}
		if shard.spill != nil {
			if err := collectSpillPopStream(shard, probes, removeAll, fn, &removedTotal, collectValues); err != nil {
				shard.unlock()
				return int(totalRemoved), err
			}
		}
		if removedTotal > 0 {
			atomic.AddInt64(&shard.items, -removedTotal)
			totalRemoved += removedTotal
		}
		shard.unlock()
	}

	return int(totalRemoved), nil
}

func (bh *branchHashmap) lookupByVectors(keyVecs []*vector.Vector, removeAll *bool) ([]GetResult, error) {
	if len(keyVecs) == 0 {
		return nil, nil
	}

	rowCount := keyVecs[0].Length()
	results := make([]GetResult, rowCount)

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	keyTypes := bh.keyTypes
	shardCount := bh.shardCount
	bh.metaMu.RUnlock()

	if len(keyTypes) == 0 {
		return results, nil
	}
	if len(keyVecs) != len(keyTypes) {
		return nil, moerr.NewInvalidInputNoCtxf("expected %d key vectors, got %d", len(keyTypes), len(keyVecs))
	}
	for i := 1; i < len(keyVecs); i++ {
		if keyVecs[i].Length() != rowCount {
			return nil, moerr.NewInvalidInputNoCtxf("key vector length mismatch at column %d", i)
		}
	}
	for i, vec := range keyVecs {
		if *vec.GetType() != keyTypes[i] {
			return nil, moerr.NewInvalidInputNoCtxf("key vector type mismatch at column %d", i)
		}
	}

	keyIndexes := make([]int, len(keyVecs))
	for i := range keyVecs {
		keyIndexes[i] = i
	}

	packer := bh.getPacker()
	defer bh.putPacker(packer)
	probesByShard := make(map[*hashShard][]*keyProbe, shardCount)
	for idx := 0; idx < rowCount; idx++ {
		packer.Reset()
		if err := encodeRow(packer, keyVecs, keyIndexes, idx); err != nil {
			return nil, err
		}
		keyBytes := packer.Bytes()
		keyCopy := make([]byte, len(keyBytes))
		copy(keyCopy, keyBytes)
		hash := hashKey(keyCopy)
		shard := bh.shards[int(hash%uint64(shardCount))]
		probe := &keyProbe{
			idx:    idx,
			key:    keyCopy,
			keyStr: string(keyCopy),
			hash:   hash,
		}
		if removeAll != nil {
			probe.plan = newRemovalPlan(*removeAll)
		}
		probesByShard[shard] = append(probesByShard[shard], probe)
	}

	for shard, probes := range probesByShard {
		if shard == nil {
			continue
		}
		shard.lock()
		var removedTotal int64
		copyValues := removeAll != nil
		for _, probe := range probes {
			if removeAll == nil {
				rows, _, _ := shard.mem.collect(probe.hash, probe.key, nil, false, true)
				results[probe.idx].Rows = rows
				continue
			}
			rows, removedBytes, removedCount := shard.mem.collect(probe.hash, probe.key, probe.plan, copyValues, true)
			results[probe.idx].Rows = rows
			if removedBytes > 0 {
				shard.memInUse -= removedBytes
			}
			if removedCount > 0 {
				removedTotal += int64(removedCount)
			}
		}

		if shard.spill != nil {
			if removeAll == nil {
				if err := collectSpillGet(shard, probes, results); err != nil {
					shard.unlock()
					return nil, err
				}
			} else {
				if err := collectSpillPop(shard, probes, results, *removeAll, &removedTotal); err != nil {
					shard.unlock()
					return nil, err
				}
			}
		}
		if removeAll != nil && removedTotal > 0 {
			atomic.AddInt64(&shard.items, -removedTotal)
		}
		shard.unlock()
	}

	for i := range results {
		results[i].Exists = len(results[i].Rows) > 0
	}
	return results, nil
}
func (bh *branchHashmap) PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error) {
	var result GetResult

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return result, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	if len(bh.keyTypes) == 0 {
		bh.metaMu.RUnlock()
		return result, nil
	}
	shardCount := bh.shardCount
	bh.metaMu.RUnlock()

	if shardCount == 0 {
		return result, nil
	}

	hash := hashKey(encodedKey)
	shard := bh.shards[int(hash%uint64(shardCount))]
	rows, err := shard.popRows(hash, encodedKey, removeAll)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	result.Exists = len(rows) > 0
	return result, nil
}

func (bh *branchHashmap) PopByEncodedKeyValue(encodedKey []byte, encodedValue []byte, removeAll bool) (int, error) {
	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return 0, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	if len(bh.keyTypes) == 0 {
		bh.metaMu.RUnlock()
		return 0, nil
	}
	shardCount := bh.shardCount
	bh.metaMu.RUnlock()

	if shardCount == 0 {
		return 0, nil
	}

	hash := hashKey(encodedKey)
	shard := bh.shards[int(hash%uint64(shardCount))]
	return shard.popRowsByValue(hash, encodedKey, encodedValue, removeAll)
}

func (bh *branchHashmap) PopByEncodedFullValue(encodedValue []byte, removeAll bool) (GetResult, error) {
	var result GetResult

	if bh.ItemCount() == 0 {
		return result, nil
	}

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return result, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	valueTypes := cloneTypesFromSlice(bh.valueTypes)
	keyTypes := cloneTypesFromSlice(bh.keyTypes)
	keyCols := cloneInts(bh.keyCols)
	bh.metaMu.RUnlock()

	if len(valueTypes) == 0 || len(keyTypes) == 0 || len(keyCols) == 0 {
		return result, moerr.NewInvalidInputNoCtx("branchHashmap PopByEncodedFullValue requires initialized key and value types")
	}
	if len(keyCols) != len(keyTypes) {
		return result, moerr.NewInvalidInputNoCtx("branchHashmap key columns/types mismatch")
	}

	tuple, err := types.Unpack(encodedValue)
	if err != nil {
		return result, err
	}
	if len(tuple) != len(valueTypes) {
		return result, moerr.NewInvalidInputNoCtxf("unexpected row width %d, want %d", len(tuple), len(valueTypes))
	}

	packer := bh.getPacker()
	defer bh.putPacker(packer)

	for i, colIdx := range keyCols {
		if valueTypes[colIdx] != keyTypes[i] {
			return result, moerr.NewInvalidInputNoCtx("key column type mismatch")
		}
		if err := encodeDecodedValue(packer, valueTypes[colIdx], tuple[colIdx]); err != nil {
			return result, err
		}
	}
	encodedKey := packer.Bytes()
	return bh.PopByEncodedKey(encodedKey, removeAll)
}

func (bh *branchHashmap) PopByEncodedFullValueExact(encodedValue []byte, removeAll bool) (int, error) {
	if bh.ItemCount() == 0 {
		return 0, nil
	}

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return 0, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	valueTypes := cloneTypesFromSlice(bh.valueTypes)
	keyTypes := cloneTypesFromSlice(bh.keyTypes)
	keyCols := cloneInts(bh.keyCols)
	bh.metaMu.RUnlock()

	if len(valueTypes) == 0 || len(keyTypes) == 0 || len(keyCols) == 0 {
		return 0, moerr.NewInvalidInputNoCtx("branchHashmap PopByEncodedFullValueExact requires initialized key and value types")
	}
	if len(keyCols) != len(keyTypes) {
		return 0, moerr.NewInvalidInputNoCtx("branchHashmap key columns/types mismatch")
	}

	tuple, err := types.Unpack(encodedValue)
	if err != nil {
		return 0, err
	}
	if len(tuple) != len(valueTypes) {
		return 0, moerr.NewInvalidInputNoCtxf("unexpected row width %d, want %d", len(tuple), len(valueTypes))
	}

	packer := bh.getPacker()
	defer bh.putPacker(packer)

	for i, colIdx := range keyCols {
		if valueTypes[colIdx] != keyTypes[i] {
			return 0, moerr.NewInvalidInputNoCtx("key column type mismatch")
		}
		if err := encodeDecodedValue(packer, valueTypes[colIdx], tuple[colIdx]); err != nil {
			return 0, err
		}
	}
	encodedKey := packer.Bytes()
	return bh.PopByEncodedKeyValue(encodedKey, encodedValue, removeAll)
}
func (bh *branchHashmap) ForEachShardParallel(fn func(cursor ShardCursor) error, parallelism int) error {
	if fn == nil {
		return nil
	}
	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	bh.metaMu.RUnlock()

	shardCount := len(bh.shards)
	if shardCount == 0 {
		return nil
	}

	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > shardCount {
		parallelism = shardCount
	}

	pool, err := ants.NewPool(parallelism)
	if err != nil {
		return err
	}
	defer pool.Release()

	var (
		wg      sync.WaitGroup
		firstMu sync.Mutex
		first   error
	)

	setError := func(err error) {
		if err == nil {
			return
		}
		firstMu.Lock()
		if first == nil {
			first = err
		}
		firstMu.Unlock()
	}

	for _, shard := range bh.shards {
		if shard == nil {
			continue
		}
		hs := shard
		wg.Add(1)
		if err := pool.Submit(func() {
			defer wg.Done()
			hs.beginIteration()
			defer hs.endIteration()
			cursor := shardCursor{shard: hs}
			if err := fn(&cursor); err != nil {
				setError(err)
			}
		}); err != nil {
			wg.Done()
			setError(err)
			break
		}
	}

	wg.Wait()
	return first
}

func (bh *branchHashmap) Project(keyCols []int, parallelism int) (BranchHashmap, error) {
	return bh.projectInternal(keyCols, parallelism, false)
}

func (bh *branchHashmap) Migrate(keyCols []int, parallelism int) (BranchHashmap, error) {
	return bh.projectInternal(keyCols, parallelism, true)
}

func (bh *branchHashmap) projectInternal(keyCols []int, parallelism int, consume bool) (BranchHashmap, error) {
	if bh.ItemCount() == 0 {
		return NewBranchHashmap()
	}

	if len(keyCols) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("branchHashmap requires at least one key column for Project")
	}

	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	valueTypes := cloneTypesFromSlice(bh.valueTypes)
	shardCount := bh.shardCount
	allocator := bh.allocator
	spillRoot := bh.spillRoot
	spillBucketCount := bh.spillBucketCount
	spillSegmentMaxBytes := bh.spillSegmentMaxBytes
	bh.metaMu.RUnlock()

	if len(valueTypes) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("branchHashmap Project requires existing value types")
	}
	newKeyTypes := make([]types.Type, len(keyCols))
	seen := make(map[int]struct{}, len(keyCols))
	for i, idx := range keyCols {
		if idx < 0 || idx >= len(valueTypes) {
			return nil, moerr.NewInvalidInputNoCtxf("key column index %d out of range", idx)
		}
		if _, ok := seen[idx]; ok {
			return nil, moerr.NewInvalidInputNoCtx("duplicate key column index in Project")
		}
		seen[idx] = struct{}{}
		newKeyTypes[i] = valueTypes[idx]
	}

	targetIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(spillRoot),
		WithBranchHashmapSpillBucketCount(int(spillBucketCount)),
		WithBranchHashmapSpillSegmentMaxBytes(spillSegmentMaxBytes),
		WithBranchHashmapShardCount(shardCount),
	)
	if err != nil {
		return nil, err
	}
	target, ok := targetIface.(*branchHashmap)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("unexpected BranchHashmap implementation")
	}
	target.metaMu.Lock()
	target.valueTypes = cloneTypesFromSlice(valueTypes)
	target.keyTypes = cloneTypesFromSlice(newKeyTypes)
	target.keyCols = cloneInts(keyCols)
	target.metaMu.Unlock()

	err = bh.ForEachShardParallel(func(cursor ShardCursor) error {
		shardEntries := make([][]int, target.shardCount)
		keyPacker := target.getPacker()
		chunk := target.getPreparedEntryBatch(0)
		flushChunk := func() error {
			if len(chunk) == 0 {
				return nil
			}
			if err := target.flushPreparedEntries(shardEntries, chunk); err != nil {
				return err
			}
			target.putPreparedEntryBatch(chunk)
			chunk = target.getPreparedEntryBatch(0)
			return nil
		}
		defer func() {
			target.putPacker(keyPacker)
			target.putPreparedEntryBatch(chunk)
		}()

		err := cursor.ForEach(func(key []byte, row []byte) error {
			var sourceRows [][]byte
			if consume {
				res, err := cursor.PopByEncodedKey(key, true)
				if err != nil {
					return err
				}
				sourceRows = res.Rows
			} else {
				sourceRows = [][]byte{row}
			}
			if len(sourceRows) == 0 {
				return nil
			}
			for _, row := range sourceRows {
				tuple, err := types.Unpack(row)
				if err != nil {
					return err
				}
				if len(tuple) != len(valueTypes) {
					return moerr.NewInvalidInputNoCtxf("unexpected row width %d, want %d", len(tuple), len(valueTypes))
				}

				keyPacker.Reset()
				for _, colIdx := range keyCols {
					if err := encodeDecodedValue(keyPacker, valueTypes[colIdx], tuple[colIdx]); err != nil {
						return err
					}
				}
				keyCopy := make([]byte, len(keyPacker.Bytes()))
				copy(keyCopy, keyPacker.Bytes())
				valueBytes := row
				if consume {
					valueCopy := make([]byte, len(row))
					copy(valueCopy, row)
					valueBytes = valueCopy
				}
				chunk = append(chunk, preparedEntry{
					key:   keyCopy,
					value: valueBytes,
				})
				if len(chunk) >= putBatchSize {
					if err := flushChunk(); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		return flushChunk()
	}, parallelism)
	if err != nil {
		_ = target.Close()
		return nil, err
	}
	return target, nil
}

func (bh *branchHashmap) ItemCount() int64 {
	bh.metaMu.RLock()
	if bh.closed {
		bh.metaMu.RUnlock()
		return 0
	}
	shards := bh.shards
	bh.metaMu.RUnlock()

	var total int64
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		total += shard.itemCount()
	}
	return total
}

type shardCursor struct {
	shard *hashShard
}

func (sc *shardCursor) ShardID() int {
	if sc == nil || sc.shard == nil {
		return -1
	}
	return sc.shard.id
}

func (sc *shardCursor) ForEach(fn func(key []byte, row []byte) error) error {
	if fn == nil || sc == nil || sc.shard == nil {
		return nil
	}
	return sc.shard.iterateUnsafe(fn)
}

func (sc *shardCursor) GetByEncodedKey(encodedKey []byte) (GetResult, error) {
	var result GetResult
	if sc == nil || sc.shard == nil {
		return result, nil
	}
	rows, err := sc.shard.getRowsDuringIteration(hashKey(encodedKey), encodedKey)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	result.Exists = len(rows) > 0
	return result, nil
}

func (sc *shardCursor) PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error) {
	var result GetResult
	if sc == nil || sc.shard == nil {
		return result, nil
	}
	rows, err := sc.shard.popRowsDuringIteration(hashKey(encodedKey), encodedKey, removeAll)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	result.Exists = len(rows) > 0
	return result, nil
}

func (sc *shardCursor) PopByEncodedKeyValue(encodedKey []byte, encodedValue []byte, removeAll bool) (int, error) {
	if sc == nil || sc.shard == nil {
		return 0, nil
	}
	return sc.shard.popRowsByValueDuringIteration(hashKey(encodedKey), encodedKey, encodedValue, removeAll)
}
func (bh *branchHashmap) DecodeRow(data []byte) (types.Tuple, []types.Type, error) {
	t, err := types.Unpack(data)
	//bh.metaMu.RLock()
	//valueTypes := bh.valueTypes
	//bh.metaMu.RUnlock()
	return t, nil, err
}
func (bh *branchHashmap) Close() error {
	bh.metaMu.Lock()
	if bh.closed {
		bh.metaMu.Unlock()
		return nil
	}
	bh.closed = true
	bh.metaMu.Unlock()

	var firstErr error
	var agg spillSnapshot
	var hadStats bool
	for _, shard := range bh.shards {
		if shard == nil {
			continue
		}
		if snapshot := shard.snapshotSpillStats(); snapshot.hasData() {
			agg.merge(snapshot)
			hadStats = true
		}
		if err := shard.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if hadStats {
		logutil.Infof("%s", agg.summary("branchHashmap"))
	}

	return firstErr
}

// hashShard owns shard-local state and serialization.
type hashShard struct {
	id        int
	spillRoot string

	mem      *memStore
	memInUse uint64

	spill                *spillStore
	spillDir             string
	spillBucketCount     uint32
	spillSegmentMaxBytes uint64

	items int64

	mu        sync.Mutex
	cond      *sync.Cond
	iterating bool
}

func newHashShard(id int, spillRoot string, spillBucketCount uint32, spillSegmentMaxBytes uint64) *hashShard {
	hs := &hashShard{
		id:                   id,
		spillRoot:            spillRoot,
		mem:                  newMemStore(),
		spillBucketCount:     spillBucketCount,
		spillSegmentMaxBytes: spillSegmentMaxBytes,
	}
	if hs.spillBucketCount == 0 {
		hs.spillBucketCount = 1
	}
	hs.spillBucketCount = normalizeBucketCount(hs.spillBucketCount)
	hs.cond = sync.NewCond(&hs.mu)
	return hs
}

func (hs *hashShard) lock() {
	hs.mu.Lock()
	for hs.iterating {
		hs.cond.Wait()
	}
}

func (hs *hashShard) unlock() {
	hs.mu.Unlock()
}

func (hs *hashShard) beginIteration() {
	hs.mu.Lock()
	for hs.iterating {
		hs.cond.Wait()
	}
	hs.iterating = true
	hs.mu.Unlock()
}

func (hs *hashShard) endIteration() {
	hs.mu.Lock()
	hs.iterating = false
	hs.mu.Unlock()
	hs.cond.Broadcast()
}

func (hs *hashShard) insertEntryLocked(entry memEntry) {
	if hs.mem == nil {
		hs.mem = newMemStore()
	}
	hs.mem.insert(entry)
	hs.memInUse += entry.size()
	atomic.AddInt64(&hs.items, 1)
	// TODO(monitoring): track shard-level insertion metrics.
}
func (hs *hashShard) getRows(hash uint64, key []byte) ([][]byte, error) {
	hs.lock()
	defer hs.unlock()
	rows, _, _ := hs.mem.collect(hash, key, nil, false, true)
	if hs.spill == nil {
		return rows, nil
	}
	if err := hs.spill.collect(hs.spill.bucketID(hash), hash, key, &rows, nil, true, scanReasonGet); err != nil {
		return nil, err
	}
	return rows, nil
}
func (hs *hashShard) getRowsDuringIteration(hash uint64, key []byte) ([][]byte, error) {
	if !hs.iterating {
		return nil, moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	rows, _, _ := hs.mem.collect(hash, key, nil, false, true)
	if hs.spill == nil {
		return rows, nil
	}
	if err := hs.spill.collect(hs.spill.bucketID(hash), hash, key, &rows, nil, true, scanReasonGet); err != nil {
		return nil, err
	}
	return rows, nil
}
func (hs *hashShard) popRows(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	hs.lock()
	defer hs.unlock()
	return hs.popRowsUnsafe(hash, key, removeAll)
}

func (hs *hashShard) popRowsByValue(hash uint64, key []byte, value []byte, removeAll bool) (int, error) {
	hs.lock()
	defer hs.unlock()
	return hs.popRowsByValueUnsafe(hash, key, value, removeAll)
}

func (hs *hashShard) popRowsDuringIteration(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	if !hs.iterating {
		return nil, moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	return hs.popRowsUnsafe(hash, key, removeAll)
}

func (hs *hashShard) popRowsByValueDuringIteration(hash uint64, key []byte, value []byte, removeAll bool) (int, error) {
	if !hs.iterating {
		return 0, moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	return hs.popRowsByValueUnsafe(hash, key, value, removeAll)
}

func (hs *hashShard) popRowsUnsafe(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	plan := newRemovalPlan(removeAll)
	rows, removedBytes, removedCount := hs.mem.collect(hash, key, plan, true, true)
	if removedBytes > 0 {
		hs.memInUse -= removedBytes
	}
	if hs.spill == nil {
		if removedCount > 0 {
			atomic.AddInt64(&hs.items, -int64(removedCount))
		}
		return rows, nil
	}
	if plan != nil && plan.hasRemaining() {
		if err := hs.spill.collect(hs.spill.bucketID(hash), hash, key, &rows, plan, true, scanReasonPop); err != nil {
			return nil, err
		}
	}
	if removed := len(rows); removed > 0 {
		atomic.AddInt64(&hs.items, -int64(removed))
	}
	return rows, nil
}

func (hs *hashShard) popRowsByValueUnsafe(hash uint64, key []byte, value []byte, removeAll bool) (int, error) {
	matchValue := value
	if removeAll && len(value) > 0 {
		matchValue = make([]byte, len(value))
		copy(matchValue, value)
	}
	plan := newValueRemovalPlan(removeAll, matchValue)
	_, removedBytes, removedCount := hs.mem.collect(hash, key, plan, false, false)
	if removedBytes > 0 {
		hs.memInUse -= removedBytes
	}
	if hs.spill == nil {
		if removedCount > 0 {
			atomic.AddInt64(&hs.items, -int64(removedCount))
		}
		return removedCount, nil
	}
	var unused [][]byte
	if plan != nil && plan.hasRemaining() {
		if err := hs.spill.collect(hs.spill.bucketID(hash), hash, key, &unused, plan, false, scanReasonPopValue); err != nil {
			return 0, err
		}
	}
	removed := plan.removed
	if removed > 0 {
		atomic.AddInt64(&hs.items, -int64(removed))
	}
	return removed, nil
}
func (hs *hashShard) spillLocked(required uint64) (uint64, error) {
	if required == 0 {
		return 0, nil
	}
	var (
		minRequired = max(mpool.MB, required)
	)
	store, err := hs.ensureSpillStore()
	if err != nil {
		return 0, err
	}
	entries, freed := hs.mem.pickEvictionEntries(minRequired)
	if len(entries) == 0 {
		return 0, nil
	}
	grouped := make(map[uint32][]spillEntry, len(entries))
	for _, idx := range entries {
		entry := &hs.mem.entries[idx]
		bucketID := store.bucketID(entry.hash)
		grouped[bucketID] = append(grouped[bucketID], spillEntry{
			hash:  entry.hash,
			key:   entry.keyBytes(),
			value: entry.valueBytes(),
		})
	}
	for bucketID, list := range grouped {
		if err := store.appendEntries(bucketID, list); err != nil {
			return 0, err
		}
	}
	for _, idx := range entries {
		freedBytes := hs.mem.removeEntry(idx)
		if freedBytes > 0 {
			hs.memInUse -= freedBytes
		}
	}
	return freed, nil
}

func (hs *hashShard) ensureSpillStore() (*spillStore, error) {
	if hs.spill != nil {
		return hs.spill, nil
	}
	if err := hs.ensureSpillDir(); err != nil {
		return nil, err
	}
	store, err := newSpillStore(hs.spillDir, hs.spillBucketCount, hs.spillSegmentMaxBytes)
	if err != nil {
		return nil, err
	}
	hs.spill = store
	return store, nil
}

func (hs *hashShard) ensureSpillDir() error {
	if hs.spillDir != "" {
		return nil
	}
	if hs.spillRoot != "" {
		if err := os.MkdirAll(hs.spillRoot, 0o755); err != nil {
			return err
		}
	}
	root := hs.spillRoot
	if root == "" {
		root = os.TempDir()
	}
	dir, err := os.MkdirTemp(root, fmt.Sprintf("branch-hashmap-shard-%d-*", hs.id))
	if err != nil {
		return err
	}
	hs.spillDir = dir
	return nil
}
func (hs *hashShard) close() error {
	hs.lock()
	defer hs.unlock()

	var firstErr error
	if hs.mem != nil {
		for idx := range hs.mem.entries {
			entry := &hs.mem.entries[idx]
			if !entry.inUse {
				continue
			}
			hs.memInUse -= entry.size()
			if entry.block != nil {
				entry.block.release()
			}
		}
		hs.mem = newMemStore()
	}
	hs.memInUse = 0

	if hs.spill != nil {
		if err := hs.spill.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		hs.spill = nil
	}

	if hs.spillDir != "" {
		if err := os.RemoveAll(hs.spillDir); err != nil && firstErr == nil {
			firstErr = err
		}
		hs.spillDir = ""
	}
	atomic.StoreInt64(&hs.items, 0)
	return firstErr
}

func (hs *hashShard) snapshotSpillStats() spillSnapshot {
	if hs == nil || hs.spill == nil {
		return spillSnapshot{}
	}
	ss := hs.spill.stats
	snap := spillSnapshot{
		spilledEntries:      ss.spilledEntries,
		spilledPayloadBytes: ss.spilledPayloadBytes,
		spilledBytes:        ss.spilledBytes,
		bucketScans:         ss.bucketScans,
		bucketScanBytes:     ss.bucketScanBytes,
		bucketScanEntries:   ss.bucketScanEntries,
		matchedEntries:      ss.matchedEntries,
		matchedBytes:        ss.matchedBytes,
		removedEntries:      ss.removedEntries,
		removedBytes:        ss.removedBytes,
		getProbeKeys:        ss.getProbeKeys,
		getBucketScans:      ss.getBucketScans,
		popProbeKeys:        ss.popProbeKeys,
		popBucketScans:      ss.popBucketScans,
		popValueProbeKeys:   ss.popValueProbeKeys,
		popValueBucketScans: ss.popValueBucketScans,
		forEachBucketScans:  ss.forEachBucketScans,
		bucketCount:         hs.spill.bucketCount,
	}
	var (
		maxSegments uint64
		segments    uint64
		bucketsUsed uint64
	)
	for _, bucket := range hs.spill.buckets {
		segCount := uint64(len(bucket.segments))
		segments += segCount
		if segCount > maxSegments {
			maxSegments = segCount
		}
		if bucket.rowCount > 0 {
			bucketsUsed++
		}
	}
	snap.segmentsTotal = segments
	snap.maxSegmentsPerBucket = maxSegments
	snap.bucketsUsed = bucketsUsed

	for _, scanCount := range ss.scanPerBucket {
		if scanCount == 0 {
			continue
		}
		snap.bucketsScanned++
		if scanCount > 1 {
			snap.repeatedBucketScans += scanCount - 1
		}
	}
	return snap
}
func (bh *branchHashmap) allocateBuffer(size uint64) ([]byte, malloc.Deallocator, error) {
	buf, deallocator, err := bh.allocator.Allocate(size, malloc.NoClear)
	if err != nil {
		return nil, nil, err
	}
	if buf == nil {
		if err := bh.spill(size); err != nil {
			return nil, nil, err
		}
		buf, deallocator, err = bh.allocator.Allocate(size, malloc.NoClear)
		if err != nil {
			return nil, nil, err
		}
		if buf == nil {
			return nil, nil, moerr.NewInternalErrorNoCtx("branchHashmap failed to allocate memory after spilling")
		}
	}
	return buf, deallocator, nil
}

func (bh *branchHashmap) spill(required uint64) error {
	if required == 0 {
		return nil
	}
	var freed uint64
	for freed < required {
		progressed := false
		for _, shard := range bh.shards {
			if shard == nil {
				continue
			}
			shard.lock()
			released, err := shard.spillLocked(required - freed)
			shard.unlock()
			if err != nil {
				return err
			}
			if released > 0 {
				freed += released
				progressed = true
			}
			if freed >= required {
				break
			}
		}
		if !progressed {
			break
		}
	}
	if freed < required {
		return moerr.NewInternalErrorNoCtx("branchHashmap cannot spill enough data to satisfy allocation request")
	}
	// TODO(monitoring): emit spill counters once metrics plumbing is ready.
	return nil
}
func (bh *branchHashmap) validatePutTypesLocked(vecs []*vector.Vector, keyCols []int) error {
	if len(vecs) != len(bh.valueTypes) {
		return moerr.NewInvalidInputNoCtx("vector count changed between PutByVectors calls")
	}
	for i, vec := range vecs {
		if *vec.GetType() != bh.valueTypes[i] {
			return moerr.NewInvalidInputNoCtxf("vector type mismatch at column %d", i)
		}
	}
	if len(keyCols) != len(bh.keyTypes) {
		return moerr.NewInvalidInputNoCtx("key column count changed between PutByVectors calls")
	}
	for i, idx := range keyCols {
		if idx < 0 || idx >= len(vecs) {
			return moerr.NewInvalidInputNoCtxf("key column index %d out of range", idx)
		}
		if *vecs[idx].GetType() != bh.keyTypes[i] {
			return moerr.NewInvalidInputNoCtxf("key column type mismatch at index %d", i)
		}
		if bh.keyCols[i] != idx {
			return moerr.NewInvalidInputNoCtx("key column indexes changed between PutByVectors calls")
		}
	}
	return nil
}

func (bh *branchHashmap) getPacker() *types.Packer {
	if v := bh.packerPool.Get(); v != nil {
		p := v.(*types.Packer)
		p.Reset()
		return p
	}
	return types.NewPacker()
}

func (bh *branchHashmap) putPacker(p *types.Packer) {
	p.Reset()
	bh.packerPool.Put(p)
}

func (bh *branchHashmap) getPreparedEntryBatch(capacity int) []preparedEntry {
	if capacity <= 0 {
		capacity = 1
	}
	if v := bh.entryBatchPool.Get(); v != nil {
		batchPtr := v.(*[]preparedEntry)
		batch := *batchPtr
		if cap(batch) < capacity {
			return make([]preparedEntry, 0, capacity)
		}
		return batch[:0]
	}
	return make([]preparedEntry, 0, capacity)
}

func (bh *branchHashmap) putPreparedEntryBatch(batch []preparedEntry) {
	for i := range batch {
		batch[i].key = nil
		batch[i].value = nil
	}
	batch = batch[:0]
	bh.entryBatchPool.Put(&batch)
}

func hashKey(key []byte) uint64 {
	rows := [][]byte{key}
	states := [][3]uint64{{}}
	hashtable.BytesBatchGenHashStatesWithSeed(&rows[0], &states[0], 1, branchHashSeed)
	return states[0][0]
}

func normalizeBucketCount(n uint32) uint32 {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

func cloneTypes(vecs []*vector.Vector) []types.Type {
	ret := make([]types.Type, len(vecs))
	for i, vec := range vecs {
		ret[i] = *vec.GetType()
	}
	return ret
}

func cloneTypesFromSlice(typesIn []types.Type) []types.Type {
	if len(typesIn) == 0 {
		return nil
	}
	out := make([]types.Type, len(typesIn))
	copy(out, typesIn)
	return out
}

func cloneInts(in []int) []int {
	if len(in) == 0 {
		return nil
	}
	out := make([]int, len(in))
	copy(out, in)
	return out
}

func encodeRow(p *types.Packer, vecs []*vector.Vector, cols []int, row int) error {
	for _, col := range cols {
		vec := vecs[col]
		if vec.IsNull(uint64(row)) {
			p.EncodeNull()
			continue
		}
		if err := encodeValue(p, vec, row); err != nil {
			return err
		}
	}
	return nil
}

func encodeDecodedValue(p *types.Packer, typ types.Type, v any) error {
	if v == nil {
		p.EncodeNull()
		return nil
	}
	switch typ.Oid {
	case types.T_bool:
		val, ok := v.(bool)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected bool value")
		}
		p.EncodeBool(val)
	case types.T_int8:
		val, ok := v.(int8)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int8 value")
		}
		p.EncodeInt8(val)
	case types.T_int16:
		val, ok := v.(int16)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int16 value")
		}
		p.EncodeInt16(val)
	case types.T_int32:
		val, ok := v.(int32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int32 value")
		}
		p.EncodeInt32(val)
	case types.T_int64:
		val, ok := v.(int64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected int64 value")
		}
		p.EncodeInt64(val)
	case types.T_uint8:
		val, ok := v.(uint8)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint8 value")
		}
		p.EncodeUint8(val)
	case types.T_uint16:
		val, ok := v.(uint16)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint16 value")
		}
		p.EncodeUint16(val)
	case types.T_uint32:
		val, ok := v.(uint32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint32 value")
		}
		p.EncodeUint32(val)
	case types.T_uint64:
		val, ok := v.(uint64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uint64 value")
		}
		p.EncodeUint64(val)
	case types.T_float32:
		val, ok := v.(float32)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected float32 value")
		}
		p.EncodeFloat32(val)
	case types.T_float64:
		val, ok := v.(float64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected float64 value")
		}
		p.EncodeFloat64(val)
	case types.T_date:
		val, ok := v.(types.Date)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected date value")
		}
		p.EncodeDate(val)
	case types.T_time:
		val, ok := v.(types.Time)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected time value")
		}
		p.EncodeTime(val)
	case types.T_datetime:
		val, ok := v.(types.Datetime)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected datetime value")
		}
		p.EncodeDatetime(val)
	case types.T_timestamp:
		val, ok := v.(types.Timestamp)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected timestamp value")
		}
		p.EncodeTimestamp(val)
	case types.T_decimal64:
		val, ok := v.(types.Decimal64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected decimal64 value")
		}
		p.EncodeDecimal64(val)
	case types.T_decimal128:
		val, ok := v.(types.Decimal128)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected decimal128 value")
		}
		p.EncodeDecimal128(val)
	case types.T_uuid:
		val, ok := v.(types.Uuid)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected uuid value")
		}
		p.EncodeUuid(val)
	case types.T_bit:
		val, ok := v.(uint64)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected bit value")
		}
		p.EncodeBit(val)
	case types.T_enum:
		switch val := v.(type) {
		case types.Enum:
			p.EncodeUint16(uint16(val))
		case uint16:
			p.EncodeUint16(val)
		default:
			return moerr.NewInvalidInputNoCtx("expected enum value")
		}
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64, types.T_TS:
		bytesVal, ok := v.([]byte)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected byte slice value")
		}
		p.EncodeStringType(bytesVal)
	default:
		bytesVal, ok := v.([]byte)
		if !ok {
			return moerr.NewInvalidInputNoCtx("expected byte slice value")
		}
		p.EncodeStringType(bytesVal)
	}
	return nil
}

func encodeValue(p *types.Packer, vec *vector.Vector, row int) error {
	switch vec.GetType().Oid {
	case types.T_bool:
		v := vector.GetFixedAtNoTypeCheck[bool](vec, row)
		p.EncodeBool(v)
	case types.T_int8:
		v := vector.GetFixedAtNoTypeCheck[int8](vec, row)
		p.EncodeInt8(v)
	case types.T_int16:
		v := vector.GetFixedAtNoTypeCheck[int16](vec, row)
		p.EncodeInt16(v)
	case types.T_int32:
		v := vector.GetFixedAtNoTypeCheck[int32](vec, row)
		p.EncodeInt32(v)
	case types.T_int64:
		v := vector.GetFixedAtNoTypeCheck[int64](vec, row)
		p.EncodeInt64(v)
	case types.T_uint8:
		v := vector.GetFixedAtNoTypeCheck[uint8](vec, row)
		p.EncodeUint8(v)
	case types.T_uint16:
		v := vector.GetFixedAtNoTypeCheck[uint16](vec, row)
		p.EncodeUint16(v)
	case types.T_uint32:
		v := vector.GetFixedAtNoTypeCheck[uint32](vec, row)
		p.EncodeUint32(v)
	case types.T_uint64:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, row)
		p.EncodeUint64(v)
	case types.T_float32:
		v := vector.GetFixedAtNoTypeCheck[float32](vec, row)
		p.EncodeFloat32(v)
	case types.T_float64:
		v := vector.GetFixedAtNoTypeCheck[float64](vec, row)
		p.EncodeFloat64(v)
	case types.T_date:
		v := vector.GetFixedAtNoTypeCheck[types.Date](vec, row)
		p.EncodeDate(v)
	case types.T_time:
		v := vector.GetFixedAtNoTypeCheck[types.Time](vec, row)
		p.EncodeTime(v)
	case types.T_datetime:
		v := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, row)
		p.EncodeDatetime(v)
	case types.T_timestamp:
		v := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row)
		p.EncodeTimestamp(v)
	case types.T_decimal64:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row)
		p.EncodeDecimal64(v)
	case types.T_decimal128:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row)
		p.EncodeDecimal128(v)
	case types.T_uuid:
		v := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, row)
		p.EncodeUuid(v)
	case types.T_bit:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, row)
		p.EncodeBit(v)
	case types.T_enum:
		v := vector.GetFixedAtNoTypeCheck[types.Enum](vec, row)
		p.EncodeUint16(uint16(v))
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64:
		p.EncodeStringType(vec.GetBytesAt(row))
	case types.T_TS:
		v := vector.GetFixedAtNoTypeCheck[types.TS](vec, row)
		p.EncodeStringType(v[:])
	default:
		raw := vec.GetRawBytesAt(row)
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		p.EncodeStringType(tmp)
	}
	return nil
}

type preparedEntry struct {
	key   []byte
	value []byte
}

type entryBlock struct {
	buf         []byte
	deallocator malloc.Deallocator
	remaining   atomic.Int32
}

func (eb *entryBlock) release() {
	if eb == nil {
		return
	}
	for {
		cur := eb.remaining.Load()
		if cur <= 0 {
			return
		}
		if eb.remaining.CompareAndSwap(cur, cur-1) {
			if cur-1 == 0 && eb.deallocator != nil {
				eb.deallocator.Deallocate()
				eb.deallocator = nil
				eb.buf = nil
			}
			return
		}
	}
}

const (
	memSlotEmpty     int32 = -1
	memSlotTombstone int32 = -2
)

type memEntry struct {
	hash    uint64
	keyOff  uint32
	keyLen  uint32
	valOff  uint32
	valLen  uint32
	block   *entryBlock
	slot    int32
	lruPrev int32
	lruNext int32
	inUse   bool
}

func (e *memEntry) keyBytes() []byte {
	if e == nil || e.keyLen == 0 || e.block == nil {
		return nil
	}
	start := int(e.keyOff)
	end := start + int(e.keyLen)
	return e.block.buf[start:end]
}

func (e *memEntry) valueBytes() []byte {
	if e == nil || e.valLen == 0 || e.block == nil {
		return nil
	}
	start := int(e.valOff)
	end := start + int(e.valLen)
	return e.block.buf[start:end]
}

func (e *memEntry) size() uint64 {
	return uint64(e.keyLen + e.valLen)
}

type memStore struct {
	index      []int32
	entries    []memEntry
	freeList   []int32
	count      int
	tombstones int
	lruHead    int32
	lruTail    int32
}

func newMemStore() *memStore {
	return &memStore{
		lruHead: -1,
		lruTail: -1,
	}
}

func (ms *memStore) len() int {
	if ms == nil {
		return 0
	}
	return ms.count
}

func (ms *memStore) ensureCapacity(additional int) {
	if ms.index == nil {
		ms.init(defaultMemIndexSize)
	}
	needed := ms.count + additional
	limit := int(float64(len(ms.index)) * 0.7)
	if needed+ms.tombstones <= limit {
		return
	}
	newCap := len(ms.index) * 2
	if newCap == 0 {
		newCap = defaultMemIndexSize
	}
	for int(float64(newCap)*0.7) < needed {
		newCap *= 2
	}
	ms.rehash(newCap)
}

func (ms *memStore) init(capacity int) {
	if capacity < defaultMemIndexSize {
		capacity = defaultMemIndexSize
	}
	capacity = int(normalizeBucketCount(uint32(capacity)))
	ms.index = make([]int32, capacity)
	for i := range ms.index {
		ms.index[i] = memSlotEmpty
	}
}

func (ms *memStore) rehash(capacity int) {
	if capacity < defaultMemIndexSize {
		capacity = defaultMemIndexSize
	}
	capacity = int(normalizeBucketCount(uint32(capacity)))
	newIndex := make([]int32, capacity)
	for i := range newIndex {
		newIndex[i] = memSlotEmpty
	}
	mask := uint64(capacity - 1)
	for idx := range ms.entries {
		entry := &ms.entries[idx]
		if !entry.inUse {
			continue
		}
		slot := int(entry.hash & mask)
		for {
			if newIndex[slot] == memSlotEmpty {
				newIndex[slot] = int32(idx)
				entry.slot = int32(slot)
				break
			}
			slot = (slot + 1) & (capacity - 1)
		}
	}
	ms.index = newIndex
	ms.tombstones = 0
}

func (ms *memStore) allocateEntryIndex() int32 {
	if n := len(ms.freeList); n > 0 {
		idx := ms.freeList[n-1]
		ms.freeList = ms.freeList[:n-1]
		return idx
	}
	ms.entries = append(ms.entries, memEntry{})
	return int32(len(ms.entries) - 1)
}

func (ms *memStore) insert(entry memEntry) int32 {
	ms.ensureCapacity(1)
	slot, usedTombstone := ms.findSlot(entry.hash)
	idx := ms.allocateEntryIndex()
	entry.slot = int32(slot)
	entry.inUse = true
	entry.lruPrev = -1
	entry.lruNext = -1
	ms.entries[idx] = entry
	ms.index[slot] = idx
	if usedTombstone {
		ms.tombstones--
	}
	ms.count++
	ms.lruAppend(idx)
	return idx
}

func (ms *memStore) findSlot(hash uint64) (int, bool) {
	mask := len(ms.index) - 1
	slot := int(hash & uint64(mask))
	firstTombstone := -1
	for {
		cur := ms.index[slot]
		switch cur {
		case memSlotEmpty:
			if firstTombstone >= 0 {
				return firstTombstone, true
			}
			return slot, false
		case memSlotTombstone:
			if firstTombstone < 0 {
				firstTombstone = slot
			}
		default:
		}
		slot = (slot + 1) & mask
	}
}

func (ms *memStore) lruAppend(idx int32) {
	if idx < 0 {
		return
	}
	if ms.lruTail < 0 {
		ms.lruHead = idx
		ms.lruTail = idx
		return
	}
	tail := ms.lruTail
	ms.entries[idx].lruPrev = tail
	ms.entries[idx].lruNext = -1
	ms.entries[tail].lruNext = idx
	ms.lruTail = idx
}

func (ms *memStore) lruRemove(idx int32) {
	if idx < 0 {
		return
	}
	entry := &ms.entries[idx]
	prev := entry.lruPrev
	next := entry.lruNext
	if prev >= 0 {
		ms.entries[prev].lruNext = next
	} else {
		ms.lruHead = next
	}
	if next >= 0 {
		ms.entries[next].lruPrev = prev
	} else {
		ms.lruTail = prev
	}
	entry.lruPrev = -1
	entry.lruNext = -1
}

func (ms *memStore) lruTouch(idx int32) {
	if idx < 0 || ms.lruTail == idx {
		return
	}
	ms.lruRemove(idx)
	ms.lruAppend(idx)
}

func (ms *memStore) removeEntry(idx int32) uint64 {
	if idx < 0 {
		return 0
	}
	entry := &ms.entries[idx]
	if !entry.inUse {
		return 0
	}
	if entry.slot >= 0 && entry.slot < int32(len(ms.index)) {
		if ms.index[entry.slot] == idx {
			ms.index[entry.slot] = memSlotTombstone
			ms.tombstones++
		}
	}
	ms.count--
	ms.lruRemove(idx)
	entry.inUse = false
	size := entry.size()
	if entry.block != nil {
		entry.block.release()
	}
	*entry = memEntry{}
	ms.freeList = append(ms.freeList, idx)
	return size
}

func (ms *memStore) collect(hash uint64, key []byte, plan *removalPlan, copyValues bool, collectValues bool) ([][]byte, uint64, int) {
	if len(ms.index) == 0 || ms.count == 0 {
		return nil, 0, 0
	}
	var (
		rows         [][]byte
		removedCount int
		removedBytes uint64
	)
	mask := len(ms.index) - 1
	slot := int(hash & uint64(mask))
	for {
		cur := ms.index[slot]
		if cur == memSlotEmpty {
			break
		}
		if cur >= 0 {
			entry := &ms.entries[cur]
			if entry.inUse && entry.hash == hash && bytes.Equal(entry.keyBytes(), key) {
				if plan == nil {
					if collectValues {
						value := entry.valueBytes()
						if copyValues {
							payload := make([]byte, len(value))
							copy(payload, value)
							rows = append(rows, payload)
						} else {
							rows = append(rows, value)
						}
					}
					ms.lruTouch(cur)
				} else if plan.matchesValue(entry.valueBytes()) && plan.take() {
					if collectValues {
						value := entry.valueBytes()
						payload := make([]byte, len(value))
						copy(payload, value)
						rows = append(rows, payload)
					}
					removedCount++
					ms.lruTouch(cur)
					removedBytes += ms.removeEntry(cur)
				}
			}
		}
		slot = (slot + 1) & mask
	}
	return rows, removedBytes, removedCount
}

func (ms *memStore) forEach(fn func(entry *memEntry) error) error {
	if fn == nil {
		return nil
	}
	for idx := range ms.entries {
		entry := &ms.entries[idx]
		if !entry.inUse {
			continue
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (ms *memStore) pickEvictionEntries(required uint64) ([]int32, uint64) {
	var (
		selected []int32
		freed    uint64
	)
	cur := ms.lruHead
	for cur >= 0 && freed < required {
		entry := &ms.entries[cur]
		if entry.inUse {
			selected = append(selected, cur)
			freed += entry.size()
		}
		cur = entry.lruNext
	}
	return selected, freed
}

type spillStore struct {
	dir             string
	bucketCount     uint32
	bucketMask      uint64
	maxSegmentBytes uint64
	buckets         []spillBucket
	stats           spillStats
}

type spillEntry struct {
	hash  uint64
	key   []byte
	value []byte
}

type spillBucket struct {
	segments  []*spillSegment
	rowCount  uint64
	tombstone *tombstoneBitmap
}

type spillSegment struct {
	path string
	size int64
}

type tombstoneBitmap struct {
	bits []uint64
}

type scanReason uint8

const (
	scanReasonUnknown scanReason = iota
	scanReasonForEach
	scanReasonGet
	scanReasonPop
	scanReasonPopValue
)

type spillStats struct {
	spilledEntries      uint64
	spilledPayloadBytes uint64
	spilledBytes        uint64
	segmentsCreated     uint64

	bucketScans       uint64
	bucketScanBytes   uint64
	bucketScanEntries uint64

	matchedEntries uint64
	matchedBytes   uint64
	removedEntries uint64
	removedBytes   uint64

	getProbeKeys        uint64
	getBucketScans      uint64
	popProbeKeys        uint64
	popBucketScans      uint64
	popValueProbeKeys   uint64
	popValueBucketScans uint64
	forEachBucketScans  uint64

	scanPerBucket []uint64
}

type spillSnapshot struct {
	spilledEntries       uint64
	spilledPayloadBytes  uint64
	spilledBytes         uint64
	segmentsTotal        uint64
	maxSegmentsPerBucket uint64
	bucketsUsed          uint64
	bucketCount          uint32

	bucketScans         uint64
	bucketScanBytes     uint64
	bucketScanEntries   uint64
	bucketsScanned      uint64
	repeatedBucketScans uint64

	matchedEntries uint64
	matchedBytes   uint64
	removedEntries uint64
	removedBytes   uint64

	getProbeKeys        uint64
	getBucketScans      uint64
	popProbeKeys        uint64
	popBucketScans      uint64
	popValueProbeKeys   uint64
	popValueBucketScans uint64
	forEachBucketScans  uint64
}

func (ss *spillStats) recordProbe(reason scanReason, keys int, buckets int) {
	if keys <= 0 || buckets <= 0 {
		return
	}
	switch reason {
	case scanReasonGet:
		ss.getProbeKeys += uint64(keys)
	case scanReasonPop:
		ss.popProbeKeys += uint64(keys)
	case scanReasonPopValue:
		ss.popValueProbeKeys += uint64(keys)
	}
}

func (ss *spillStats) recordMatch(valueLen int, removed bool) {
	if valueLen < 0 {
		valueLen = 0
	}
	ss.matchedEntries++
	ss.matchedBytes += uint64(valueLen)
	if removed {
		ss.removedEntries++
		ss.removedBytes += uint64(valueLen)
	}
}

func (ss *spillStats) recordScan(reason scanReason, bucketID uint32, entries uint64, bytes uint64) {
	if entries == 0 && bytes == 0 {
		return
	}
	ss.bucketScans++
	ss.bucketScanEntries += entries
	ss.bucketScanBytes += bytes
	if int(bucketID) < len(ss.scanPerBucket) {
		ss.scanPerBucket[bucketID]++
	}
	switch reason {
	case scanReasonForEach:
		ss.forEachBucketScans++
	case scanReasonGet:
		ss.getBucketScans++
	case scanReasonPop:
		ss.popBucketScans++
	case scanReasonPopValue:
		ss.popValueBucketScans++
	}
}

func (sn *spillSnapshot) merge(other spillSnapshot) {
	sn.spilledEntries += other.spilledEntries
	sn.spilledPayloadBytes += other.spilledPayloadBytes
	sn.spilledBytes += other.spilledBytes
	sn.segmentsTotal += other.segmentsTotal
	if other.maxSegmentsPerBucket > sn.maxSegmentsPerBucket {
		sn.maxSegmentsPerBucket = other.maxSegmentsPerBucket
	}
	sn.bucketsUsed += other.bucketsUsed
	if sn.bucketCount == 0 {
		sn.bucketCount = other.bucketCount
	}
	sn.bucketScans += other.bucketScans
	sn.bucketScanBytes += other.bucketScanBytes
	sn.bucketScanEntries += other.bucketScanEntries
	sn.bucketsScanned += other.bucketsScanned
	sn.repeatedBucketScans += other.repeatedBucketScans
	sn.matchedEntries += other.matchedEntries
	sn.matchedBytes += other.matchedBytes
	sn.removedEntries += other.removedEntries
	sn.removedBytes += other.removedBytes
	sn.getProbeKeys += other.getProbeKeys
	sn.getBucketScans += other.getBucketScans
	sn.popProbeKeys += other.popProbeKeys
	sn.popBucketScans += other.popBucketScans
	sn.popValueProbeKeys += other.popValueProbeKeys
	sn.popValueBucketScans += other.popValueBucketScans
	sn.forEachBucketScans += other.forEachBucketScans
}

func (sn spillSnapshot) hasData() bool {
	return sn.spilledEntries > 0 || sn.bucketScans > 0 || sn.matchedEntries > 0
}

func (sn spillSnapshot) summary(prefix string) string {
	avgEntry := uint64(0)
	if sn.spilledEntries > 0 {
		avgEntry = sn.spilledPayloadBytes / sn.spilledEntries
	}
	scanAmp := 0.0
	if sn.matchedEntries > 0 {
		scanAmp = float64(sn.bucketScanEntries) / float64(sn.matchedEntries)
	}
	avgKeysPerGetScan := 0.0
	if sn.getBucketScans > 0 {
		avgKeysPerGetScan = float64(sn.getProbeKeys) / float64(sn.getBucketScans)
	}
	avgKeysPerPopScan := 0.0
	if sn.popBucketScans > 0 {
		avgKeysPerPopScan = float64(sn.popProbeKeys) / float64(sn.popBucketScans)
	}
	hints := make([]string, 0, 3)
	if sn.matchedEntries > 0 && scanAmp > 50 {
		hints = append(hints, "high scan amplification")
	}
	if sn.getBucketScans > 0 && avgKeysPerGetScan < 2 {
		hints = append(hints, "low key batching on get")
	}
	if sn.popBucketScans > 0 && avgKeysPerPopScan < 2 {
		hints = append(hints, "low key batching on pop")
	}
	if sn.repeatedBucketScans > sn.bucketScans/2 {
		hints = append(hints, "many repeated bucket scans")
	}
	hintText := "ok"
	if len(hints) > 0 {
		hintText = strings.Join(hints, "; ")
	}
	return fmt.Sprintf(
		"%s spill summary: spilled_entries=%d spilled_payload_bytes=%d spilled_bytes=%d avg_entry_bytes=%d segments=%d max_segments_per_bucket=%d buckets_used=%d bucket_scans=%d scan_entries=%d scan_bytes=%d matched_entries=%d removed_entries=%d scan_amp=%.2f get_keys=%d get_bucket_scans=%d pop_keys=%d pop_bucket_scans=%d foreach_bucket_scans=%d repeated_bucket_scans=%d hints=%s",
		prefix,
		sn.spilledEntries,
		sn.spilledPayloadBytes,
		sn.spilledBytes,
		avgEntry,
		sn.segmentsTotal,
		sn.maxSegmentsPerBucket,
		sn.bucketsUsed,
		sn.bucketScans,
		sn.bucketScanEntries,
		sn.bucketScanBytes,
		sn.matchedEntries,
		sn.removedEntries,
		scanAmp,
		sn.getProbeKeys,
		sn.getBucketScans,
		sn.popProbeKeys,
		sn.popBucketScans,
		sn.forEachBucketScans,
		sn.repeatedBucketScans,
		hintText,
	)
}
func newSpillStore(dir string, bucketCount uint32, maxSegmentBytes uint64) (*spillStore, error) {
	if bucketCount == 0 {
		bucketCount = 1
	}
	bucketCount = normalizeBucketCount(bucketCount)
	if maxSegmentBytes == 0 {
		maxSegmentBytes = defaultSpillSegmentMaxSize
	}
	store := &spillStore{
		dir:             dir,
		bucketCount:     bucketCount,
		bucketMask:      uint64(bucketCount - 1),
		maxSegmentBytes: maxSegmentBytes,
		buckets:         make([]spillBucket, bucketCount),
	}
	store.stats.scanPerBucket = make([]uint64, bucketCount)
	return store, nil
}

func (ss *spillStore) bucketID(hash uint64) uint32 {
	return uint32(hash & ss.bucketMask)
}

func (ss *spillStore) appendEntries(bucketID uint32, entries []spillEntry) error {
	if len(entries) == 0 {
		return nil
	}
	bucket := &ss.buckets[bucketID]
	var (
		seg    *spillSegment
		file   *os.File
		writer *bufio.Writer
	)
	if len(bucket.segments) > 0 {
		seg = bucket.segments[len(bucket.segments)-1]
	}
	closeWriter := func() error {
		if writer != nil {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
		if file != nil {
			if err := file.Close(); err != nil {
				return err
			}
		}
		writer = nil
		file = nil
		return nil
	}
	for _, entry := range entries {
		entryBytes := spillEntryHeaderSize + len(entry.key) + len(entry.value)
		needsNewSegment := seg == nil
		if !needsNewSegment && ss.maxSegmentBytes > 0 && seg.size > 0 &&
			seg.size+int64(entryBytes) > int64(ss.maxSegmentBytes) {
			needsNewSegment = true
		}
		if needsNewSegment {
			if err := closeWriter(); err != nil {
				return err
			}
			var err error
			seg, file, writer, err = ss.createSegment(bucket, bucketID)
			if err != nil {
				return err
			}
		} else if file == nil || writer == nil {
			var err error
			file, err = os.OpenFile(seg.path, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return err
			}
			writer = bufio.NewWriter(file)
		}
		var header [spillEntryHeaderSize]byte
		binary.LittleEndian.PutUint64(header[0:8], entry.hash)
		binary.LittleEndian.PutUint32(header[8:12], uint32(len(entry.key)))
		binary.LittleEndian.PutUint32(header[12:16], uint32(len(entry.value)))
		if _, err := writer.Write(header[:]); err != nil {
			_ = closeWriter()
			return err
		}
		if len(entry.key) > 0 {
			if _, err := writer.Write(entry.key); err != nil {
				_ = closeWriter()
				return err
			}
		}
		if len(entry.value) > 0 {
			if _, err := writer.Write(entry.value); err != nil {
				_ = closeWriter()
				return err
			}
		}
		seg.size += int64(entryBytes)
		bucket.rowCount++
		ss.stats.spilledEntries++
		ss.stats.spilledPayloadBytes += uint64(len(entry.key) + len(entry.value))
		ss.stats.spilledBytes += uint64(entryBytes)
	}
	return closeWriter()
}

func (ss *spillStore) createSegment(bucket *spillBucket, bucketID uint32) (*spillSegment, *os.File, *bufio.Writer, error) {
	segmentID := len(bucket.segments)
	path := filepath.Join(ss.dir, fmt.Sprintf("spill-b%05d-%06d.bin", bucketID, segmentID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, nil, nil, err
	}
	seg := &spillSegment{path: path}
	bucket.segments = append(bucket.segments, seg)
	ss.stats.segmentsCreated++
	return seg, file, bufio.NewWriter(file), nil
}

func (ss *spillStore) forEachEntry(bucketID uint32, scratch *[]byte, fn func(key []byte, value []byte) error) error {
	return ss.scanBucket(bucketID, scanReasonForEach, scratch, func(_ uint64, key []byte, value []byte, _ uint64) (bool, error) {
		return false, fn(key, value)
	})
}

func (ss *spillStore) collect(bucketID uint32, hash uint64, key []byte, dst *[][]byte, plan *removalPlan, collectValues bool, reason scanReason) error {
	bucket := &ss.buckets[bucketID]
	if bucket.rowCount == 0 {
		return nil
	}
	ss.stats.recordProbe(reason, 1, 1)
	var scratch []byte
	return ss.scanBucket(bucketID, reason, &scratch, func(entryHash uint64, entryKey []byte, entryValue []byte, rid uint64) (bool, error) {
		if entryHash != hash || !bytes.Equal(entryKey, key) {
			return false, nil
		}
		if plan == nil {
			if collectValues {
				payload := make([]byte, len(entryValue))
				copy(payload, entryValue)
				*dst = append(*dst, payload)
			}
			ss.stats.recordMatch(len(entryValue), false)
			return false, nil
		}
		if plan.matchesValue(entryValue) && plan.take() {
			if collectValues {
				payload := make([]byte, len(entryValue))
				copy(payload, entryValue)
				*dst = append(*dst, payload)
			}
			bucket.setTombstone(rid)
			ss.stats.recordMatch(len(entryValue), true)
		}
		if !plan.hasRemaining() {
			return true, nil
		}
		return false, nil
	})
}

func (ss *spillStore) scanBucket(bucketID uint32, reason scanReason, scratch *[]byte, fn func(hash uint64, key []byte, value []byte, rid uint64) (bool, error)) error {
	bucket := &ss.buckets[bucketID]
	if bucket.rowCount == 0 {
		return nil
	}
	var header [spillEntryHeaderSize]byte
	var rid uint64
	var scannedEntries uint64
	var scannedBytes uint64
	for _, seg := range bucket.segments {
		file, err := os.Open(seg.path)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(file)
		for {
			if _, err := io.ReadFull(reader, header[:]); err != nil {
				if err == io.EOF {
					break
				}
				if err == io.ErrUnexpectedEOF {
					_ = file.Close()
					return io.ErrUnexpectedEOF
				}
				_ = file.Close()
				return err
			}
			entryHash := binary.LittleEndian.Uint64(header[0:8])
			keyLen := binary.LittleEndian.Uint32(header[8:12])
			valLen := binary.LittleEndian.Uint32(header[12:16])
			total := int(keyLen + valLen)
			scannedEntries++
			scannedBytes += uint64(spillEntryHeaderSize) + uint64(total)
			if cap(*scratch) < total {
				*scratch = make([]byte, total)
			}
			buf := (*scratch)[:total]
			if _, err := io.ReadFull(reader, buf); err != nil {
				_ = file.Close()
				return err
			}
			if bucket.tombstone != nil && bucket.tombstone.isSet(rid) {
				rid++
				continue
			}
			keyBytes := buf[:keyLen]
			valBytes := buf[keyLen:]
			stop, err := fn(entryHash, keyBytes, valBytes, rid)
			if err != nil {
				_ = file.Close()
				return err
			}
			rid++
			if stop {
				_ = file.Close()
				return nil
			}
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	ss.stats.recordScan(reason, bucketID, scannedEntries, scannedBytes)
	return nil
}

func (ss *spillStore) close() error {
	var firstErr error
	for _, bucket := range ss.buckets {
		for _, seg := range bucket.segments {
			if seg.path == "" {
				continue
			}
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (sb *spillBucket) setTombstone(rid uint64) {
	if sb.tombstone == nil {
		sb.tombstone = &tombstoneBitmap{}
	}
	sb.tombstone.set(rid)
}

func (tb *tombstoneBitmap) isSet(rid uint64) bool {
	idx := rid >> 6
	if int(idx) >= len(tb.bits) {
		return false
	}
	return tb.bits[idx]&(1<<(rid&63)) != 0
}

func (tb *tombstoneBitmap) set(rid uint64) {
	idx := rid >> 6
	if int(idx) >= len(tb.bits) {
		expanded := make([]uint64, idx+1)
		copy(expanded, tb.bits)
		tb.bits = expanded
	}
	tb.bits[idx] |= 1 << (rid & 63)
}

func collectSpillGet(shard *hashShard, probes []*keyProbe, results []GetResult) error {
	buckets := make(map[uint32][]*keyProbe, len(probes))
	for _, probe := range probes {
		bucketID := shard.spill.bucketID(probe.hash)
		buckets[bucketID] = append(buckets[bucketID], probe)
	}
	var scratch []byte
	for bucketID, bucketProbes := range buckets {
		shard.spill.stats.recordProbe(scanReasonGet, len(bucketProbes), 1)
		groupsByHash := make(map[uint64]map[string]*probeGroup, len(bucketProbes))
		for _, probe := range bucketProbes {
			groups, ok := groupsByHash[probe.hash]
			if !ok {
				groups = make(map[string]*probeGroup, 1)
				groupsByHash[probe.hash] = groups
			}
			group, ok := groups[probe.keyStr]
			if !ok {
				group = &probeGroup{}
				groups[probe.keyStr] = group
			}
			group.indices = append(group.indices, probe.idx)
		}

		if err := shard.spill.scanBucket(bucketID, scanReasonGet, &scratch, func(entryHash uint64, entryKey []byte, entryValue []byte, _ uint64) (bool, error) {
			groups := groupsByHash[entryHash]
			if len(groups) == 0 {
				return false, nil
			}
			group := groups[string(entryKey)]
			if group == nil {
				return false, nil
			}
			shard.spill.stats.recordMatch(len(entryValue), false)
			payload := make([]byte, len(entryValue))
			copy(payload, entryValue)
			group.values = append(group.values, payload)
			return false, nil
		}); err != nil {
			return err
		}

		for _, groups := range groupsByHash {
			for _, group := range groups {
				if len(group.values) == 0 {
					continue
				}
				for _, idx := range group.indices {
					results[idx].Rows = append(results[idx].Rows, group.values...)
				}
			}
		}
	}
	return nil
}

func collectSpillPop(shard *hashShard, probes []*keyProbe, results []GetResult, removeAll bool, removedTotal *int64) error {
	buckets := make(map[uint32][]*keyProbe, len(probes))
	seenKeys := map[string]struct{}{}
	for _, probe := range probes {
		if probe.plan == nil || !probe.plan.hasRemaining() {
			continue
		}
		if removeAll {
			if _, ok := seenKeys[probe.keyStr]; ok {
				continue
			}
			seenKeys[probe.keyStr] = struct{}{}
		}
		bucketID := shard.spill.bucketID(probe.hash)
		buckets[bucketID] = append(buckets[bucketID], probe)
	}

	var scratch []byte
	for bucketID, bucketProbes := range buckets {
		if len(bucketProbes) == 0 {
			continue
		}
		shard.spill.stats.recordProbe(scanReasonPop, len(bucketProbes), 1)
		groupsByHash := make(map[uint64]map[string]*probeGroup, len(bucketProbes))
		remaining := 0
		for _, probe := range bucketProbes {
			groups, ok := groupsByHash[probe.hash]
			if !ok {
				groups = make(map[string]*probeGroup, 1)
				groupsByHash[probe.hash] = groups
			}
			group, ok := groups[probe.keyStr]
			if !ok {
				group = &probeGroup{}
				groups[probe.keyStr] = group
			}
			group.indices = append(group.indices, probe.idx)
			group.plans = append(group.plans, probe.plan)
			if !removeAll && probe.plan.hasRemaining() {
				remaining++
			}
		}

		err := shard.spill.scanBucket(bucketID, scanReasonPop, &scratch, func(entryHash uint64, entryKey []byte, entryValue []byte, rid uint64) (bool, error) {
			groups := groupsByHash[entryHash]
			if len(groups) == 0 {
				return false, nil
			}
			group := groups[string(entryKey)]
			if group == nil {
				return false, nil
			}
			if removeAll {
				plan := group.plans[0]
				if plan.matchesValue(entryValue) && plan.take() {
					shard.spill.buckets[bucketID].setTombstone(rid)
					shard.spill.stats.recordMatch(len(entryValue), true)
					if removedTotal != nil {
						*removedTotal = *removedTotal + 1
					}
					payload := make([]byte, len(entryValue))
					copy(payload, entryValue)
					results[group.indices[0]].Rows = append(results[group.indices[0]].Rows, payload)
				}
				return false, nil
			}

			for group.next < len(group.plans) && !group.plans[group.next].hasRemaining() {
				group.next++
			}
			if group.next >= len(group.plans) {
				return false, nil
			}
			plan := group.plans[group.next]
			if plan.matchesValue(entryValue) && plan.take() {
				shard.spill.buckets[bucketID].setTombstone(rid)
				shard.spill.stats.recordMatch(len(entryValue), true)
				if removedTotal != nil {
					*removedTotal = *removedTotal + 1
				}
				payload := make([]byte, len(entryValue))
				copy(payload, entryValue)
				results[group.indices[group.next]].Rows = append(results[group.indices[group.next]].Rows, payload)
				if !plan.hasRemaining() {
					group.next++
					remaining--
					if remaining <= 0 {
						return true, nil
					}
				}
			}
			return false, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func collectSpillPopStream(shard *hashShard, probes []*keyProbe, removeAll bool, fn func(idx int, key []byte, row []byte) error, removedTotal *int64, collectValues bool) error {
	buckets := make(map[uint32][]*keyProbe, len(probes))
	seenKeys := map[string]struct{}{}
	for _, probe := range probes {
		if probe.plan == nil || !probe.plan.hasRemaining() {
			continue
		}
		if removeAll {
			if _, ok := seenKeys[probe.keyStr]; ok {
				continue
			}
			seenKeys[probe.keyStr] = struct{}{}
		}
		bucketID := shard.spill.bucketID(probe.hash)
		buckets[bucketID] = append(buckets[bucketID], probe)
	}

	var scratch []byte
	for bucketID, bucketProbes := range buckets {
		if len(bucketProbes) == 0 {
			continue
		}
		shard.spill.stats.recordProbe(scanReasonPop, len(bucketProbes), 1)
		groupsByHash := make(map[uint64]map[string]*probeGroup, len(bucketProbes))
		remaining := 0
		for _, probe := range bucketProbes {
			groups, ok := groupsByHash[probe.hash]
			if !ok {
				groups = make(map[string]*probeGroup, 1)
				groupsByHash[probe.hash] = groups
			}
			group, ok := groups[probe.keyStr]
			if !ok {
				group = &probeGroup{}
				groups[probe.keyStr] = group
			}
			group.indices = append(group.indices, probe.idx)
			group.plans = append(group.plans, probe.plan)
			if !removeAll && probe.plan.hasRemaining() {
				remaining++
			}
		}

		err := shard.spill.scanBucket(bucketID, scanReasonPop, &scratch, func(entryHash uint64, entryKey []byte, entryValue []byte, rid uint64) (bool, error) {
			groups := groupsByHash[entryHash]
			if len(groups) == 0 {
				return false, nil
			}
			group := groups[string(entryKey)]
			if group == nil {
				return false, nil
			}
			if removeAll {
				plan := group.plans[0]
				if plan.matchesValue(entryValue) && plan.take() {
					shard.spill.buckets[bucketID].setTombstone(rid)
					shard.spill.stats.recordMatch(len(entryValue), true)
					if removedTotal != nil {
						*removedTotal = *removedTotal + 1
					}
					if collectValues {
						payload := make([]byte, len(entryValue))
						copy(payload, entryValue)
						if err := fn(group.indices[0], entryKey, payload); err != nil {
							return true, err
						}
					}
				}
				return false, nil
			}

			for group.next < len(group.plans) && !group.plans[group.next].hasRemaining() {
				group.next++
			}
			if group.next >= len(group.plans) {
				return false, nil
			}
			plan := group.plans[group.next]
			if plan.matchesValue(entryValue) && plan.take() {
				shard.spill.buckets[bucketID].setTombstone(rid)
				shard.spill.stats.recordMatch(len(entryValue), true)
				if removedTotal != nil {
					*removedTotal = *removedTotal + 1
				}
				if collectValues {
					payload := make([]byte, len(entryValue))
					copy(payload, entryValue)
					if err := fn(group.indices[group.next], entryKey, payload); err != nil {
						return true, err
					}
				}
				if !plan.hasRemaining() {
					group.next++
					remaining--
					if remaining <= 0 {
						return true, nil
					}
				}
			}
			return false, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func newRemovalPlan(removeAll bool) *removalPlan {
	if removeAll {
		return &removalPlan{remaining: -1}
	}
	return &removalPlan{remaining: 1}
}

func newValueRemovalPlan(removeAll bool, value []byte) *removalPlan {
	plan := newRemovalPlan(removeAll)
	plan.matchValue = value
	plan.matchValueSet = true
	return plan
}

type removalPlan struct {
	remaining     int
	removed       int
	matchValue    []byte
	matchValueSet bool
}

func (rp *removalPlan) take() bool {
	if rp == nil {
		return false
	}
	if rp.remaining < 0 {
		rp.removed++
		return true
	}
	if rp.remaining == 0 {
		return false
	}
	rp.remaining--
	rp.removed++
	return true
}

func (rp *removalPlan) hasRemaining() bool {
	if rp == nil {
		return false
	}
	if rp.remaining < 0 {
		return true
	}
	return rp.remaining > 0
}

func (rp *removalPlan) matchesValue(value []byte) bool {
	if rp == nil || !rp.matchValueSet {
		return true
	}
	return bytes.Equal(value, rp.matchValue)
}

func (hs *hashShard) iterateUnsafe(fn func(key []byte, row []byte) error) error {
	if fn == nil {
		return nil
	}
	if !hs.iterating {
		return moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	if hs.mem != nil {
		if err := hs.mem.forEach(func(entry *memEntry) error {
			return fn(entry.keyBytes(), entry.valueBytes())
		}); err != nil {
			return err
		}
	}
	if hs.spill == nil {
		return nil
	}
	var scratch []byte
	for bucketID := uint32(0); bucketID < hs.spill.bucketCount; bucketID++ {
		err := hs.spill.forEachEntry(bucketID, &scratch, func(key []byte, value []byte) error {
			return fn(key, value)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (hs *hashShard) itemCount() int64 {
	return atomic.LoadInt64(&hs.items)
}
