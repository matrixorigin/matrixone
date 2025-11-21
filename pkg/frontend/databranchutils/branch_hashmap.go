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
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	// PopByVectors behaves like GetByVectors but removes matching rows from the
	// hashmap. When removeAll is true, all rows associated with a key are removed.
	// When removeAll is false, only a single row is removed. Returned rows are
	// detached copies because the underlying entries have been removed.
	PopByVectors(keyVecs []*vector.Vector, removeAll bool) ([]GetResult, error)
	// PopByEncodedKey removes rows using a pre-encoded key such as one obtained
	// from ForEachShardParallel. It mirrors PopByVectors semantics.
	PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error)
	// PopByEncodedFullValues removes rows by reconstructing the key from a full
	// encoded row payload. The full row must match the value encoding used by
	// PutByVectors. It mirrors PopByEncodedKey semantics.
	PopByEncodedFullValue(encodedValue []byte, removeAll bool) (GetResult, error)
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
	// that have spilled to disk. The provided rows slices reference internal
	// buffers and are therefore read-only and only valid inside the callback.
	ForEach(fn func(key []byte, rows [][]byte) error) error
	// PopByEncodedKey removes entries from the shard while the caller still owns
	// the iteration lock, ensuring In-shard mutations stay non-blocking.
	PopByEncodedKey(encodedKey []byte, removeAll bool) (GetResult, error)
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

const (
	branchHashSeed = 0x9e3779b97f4a7c15
	minShardCount  = 4
	maxShardCount  = 64
	putBatchSize   = 8192
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

// WithBranchHashmapShardCount sets the shard count. Values outside [4, 64] are clamped.
func WithBranchHashmapShardCount(shards int) BranchHashmapOption {
	return func(bh *branchHashmap) {
		bh.shardCount = shards
	}
}

// NewBranchHashmap constructs a new branchHashmap.
func NewBranchHashmap(opts ...BranchHashmapOption) (BranchHashmap, error) {
	bh := &branchHashmap{
		allocator: malloc.GetDefault(nil),
	}
	for _, opt := range opts {
		opt(bh)
	}
	if bh.allocator == nil {
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap requires a non-nil allocator")
	}
	if bh.shardCount <= 0 {
		cpu := runtime.NumCPU() * 4
		if cpu <= 0 {
			cpu = 1
		}
		bh.shardCount = 1
	}
	if bh.shardCount < minShardCount {
		bh.shardCount = minShardCount
	}
	if bh.shardCount > maxShardCount {
		bh.shardCount = maxShardCount
	}
	bh.shards = make([]*hashShard, bh.shardCount)
	for i := range bh.shards {
		bh.shards[i] = newHashShard(i, bh.spillRoot)
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
	shardBuckets := make([][]*hashEntry, bh.shardCount)
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
		if err := bh.flushPreparedEntries(shardBuckets, chunk); err != nil {
			bh.putPreparedEntryBatch(chunk)
			return err
		}
		bh.putPreparedEntryBatch(chunk)
	}

	return nil
}

func (bh *branchHashmap) flushPreparedEntries(shardBuckets [][]*hashEntry, chunk []preparedEntry) error {
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
				if err := bh.flushPreparedEntries(shardBuckets, chunk[:mid]); err != nil {
					return err
				}
				return bh.flushPreparedEntries(shardBuckets, chunk[mid:])
			}
		}
	}

	block := &entryBlock{
		deallocator: deallocator,
		remaining:   len(chunk),
	}
	offset := 0
	for i := range chunk {
		prepared := &chunk[i]
		entrySize := len(prepared.key) + len(prepared.value)
		var entryBuf []byte
		if entrySize > 0 {
			entryBuf = buf[offset : offset+entrySize]
			copy(entryBuf[:len(prepared.key)], prepared.key)
			copy(entryBuf[len(prepared.key):], prepared.value)
		}
		offset += entrySize
		entry := &hashEntry{
			hash:     hashKey(prepared.key),
			buf:      entryBuf,
			keyLen:   uint32(len(prepared.key)),
			valueLen: uint32(len(prepared.value)),
			block:    block,
		}
		shardIdx := int(entry.hash % uint64(bh.shardCount))
		shardBuckets[shardIdx] = append(shardBuckets[shardIdx], entry)
	}
	for idx, entries := range shardBuckets {
		if len(entries) == 0 {
			continue
		}
		shard := bh.shards[idx]
		shard.lock()
		for _, entry := range entries {
			shard.insertEntryLocked(entry)
		}
		shard.unlock()
		for i := range entries {
			entries[i] = nil
		}
		shardBuckets[idx] = entries[:0]
	}
	return nil
}
func (bh *branchHashmap) GetByVectors(keyVecs []*vector.Vector) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, nil)
}

func (bh *branchHashmap) PopByVectors(keyVecs []*vector.Vector, removeAll bool) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, &removeAll)
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

	for idx := 0; idx < rowCount; idx++ {
		packer.Reset()
		if err := encodeRow(packer, keyVecs, keyIndexes, idx); err != nil {
			return nil, err
		}
		keyBytes := packer.Bytes()
		hash := hashKey(keyBytes)
		shard := bh.shards[int(hash%uint64(shardCount))]
		var (
			rows [][]byte
			err  error
		)
		if removeAll == nil {
			rows, err = shard.getRows(hash, keyBytes)
		} else {
			rows, err = shard.popRows(hash, keyBytes, *removeAll)
		}
		if err != nil {
			return nil, err
		}
		res := &results[idx]
		res.Rows = rows
		res.Exists = len(rows) > 0
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

func (bh *branchHashmap) PopByEncodedFullValue(encodedValue []byte, removeAll bool) (GetResult, error) {
	var result GetResult

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
		if *(&valueTypes[colIdx]) != keyTypes[i] {
			return result, moerr.NewInvalidInputNoCtx("key column type mismatch")
		}
		if err := encodeDecodedValue(packer, valueTypes[colIdx], tuple[colIdx]); err != nil {
			return result, err
		}
	}
	encodedKey := packer.Bytes()
	return bh.PopByEncodedKey(encodedKey, removeAll)
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
		shardBuckets := make([][]*hashEntry, target.shardCount)
		keyPacker := target.getPacker()
		chunk := target.getPreparedEntryBatch(0)
		flushChunk := func() error {
			if len(chunk) == 0 {
				return nil
			}
			if err := target.flushPreparedEntries(shardBuckets, chunk); err != nil {
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

		err := cursor.ForEach(func(key []byte, rows [][]byte) error {
			var sourceRows [][]byte
			if consume {
				res, err := cursor.PopByEncodedKey(key, true)
				if err != nil {
					return err
				}
				sourceRows = res.Rows
			} else {
				sourceRows = rows
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

func (sc *shardCursor) ForEach(fn func(key []byte, rows [][]byte) error) error {
	if fn == nil || sc == nil || sc.shard == nil {
		return nil
	}
	return sc.shard.iterateUnsafe(fn)
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
	for _, shard := range bh.shards {
		if shard == nil {
			continue
		}
		if err := shard.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// hashShard owns shard-local state and serialization.
type hashShard struct {
	id        int
	spillRoot string

	inMemory map[uint64]*hashBucket
	order    list.List
	memInUse uint64

	spills   []*spillPartition
	spillDir string

	items int64

	mu        sync.Mutex
	cond      *sync.Cond
	iterating bool
}

func newHashShard(id int, spillRoot string) *hashShard {
	hs := &hashShard{
		id:        id,
		spillRoot: spillRoot,
		inMemory:  make(map[uint64]*hashBucket),
	}
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

func (hs *hashShard) insertEntryLocked(entry *hashEntry) {
	bucket := hs.inMemory[entry.hash]
	if bucket == nil {
		bucket = &hashBucket{hash: entry.hash}
		bucket.orderNode = hs.order.PushBack(entry.hash)
		hs.inMemory[entry.hash] = bucket
	}
	bucket.add(entry)
	if bucket.orderNode != nil {
		hs.order.MoveToBack(bucket.orderNode)
	}
	hs.memInUse += uint64(len(entry.buf))
	atomic.AddInt64(&hs.items, 1)
	// TODO(monitoring): track shard-level insertion metrics.
}
func (hs *hashShard) getRows(hash uint64, key []byte) ([][]byte, error) {
	hs.lock()
	defer hs.unlock()
	rows := hs.collectFromMemory(hash, key, nil, nil, false)
	if len(hs.spills) == 0 {
		return rows, nil
	}
	var scratch []byte
	for _, part := range hs.spills {
		if err := part.collect(hash, key, &rows, &scratch, nil); err != nil {
			return nil, err
		}
	}
	return rows, nil
}
func (hs *hashShard) popRows(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	hs.lock()
	defer hs.unlock()
	return hs.popRowsUnsafe(hash, key, removeAll)
}

func (hs *hashShard) popRowsDuringIteration(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	if !hs.iterating {
		return nil, moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	return hs.popRowsUnsafe(hash, key, removeAll)
}

func (hs *hashShard) popRowsUnsafe(hash uint64, key []byte, removeAll bool) ([][]byte, error) {
	plan := newRemovalPlan(removeAll)
	rows := hs.collectFromMemory(hash, key, nil, plan, true)
	if len(hs.spills) == 0 {
		if removed := len(rows); removed > 0 {
			atomic.AddInt64(&hs.items, -int64(removed))
		}
		return rows, nil
	}
	var scratch []byte
	for _, part := range hs.spills {
		if plan != nil && !plan.hasRemaining() {
			break
		}
		if err := part.collect(hash, key, &rows, &scratch, plan); err != nil {
			return nil, err
		}
	}
	if removed := len(rows); removed > 0 {
		atomic.AddInt64(&hs.items, -int64(removed))
	}
	return rows, nil
}
func (hs *hashShard) collectFromMemory(hash uint64, key []byte, dst [][]byte, plan *removalPlan, copyValues bool) [][]byte {
	bucket, ok := hs.inMemory[hash]
	if !ok {
		return dst
	}
	matchLen := len(key)
	if plan != nil {
		newEntries := bucket.entries[:0]
		for _, entry := range bucket.entries {
			if !plan.hasRemaining() {
				newEntries = append(newEntries, entry)
				continue
			}
			if int(entry.keyLen) == matchLen && bytes.Equal(entry.keyBytes(), key) && plan.take() {
				value := entry.valueBytes()
				var payload []byte
				if copyValues {
					payload = make([]byte, len(value))
					copy(payload, value)
				} else {
					payload = value
				}
				dst = append(dst, payload)
				hs.memInUse -= uint64(len(entry.buf))
				entry.release()
			} else {
				newEntries = append(newEntries, entry)
			}
		}
		if len(newEntries) == 0 {
			if bucket.orderNode != nil {
				hs.order.Remove(bucket.orderNode)
			}
			delete(hs.inMemory, hash)
		} else {
			bucket.entries = newEntries
			if bucket.orderNode != nil {
				hs.order.MoveToBack(bucket.orderNode)
			}
		}
		return dst
	}
	if bucket.orderNode != nil {
		hs.order.MoveToBack(bucket.orderNode)
	}
	for _, entry := range bucket.entries {
		if int(entry.keyLen) != matchLen {
			continue
		}
		if bytes.Equal(entry.keyBytes(), key) {
			value := entry.valueBytes()
			if copyValues {
				copied := make([]byte, len(value))
				copy(copied, value)
				dst = append(dst, copied)
			} else {
				dst = append(dst, value)
			}
		}
	}
	return dst
}
func (hs *hashShard) spillLocked(required uint64) (uint64, error) {
	if required == 0 {
		return 0, nil
	}
	var (
		freed       uint64
		minRequired = max(mpool.MB, required)
	)
	for freed < minRequired {
		element := hs.order.Front()
		if element == nil {
			break
		}
		hash := element.Value.(uint64)
		bucket := hs.inMemory[hash]
		hs.order.Remove(element)
		if bucket == nil {
			delete(hs.inMemory, hash)
			continue
		}
		released, err := hs.spillBucketLocked(bucket)
		if err != nil {
			return freed, err
		}
		freed += released
		delete(hs.inMemory, hash)
	}
	return freed, nil
}

func (hs *hashShard) spillBucketLocked(bucket *hashBucket) (uint64, error) {
	if len(bucket.entries) == 0 {
		return 0, nil
	}
	part, err := hs.ensureSpillPartition()
	if err != nil {
		return 0, err
	}
	var freed uint64
	for _, entry := range bucket.entries {
		if err := part.append(entry); err != nil {
			return freed, err
		}
		freed += uint64(len(entry.buf))
		entry.release()
	}
	bucket.entries = nil
	hs.memInUse -= freed
	return freed, nil
}

func (hs *hashShard) ensureSpillPartition() (*spillPartition, error) {
	if err := hs.ensureSpillDir(); err != nil {
		return nil, err
	}
	part, err := newSpillPartition(hs.spillDir)
	if err != nil {
		return nil, err
	}
	hs.spills = append(hs.spills, part)
	return part, nil
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
	for hash, bucket := range hs.inMemory {
		if bucket.orderNode != nil {
			hs.order.Remove(bucket.orderNode)
		}
		for _, entry := range bucket.entries {
			hs.memInUse -= uint64(len(entry.buf))
			entry.release()
		}
		delete(hs.inMemory, hash)
	}

	for _, part := range hs.spills {
		if err := part.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if part.path != "" {
			if err := os.Remove(part.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
		}
	}
	hs.spills = nil

	if hs.spillDir != "" {
		if err := os.RemoveAll(hs.spillDir); err != nil && firstErr == nil {
			firstErr = err
		}
		hs.spillDir = ""
	}
	atomic.StoreInt64(&hs.items, 0)
	return firstErr
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
	deallocator malloc.Deallocator
	remaining   int
}

func (eb *entryBlock) release() {
	if eb == nil {
		return
	}
	if eb.remaining <= 0 {
		return
	}
	eb.remaining--
	if eb.remaining == 0 && eb.deallocator != nil {
		eb.deallocator.Deallocate(malloc.NoHints)
		eb.deallocator = nil
	}
}

type hashEntry struct {
	hash     uint64
	buf      []byte
	keyLen   uint32
	valueLen uint32
	block    *entryBlock
}

func (e *hashEntry) keyBytes() []byte {
	if e.keyLen == 0 {
		return nil
	}
	return e.buf[:e.keyLen]
}

func (e *hashEntry) valueBytes() []byte {
	if e.valueLen == 0 {
		return nil
	}
	return e.buf[e.keyLen : e.keyLen+e.valueLen]
}

func (e *hashEntry) release() {
	e.buf = nil
	if e.block != nil {
		e.block.release()
		e.block = nil
	}
}

type hashBucket struct {
	hash      uint64
	entries   []*hashEntry
	orderNode *list.Element
}

func (b *hashBucket) add(entry *hashEntry) {
	b.entries = append(b.entries, entry)
}

type spillPartition struct {
	file   *os.File
	path   string
	offset int64
	index  map[uint64][]spillPointer
}

type spillPointer struct {
	offset   int64
	keyLen   uint32
	valueLen uint32
}

func newSpillPartition(dir string) (*spillPartition, error) {
	file, err := os.CreateTemp(dir, "branch-spill-*.bin")
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to create spill file: %v", err)
	}
	return &spillPartition{
		file:  file,
		path:  file.Name(),
		index: make(map[uint64][]spillPointer),
	}, nil
}

func (sp *spillPartition) append(entry *hashEntry) error {
	var header [16]byte
	binary.LittleEndian.PutUint64(header[0:8], entry.hash)
	binary.LittleEndian.PutUint32(header[8:12], entry.keyLen)
	binary.LittleEndian.PutUint32(header[12:16], entry.valueLen)
	if n, err := sp.file.Write(header[:]); err != nil {
		return err
	} else if n != len(header) {
		return io.ErrShortWrite
	}
	if n, err := sp.file.Write(entry.buf); err != nil {
		return err
	} else if n != len(entry.buf) {
		return io.ErrShortWrite
	}
	dataOffset := sp.offset + 16
	sp.index[entry.hash] = append(sp.index[entry.hash], spillPointer{
		offset:   dataOffset,
		keyLen:   entry.keyLen,
		valueLen: entry.valueLen,
	})
	sp.offset += int64(16 + len(entry.buf))
	return nil
}

func (sp *spillPartition) collect(hash uint64, key []byte, dst *[][]byte, scratch *[]byte, plan *removalPlan) error {
	pointers := sp.index[hash]
	if len(pointers) == 0 {
		return nil
	}
	kept := pointers[:0]
	for _, ptr := range pointers {
		if plan != nil && !plan.hasRemaining() {
			kept = append(kept, ptr)
			continue
		}
		need := int(ptr.keyLen + ptr.valueLen)
		if cap(*scratch) < need {
			*scratch = make([]byte, need)
		}
		buf := (*scratch)[:need]
		n, err := sp.file.ReadAt(buf, ptr.offset)
		if err != nil && err != io.EOF {
			return err
		}
		if n != len(buf) {
			return io.ErrUnexpectedEOF
		}
		matched := bytes.Equal(buf[:ptr.keyLen], key)
		switch {
		case plan == nil:
			if matched {
				payload := make([]byte, ptr.valueLen)
				copy(payload, buf[ptr.keyLen:])
				*dst = append(*dst, payload)
			}
			kept = append(kept, ptr)
		case matched && plan.take():
			payload := make([]byte, ptr.valueLen)
			copy(payload, buf[ptr.keyLen:])
			*dst = append(*dst, payload)
		default:
			kept = append(kept, ptr)
		}
	}
	if plan != nil {
		if len(kept) == 0 {
			delete(sp.index, hash)
		} else {
			sp.index[hash] = append(sp.index[hash][:0], kept...)
		}
	} else if len(kept) != len(pointers) {
		sp.index[hash] = append(sp.index[hash][:0], kept...)
	}
	return nil
}

func (sp *spillPartition) close() error {
	if sp.file == nil {
		return nil
	}
	err := sp.file.Close()
	sp.file = nil
	return err
}

func newRemovalPlan(removeAll bool) *removalPlan {
	if removeAll {
		return &removalPlan{remaining: -1}
	}
	return &removalPlan{remaining: 1}
}

type removalPlan struct {
	remaining int
}

func (rp *removalPlan) take() bool {
	if rp == nil {
		return false
	}
	if rp.remaining < 0 {
		return true
	}
	if rp.remaining == 0 {
		return false
	}
	rp.remaining--
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

type iterationGroup struct {
	key  []byte
	rows [][]byte
}

func bytesToStableString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
func (hs *hashShard) iterateUnsafe(fn func(key []byte, rows [][]byte) error) error {
	if fn == nil {
		return nil
	}
	if !hs.iterating {
		return moerr.NewInternalErrorNoCtx("shard iteration context required")
	}
	groups := make(map[string]*iterationGroup, len(hs.inMemory))
	for _, bucket := range hs.inMemory {
		for _, entry := range bucket.entries {
			keyBytes := entry.keyBytes()
			keyStr := bytesToStableString(keyBytes)
			group, ok := groups[keyStr]
			if !ok {
				group = &iterationGroup{
					key:  keyBytes,
					rows: make([][]byte, 0, 1),
				}
				groups[keyStr] = group
			}
			group.rows = append(group.rows, entry.valueBytes())
		}
	}

	var scratch []byte
	for _, part := range hs.spills {
		for hash, pointers := range part.index {
			_ = hash
			for _, ptr := range pointers {
				need := int(ptr.keyLen + ptr.valueLen)
				if cap(scratch) < need {
					scratch = make([]byte, need)
				}
				buf := scratch[:need]
				n, err := part.file.ReadAt(buf, ptr.offset)
				if err != nil && err != io.EOF {
					return err
				}
				if n != len(buf) {
					return io.ErrUnexpectedEOF
				}
				keyCopy := make([]byte, ptr.keyLen)
				copy(keyCopy, buf[:ptr.keyLen])
				keyStr := string(keyCopy)
				group, ok := groups[keyStr]
				if !ok {
					group = &iterationGroup{
						key:  keyCopy,
						rows: make([][]byte, 0, 1),
					}
					groups[keyStr] = group
				}
				valueCopy := make([]byte, ptr.valueLen)
				copy(valueCopy, buf[ptr.keyLen:ptr.keyLen+ptr.valueLen])
				group.rows = append(group.rows, valueCopy)
			}
		}
	}

	for keyStr, group := range groups {
		if err := fn(group.key, group.rows); err != nil {
			return err
		}
		delete(groups, keyStr)
	}
	return nil
}

func (hs *hashShard) itemCount() int64 {
	return atomic.LoadInt64(&hs.items)
}
