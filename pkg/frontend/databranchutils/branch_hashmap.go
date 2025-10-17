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

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	// GetResult per probed row, preserving the order of the original vectors.
	GetByVectors(keyVecs []*vector.Vector) ([]GetResult, error)
	// PopByVectors behaves like GetByVectors but removes every matching key from
	// the hashmap. The returned rows hold encoded payloads that can be decoded via
	// DecodeRow to recover the original column values.
	PopByVectors(keyVecs []*vector.Vector) ([]GetResult, error)
	// ForEach iterates through all in-memory and spilled entries, invoking fn with
	// the encoded key and all rows associated with that key.
	ForEach(fn func(key []byte, rows [][]byte) error) error
	// DecodeRow turns the encoded row emitted by Put/Get/Pop/ForEach back into a
	// tuple of column values in the same order that was originally supplied.
	DecodeRow(data []byte) (types.Tuple, []types.Type, error)
	Close() error
}

// branchHashmap is an adaptive hash map that accepts vector columns and
// automatically spills to disk when the provided allocator cannot satisfy
// further allocations.
type branchHashmap struct {
	allocator malloc.Allocator

	inMemory map[uint64]*hashBucket
	order    list.List
	memInUse uint64

	valueTypes []types.Type
	keyTypes   []types.Type

	spillRoot string
	spillDir  string
	spills    []*spillPartition

	packerPool sync.Pool
	closed     bool
	mu         sync.Mutex

	hashScratch struct {
		rows   [][]byte
		states [][3]uint64
	}

	concurrency    int
	pool           *ants.Pool
	entryBatchPool sync.Pool
}

const branchHashSeed uint64 = 0x9e3779b97f4a7c15

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

// WithBranchHashmapConcurrency overrides the worker concurrency used by branchHashmap.
func WithBranchHashmapConcurrency(concurrency int) BranchHashmapOption {
	return func(bh *branchHashmap) {
		bh.concurrency = concurrency
	}
}

// NewBranchHashmap constructs a new branchHashmap.
func NewBranchHashmap(opts ...BranchHashmapOption) (BranchHashmap, error) {
	bh := &branchHashmap{
		allocator: malloc.GetDefault(nil),
		inMemory:  make(map[uint64]*hashBucket),
	}
	bh.hashScratch.rows = make([][]byte, 1)
	bh.hashScratch.states = make([][3]uint64, 1)
	for _, opt := range opts {
		opt(bh)
	}
	if bh.allocator == nil {
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap requires a non-nil allocator")
	}
	if bh.concurrency <= 0 {
		bh.concurrency = runtime.GOMAXPROCS(0)
		if bh.concurrency <= 0 {
			bh.concurrency = 1
		}
	}
	pool, err := ants.NewPool(bh.concurrency, ants.WithPreAlloc(true))
	if err != nil {
		return nil, err
	}
	bh.pool = pool
	return bh, nil
}

// GetResult bundles the original rows associated with a probed key.
type GetResult struct {
	// Exists reports whether at least one row matched the probed key.
	Exists bool
	// Rows contains the encoded payloads for every row that matched the key.
	// Each element can be decoded back into column values using DecodeRow.
	Rows [][]byte
}

func (bh *branchHashmap) PutByVectors(vecs []*vector.Vector, keyCols []int) error {
	if len(vecs) == 0 {
		return nil
	}
	if len(keyCols) == 0 {
		return moerr.NewInvalidInputNoCtx("branchHashmap requires at least one key column")
	}

	bh.mu.Lock()
	defer bh.mu.Unlock()

	if bh.closed {
		return moerr.NewInternalErrorNoCtx("branchHashmap is closed")
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

	if len(bh.valueTypes) == 0 {
		bh.valueTypes = cloneTypes(vecs)
		bh.keyTypes = make([]types.Type, len(keyCols))
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
	} else {
		if err := bh.validatePutTypes(vecs, keyCols); err != nil {
			return err
		}
	}

	valueCols := make([]int, len(vecs))
	for i := range vecs {
		valueCols[i] = i
	}

	if bh.concurrency <= 0 {
		bh.concurrency = 1
	}
	concurrency := bh.concurrency
	if concurrency > rowCount {
		concurrency = rowCount
	}
	if concurrency == 0 {
		return nil
	}

	entryCh := make(chan []preparedEntry, concurrency)
	var wg sync.WaitGroup
	var encodeErr atomic.Pointer[error]
	chunkSize := (rowCount + concurrency - 1) / concurrency

	submitTask := func(start, end int) error {
		if start >= end {
			return nil
		}
		wg.Add(1)
		err := bh.pool.Submit(func() {
			defer wg.Done()
			if encodeErr.Load() != nil {
				return
			}
			keyPacker := bh.getPacker()
			valuePacker := bh.getPacker()
			defer bh.putPacker(keyPacker)
			defer bh.putPacker(valuePacker)

			chunk := bh.getPreparedEntryBatch(end - start)
			submitted := false
			defer func() {
				if !submitted {
					bh.putPreparedEntryBatch(chunk)
				}
			}()

			for idx := start; idx < end; idx++ {
				if encodeErr.Load() != nil {
					return
				}
				keyPacker.Reset()
				if err := encodeRow(keyPacker, vecs, keyCols, idx); err != nil {
					errCopy := err
					encodeErr.CompareAndSwap(nil, &errCopy)
					return
				}
				valuePacker.Reset()
				if err := encodeRow(valuePacker, vecs, valueCols, idx); err != nil {
					errCopy := err
					encodeErr.CompareAndSwap(nil, &errCopy)
					return
				}
				chunk = append(chunk, preparedEntry{
					key:   keyPacker.Bytes(),
					value: valuePacker.Bytes(),
				})
			}
			if len(chunk) > 0 {
				submitted = true
				entryCh <- chunk
			}
		})
		if err != nil {
			wg.Done()
		}
		return err
	}

	for start := 0; start < rowCount; start += chunkSize {
		end := start + chunkSize
		if end > rowCount {
			end = rowCount
		}
		if err := submitTask(start, end); err != nil {
			return err
		}
	}

	go func() {
		wg.Wait()
		close(entryCh)
	}()

	for chunk := range entryCh {
		if encodeErr.Load() != nil {
			bh.putPreparedEntryBatch(chunk)
			continue
		}
		if len(chunk) == 0 {
			bh.putPreparedEntryBatch(chunk)
			continue
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
				errCopy := err
				encodeErr.CompareAndSwap(nil, &errCopy)
				bh.putPreparedEntryBatch(chunk)
				continue
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
				hash:     bh.hashKey(prepared.key),
				buf:      entryBuf,
				keyLen:   uint32(len(prepared.key)),
				valueLen: uint32(len(prepared.value)),
				block:    block,
			}
			bh.insertEntry(entry)
		}
		bh.memInUse += uint64(totalBytes)
		bh.putPreparedEntryBatch(chunk)
	}

	if errPtr := encodeErr.Load(); errPtr != nil {
		return *errPtr
	}

	return nil
}

func (bh *branchHashmap) GetByVectors(keyVecs []*vector.Vector) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, false)
}

func (bh *branchHashmap) PopByVectors(keyVecs []*vector.Vector) ([]GetResult, error) {
	return bh.lookupByVectors(keyVecs, true)
}

func (bh *branchHashmap) lookupByVectors(keyVecs []*vector.Vector, remove bool) ([]GetResult, error) {
	if len(keyVecs) == 0 {
		return nil, nil
	}

	bh.mu.Lock()
	defer bh.mu.Unlock()

	var (
		rowCount = keyVecs[0].Length()
		results  = make([]GetResult, rowCount)
	)

	if bh.closed {
		return nil, moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}
	if len(bh.keyTypes) == 0 {
		// empty hashmap
		return results, nil
	}

	if len(keyVecs) != len(bh.keyTypes) {
		return nil, moerr.NewInvalidInputNoCtxf("expected %d key vectors, got %d", len(bh.keyTypes), len(keyVecs))
	}

	for i := 1; i < len(keyVecs); i++ {
		if keyVecs[i].Length() != rowCount {
			return nil, moerr.NewInvalidInputNoCtxf("key vector length mismatch at column %d", i)
		}
	}

	for i, vec := range keyVecs {
		if *vec.GetType() != bh.keyTypes[i] {
			return nil, moerr.NewInvalidInputNoCtxf("key vector type mismatch at column %d", i)
		}
	}

	keyIndexes := make([]int, len(keyVecs))
	for i := range keyVecs {
		keyIndexes[i] = i
	}

	if bh.concurrency <= 0 {
		bh.concurrency = 1
	}
	concurrency := bh.concurrency
	if concurrency > rowCount {
		concurrency = rowCount
	}
	if concurrency == 0 {
		return results, nil
	}

	type preparedKey struct {
		idx int
		key []byte
	}

	keyCh := make(chan []preparedKey, concurrency)
	var wg sync.WaitGroup
	var encodeErr atomic.Pointer[error]
	chunkSize := (rowCount + concurrency - 1) / concurrency

	submitTask := func(start, end int) error {
		if start >= end {
			return nil
		}
		wg.Add(1)
		err := bh.pool.Submit(func() {
			defer wg.Done()
			if encodeErr.Load() != nil {
				return
			}
			packer := types.NewPacker()
			defer packer.Close()
			chunk := make([]preparedKey, 0, end-start)
			for idx := start; idx < end; idx++ {
				if encodeErr.Load() != nil {
					return
				}
				packer.Reset()
				if err := encodeRow(packer, keyVecs, keyIndexes, idx); err != nil {
					errCopy := err
					encodeErr.CompareAndSwap(nil, &errCopy)
					return
				}
				chunk = append(chunk, preparedKey{
					idx: idx,
					key: packer.Bytes(),
				})
			}
			if len(chunk) > 0 {
				keyCh <- chunk
			}
		})
		if err != nil {
			wg.Done()
		}
		return err
	}

	for start := 0; start < rowCount; start += chunkSize {
		end := start + chunkSize
		if end > rowCount {
			end = rowCount
		}
		if err := submitTask(start, end); err != nil {
			return nil, err
		}
	}

	go func() {
		wg.Wait()
		close(keyCh)
	}()

	var spillBuf []byte
	for chunk := range keyCh {
		if encodeErr.Load() != nil {
			continue
		}
		for _, prepared := range chunk {
			hash := bh.hashKey(prepared.key)
			res := &results[prepared.idx]
			res.Rows = res.Rows[:0]
			res.Rows = bh.collectFromMemory(hash, prepared.key, res.Rows, remove)

			if len(bh.spills) > 0 {
				for _, part := range bh.spills {
					if err := part.collect(hash, prepared.key, &res.Rows, &spillBuf, remove); err != nil {
						errCopy := err
						encodeErr.CompareAndSwap(nil, &errCopy)
						break
					}
				}
			}
			res.Exists = len(res.Rows) > 0
		}
	}

	if errPtr := encodeErr.Load(); errPtr != nil {
		return nil, *errPtr
	}
	return results, nil
}

func (bh *branchHashmap) DecodeRow(data []byte) (types.Tuple, []types.Type, error) {
	t, err := types.Unpack(data)
	return t, bh.valueTypes, err
}

func (bh *branchHashmap) Close() error {
	bh.mu.Lock()
	defer bh.mu.Unlock()

	if bh.closed {
		return nil
	}
	bh.closed = true

	var firstErr error

	for hash, bucket := range bh.inMemory {
		if bucket.orderNode != nil {
			bh.order.Remove(bucket.orderNode)
		}
		for _, entry := range bucket.entries {
			bh.memInUse -= uint64(len(entry.buf))
			entry.release()
		}
		delete(bh.inMemory, hash)
	}

	for _, part := range bh.spills {
		if err := part.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if part.path != "" {
			if err := os.Remove(part.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
		}
	}
	bh.spills = nil

	if bh.spillDir != "" {
		if err := os.RemoveAll(bh.spillDir); err != nil && firstErr == nil {
			firstErr = err
		}
		bh.spillDir = ""
	}
	if bh.pool != nil {
		bh.pool.Release()
		bh.pool = nil
	}

	return firstErr
}

type iterationGroup struct {
	key  []byte
	rows [][]byte
}

func (bh *branchHashmap) ForEach(fn func(key []byte, rows [][]byte) error) error {
	bh.mu.Lock()
	if bh.closed {
		bh.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("branchHashmap is closed")
	}

	groups := make(map[string]*iterationGroup)

	for _, bucket := range bh.inMemory {
		for _, entry := range bucket.entries {
			keyBytes := entry.keyBytes()
			groupKey := string(keyBytes)
			group, ok := groups[groupKey]
			if !ok {
				keyCopy := make([]byte, len(keyBytes))
				copy(keyCopy, keyBytes)
				group = &iterationGroup{
					key:  keyCopy,
					rows: make([][]byte, 0, 1),
				}
				groups[groupKey] = group
			}
			value := entry.valueBytes()
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			group.rows = append(group.rows, valueCopy)
		}
	}

	var scratch []byte
	for _, part := range bh.spills {
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
					bh.mu.Unlock()
					return err
				}
				if n != len(buf) {
					bh.mu.Unlock()
					return io.ErrUnexpectedEOF
				}
				keyBytes := buf[:ptr.keyLen]
				groupKey := string(keyBytes)
				group, ok := groups[groupKey]
				if !ok {
					keyCopy := make([]byte, ptr.keyLen)
					copy(keyCopy, keyBytes)
					group = &iterationGroup{
						key:  keyCopy,
						rows: make([][]byte, 0, 1),
					}
					groups[groupKey] = group
				}
				valueBytes := buf[ptr.keyLen : ptr.keyLen+ptr.valueLen]
				valueCopy := make([]byte, len(valueBytes))
				copy(valueCopy, valueBytes)
				group.rows = append(group.rows, valueCopy)
			}
		}
	}
	bh.mu.Unlock()

	for _, group := range groups {
		rowsCopy := make([][]byte, len(group.rows))
		copy(rowsCopy, group.rows)
		if err := fn(group.key, rowsCopy); err != nil {
			return err
		}
	}
	return nil
}

func (bh *branchHashmap) validatePutTypes(vecs []*vector.Vector, keyCols []int) error {
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
		batch := v.([]preparedEntry)
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
	bh.entryBatchPool.Put(batch[:0])
}

func (bh *branchHashmap) hashKey(key []byte) uint64 {
	bh.hashScratch.rows[0] = key
	hashtable.BytesBatchGenHashStatesWithSeed(&bh.hashScratch.rows[0], &bh.hashScratch.states[0], 1, branchHashSeed)
	return bh.hashScratch.states[0][0]
}

func (bh *branchHashmap) allocateBuffer(size uint64) ([]byte, malloc.Deallocator, error) {
	buf, deallocator, err := bh.allocator.Allocate(size, malloc.NoClear)
	if err != nil {
		return nil, nil, err
	}
	if buf == nil {
		fmt.Println("spill")
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
	return buf[:size], deallocator, nil
}

func (bh *branchHashmap) insertEntry(entry *hashEntry) {
	b := bh.inMemory[entry.hash]
	if b == nil {
		b = &hashBucket{hash: entry.hash}
		b.orderNode = bh.order.PushBack(entry.hash)
		bh.inMemory[entry.hash] = b
	}
	b.add(entry)
	if b.orderNode != nil {
		bh.order.MoveToBack(b.orderNode)
	}
}

func (bh *branchHashmap) collectFromMemory(hash uint64, key []byte, dst [][]byte, remove bool) [][]byte {
	bucket, ok := bh.inMemory[hash]
	if !ok {
		return dst
	}
	if remove {
		newEntries := bucket.entries[:0]
		for _, entry := range bucket.entries {
			if int(entry.keyLen) == len(key) && bytes.Equal(entry.keyBytes(), key) {
				value := entry.valueBytes()
				copied := make([]byte, len(value))
				copy(copied, value)
				dst = append(dst, copied)
				bh.memInUse -= uint64(len(entry.buf))
				entry.release()
			} else {
				newEntries = append(newEntries, entry)
			}
		}
		if len(newEntries) == 0 {
			if bucket.orderNode != nil {
				bh.order.Remove(bucket.orderNode)
			}
			delete(bh.inMemory, hash)
		} else {
			bucket.entries = newEntries
			if bucket.orderNode != nil {
				bh.order.MoveToBack(bucket.orderNode)
			}
		}
		return dst
	}
	if bucket.orderNode != nil {
		bh.order.MoveToBack(bucket.orderNode)
	}
	for _, entry := range bucket.entries {
		if int(entry.keyLen) != len(key) {
			continue
		}
		if bytes.Equal(entry.keyBytes(), key) {
			value := entry.valueBytes()
			copied := make([]byte, len(value))
			copy(copied, value)
			dst = append(dst, copied)
		}
	}
	return dst
}

func (bh *branchHashmap) spill(required uint64) error {
	var freed uint64
	for freed < required {
		element := bh.order.Front()
		if element == nil {
			break
		}
		hash := element.Value.(uint64)
		bucket := bh.inMemory[hash]
		bh.order.Remove(element)
		if bucket == nil {
			delete(bh.inMemory, hash)
			continue
		}
		rel, err := bh.spillBucket(bucket)
		if err != nil {
			return err
		}
		freed += rel
		delete(bh.inMemory, hash)
	}
	if freed < required {
		return moerr.NewInternalErrorNoCtx("branchHashmap cannot spill enough data to satisfy allocation request")
	}
	return nil
}

func (bh *branchHashmap) spillBucket(b *hashBucket) (uint64, error) {
	if len(b.entries) == 0 {
		return 0, nil
	}
	part, err := bh.ensureSpillPartition()
	if err != nil {
		return 0, err
	}
	var freed uint64
	for _, entry := range b.entries {
		if err := part.append(entry); err != nil {
			return freed, err
		}
		freed += uint64(len(entry.buf))
		entry.release()
	}
	b.entries = nil
	bh.memInUse -= freed
	return freed, nil
}

func (bh *branchHashmap) ensureSpillPartition() (*spillPartition, error) {
	if err := bh.ensureSpillDir(); err != nil {
		return nil, err
	}
	part, err := newSpillPartition(bh.spillDir)
	if err != nil {
		return nil, err
	}
	bh.spills = append(bh.spills, part)
	return part, nil
}

func (bh *branchHashmap) ensureSpillDir() error {
	if bh.spillDir != "" {
		return nil
	}
	if bh.spillRoot != "" {
		if err := os.MkdirAll(bh.spillRoot, 0o755); err != nil {
			return err
		}
	}
	root := bh.spillRoot
	if root == "" {
		root = os.TempDir()
	}
	dir, err := os.MkdirTemp(root, "branch-hashmap-*")
	if err != nil {
		return err
	}
	bh.spillDir = dir
	return nil
}

func cloneTypes(vecs []*vector.Vector) []types.Type {
	ret := make([]types.Type, len(vecs))
	for i, vec := range vecs {
		ret[i] = *vec.GetType()
	}
	return ret
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
	case types.T_decimal64:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row)
		p.EncodeDecimal64(v)
	case types.T_decimal128:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row)
		p.EncodeDecimal128(v)
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
	case types.T_bit:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, row)
		p.EncodeBit(v)
	case types.T_enum:
		v := vector.GetFixedAtNoTypeCheck[uint16](vec, row)
		p.EncodeUint16(v)
	case types.T_uuid:
		v := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, row)
		p.EncodeUuid(v)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_array_float32, types.T_array_float64:
		p.EncodeStringType(vec.GetBytesAt(row))
	default:
		raw := vec.GetRawBytesAt(row)
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		p.EncodeStringType(tmp)
	}
	return nil
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

func (sp *spillPartition) collect(hash uint64, key []byte, dst *[][]byte, scratch *[]byte, remove bool) error {
	pointers := sp.index[hash]
	if len(pointers) == 0 {
		return nil
	}
	kept := pointers[:0]
	for _, ptr := range pointers {
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
		if bytes.Equal(buf[:ptr.keyLen], key) {
			payload := make([]byte, ptr.valueLen)
			copy(payload, buf[ptr.keyLen:])
			*dst = append(*dst, payload)
			if !remove {
				kept = append(kept, ptr)
			}
		} else {
			kept = append(kept, ptr)
		}
	}
	if remove {
		if len(kept) == 0 {
			delete(sp.index, hash)
		} else {
			sp.index[hash] = append(sp.index[hash][:0], kept...)
		}
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
