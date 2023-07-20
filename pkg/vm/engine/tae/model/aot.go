// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/tidwall/btree"
)

// RowsT represents a group of rows
type RowsT[T any] interface {
	// row count
	Length() int

	// returns a window of the group of rows
	Window(offset, length int) T
}

// BlockT represents a block of rows
type BlockT[R RowsT[R]] interface {
	// Append appends a group of rows into the block
	Append(R) error

	// IsAppendable specifies wether the block is appendable
	IsAppendable() bool

	// Length specifies the row count of the block
	Length() int

	String() string

	// Close release the block bound resources
	// It should be called when the block is not used
	Close()
}

// AOTSnapshot represents the snapshot of a AOT
type AOTSnapshot[B BlockT[R], R RowsT[R]] interface {
	// Ascend the table within the range [pivot, last]
	Ascend(pivot B, iter func(blk B) bool)

	// Descend the table within the range [pivot, first]
	Descend(pivot B, iter func(blk B) bool)
}

// AOT stands for append-only-table
// append-only is the most common form of data organization.
// A basic data structure is abstracted here, which can cover
// most scenarios, such as logtail data and checkpoint data
type AOT[B BlockT[R], R RowsT[R]] struct {
	sync.Mutex
	blockSize    int
	appender     B
	blocks       *btree.BTreeG[B]
	blockFactory func(R) B
}

func NewAOT[B BlockT[R], R RowsT[R]](
	blockSize int,
	blockFactory func(R) B,
	lessFn func(_, _ B) bool) *AOT[B, R] {
	return &AOT[B, R]{
		blockSize:    blockSize,
		blockFactory: blockFactory,
		blocks:       btree.NewBTreeGOptions(lessFn, btree.Options{NoLocks: true}),
	}
}

func (aot *AOT[B, R]) Scan(fn func(_ B) bool) {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()
	cpy.Scan(fn)
}

func (aot *AOT[B, R]) Snapshot() AOTSnapshot[B, R] {
	aot.Lock()
	defer aot.Unlock()
	return aot.blocks.Copy()
}

func (aot *AOT[B, R]) Close() {
	aot.Lock()
	defer aot.Unlock()
	aot.blocks.Scan(func(block B) bool {
		block.Close()
		return true
	})
	aot.blocks.Clear()
}

func (aot *AOT[B, R]) String() string {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("AOT[Len=%d]", cpy.Len()))
	cpy.Scan(func(block B) bool {
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(block.String())
		return true
	})

	return w.String()
}

func (aot *AOT[B, R]) BlockCount() int {
	aot.Lock()
	defer aot.Unlock()
	return aot.blocks.Len()
}

func (aot *AOT[B, R]) Min() (b B) {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()
	b, _ = cpy.Min()
	return
}

func (aot *AOT[B, R]) Max() (b B) {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()
	b, _ = cpy.Max()
	return
}

// Truncate prunes the blocks.
// Deletable blocks are those have all txns prepared before the given timestamp
// For example: truncate the table by timestamp
// blocks:           (Page1[bornTs=1], Page2[bornTs=10], Page3[bornTs=20])
// Call              Remain               Delete
// Truncate(ts=5):   (Page1,Page2,Page3), ()
// Truncate(ts=12):  (Page2,Page3),       (Page1)
// Truncate(ts=30):  (Page3),             (Page1,Page2)
func (aot *AOT[B, R]) Truncate(stopFn func(_ B) bool) (cnt int) {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()

	valid := false // if there is a block stopping search early
	candidates := make([]B, 0)
	cpy.Scan(func(block B) bool {
		if stopFn(block) {
			// this block's bornTS >= given ts
			valid = true
			return false
		}
		candidates = append(candidates, block)
		// logutil.Infof("candidate %s", block.String())
		return true
	})

	// logutil.Infof("valid=%v, candidates len=%d", valid, len(candidates))

	// 1. clear them all? probably not a good idea, wrong checkpoint?
	// 2. just delete one block? seems not neccessary
	if !valid || len(candidates) <= 1 {
		return
	}
	candidates = candidates[:len(candidates)-1]

	aot.Lock()
	defer aot.Unlock()

	cnt = len(candidates)
	for _, block := range candidates {
		aot.blocks.Delete(block)
	}

	return
}

func (aot *AOT[B, R]) prepareAppend(rows int) (cnt int, all bool) {
	if !aot.appender.IsAppendable() {
		return
	}
	left := aot.blockSize - aot.appender.Length()
	if rows > left {
		cnt = left
	} else {
		cnt = rows
		all = true
	}
	return
}

// One appender
func (aot *AOT[B, R]) Append(rows R) (err error) {
	var (
		done     bool
		appended int
		toAppend int
	)
	for !done {
		toAppend, done = aot.prepareAppend(rows.Length() - appended)
		if toAppend == 0 {
			newB := aot.blockFactory(rows)
			if err = aot.appendBlock(newB); err != nil {
				return
			}
			continue
		}
		if toAppend == rows.Length() {
			if err = aot.appender.Append(rows); err != nil {
				return
			}
		} else {
			if err = aot.appender.Append(rows.Window(appended, toAppend)); err != nil {
				return
			}
		}
		// logutil.Infof("Appended=%d, ToAppend=%d, done=%v, AllRows=%d", appended, toAppend, done, rows.RowCount())
		appended += toAppend
	}
	return
}

func (aot *AOT[B, R]) appendBlock(block B) (err error) {
	aot.Lock()
	defer aot.Unlock()
	if aot.appender.IsAppendable() && aot.appender.Length() < aot.blockSize {
		panic(moerr.NewInternalError(
			context.Background(),
			"append a block but the previous block is appendable"))
	}
	aot.blocks.Set(block)
	aot.appender = block
	return
}

type TimedSliceBlock[R any] struct {
	BornTS types.TS
	Rows   []R
}

func NewTimedSliceBlock[R any](ts types.TS) *TimedSliceBlock[R] {
	return &TimedSliceBlock[R]{
		BornTS: ts,
		Rows:   make([]R, 0),
	}
}

func (blk *TimedSliceBlock[R]) Append(rows R) (err error) {
	blk.Rows = append(blk.Rows, rows)
	return
}

func (blk *TimedSliceBlock[R]) IsAppendable() bool {
	return blk != nil
}

func (blk *TimedSliceBlock[R]) Length() int {
	return len(blk.Rows)
}

func (blk *TimedSliceBlock[R]) String() string {
	return "TODO"
}

func (blk *TimedSliceBlock[R]) Close() {
	blk.BornTS = types.TS{}
	blk.Rows = make([]R, 0)
}

type BatchBlock struct {
	*containers.Batch
	ID uint64
}

func NewBatchBlock(id uint64, attrs []string, colTypes []types.Type, opts containers.Options) *BatchBlock {
	bat := containers.BuildBatch(attrs, colTypes, opts)
	block := &BatchBlock{
		Batch: bat,
		ID:    id,
	}
	return block
}

func (blk *BatchBlock) IsAppendable() bool {
	return blk != nil
}
