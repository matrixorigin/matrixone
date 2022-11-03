package logtail

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type RowsT interface {
	Length() int
}

type BlockT[R RowsT] interface {
	Append(R) error
	IsAppendable() bool
	Length() int
}

type AOT[B BlockT[R], R RowsT] struct {
	sync.Mutex
	blockSize int
	appender  B
	blocks    *btree.BTreeG[B]
	factory   func() B
}

func NewAOT[B BlockT[R], R RowsT](
	blockSize int,
	factory func() B,
	lessFn func(_, _ B) bool) *AOT[B, R] {
	return &AOT[B, R]{
		blockSize: blockSize,
		factory:   factory,
		blocks:    btree.NewBTreeGOptions(lessFn, btree.Options{NoLocks: true}),
	}
}

func (aot *AOT[B, R]) Reset() {
	aot.Lock()
	defer aot.Unlock()
	aot.blocks.Clear()
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

// Truncate prunes the blocks by ts
// blocks:         (Page1[bornTs=1], Page2[bornTs=10], Page3[bornTs=20])
// Truncate(ts=5):   (Page1,Page2,Page3), ()
// Truncate(ts=12):  (Page2,Page3),       (Page1)
// Truncate(ts=30):  (),                  (Page1,Page2,Page3)
func (aot *AOT[B, R]) Truncate(filter func(_ B) bool) (cnt int) {
	aot.Lock()
	cpy := aot.blocks.Copy()
	aot.Unlock()

	valid := false
	candidates := make([]B, 0)
	cpy.Scan(func(block B) bool {
		if filter(block) {
			valid = true
			return false
		}
		candidates = append(candidates, block)
		return true
	})

	if !valid || len(candidates) == 0 {
		return
	}

	aot.Lock()
	defer aot.Unlock()

	for _, block := range candidates {
		aot.blocks.Delete(block)
	}

	return
}

// One appender
func (aot *AOT[B, R]) Append(rows R) (err error) {
	if aot.appender.IsAppendable() && aot.appender.Length() < aot.blockSize {
		err = aot.appender.Append(rows)
		return
	}
	newB := aot.factory()
	if err = aot.AppendBlock(newB); err != nil {
		return
	}
	err = aot.appender.Append(rows)
	return
}

func (aot *AOT[B, R]) AppendBlock(block B) (err error) {
	aot.Lock()
	defer aot.Unlock()
	if aot.appender.IsAppendable() && aot.appender.Length() < aot.blockSize {
		panic(moerr.NewInternalError("append a block but the previous block is appendable"))
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
