package adaptors

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
)

func NewBitSet(size int) *BitSet {
	capacity := (size-1)>>3 + 1
	if size <= int(common.K*8) {
		set := make([]byte, capacity)
		return &BitSet{
			size: size,
			set:  set,
		}
	}
	storage := containers.NewStdVector[byte](&containers.Options{
		Capacity: capacity,
	})
	return &BitSet{
		size:    size,
		set:     storage.Data()[:capacity],
		storage: storage,
	}
}

func (bs *BitSet) Close() {
	if bs.storage != nil {
		bs.storage.Close()
	}
}

func (bs *BitSet) GetCardinality() int {
	return bs.bitcnt
}

func (bs *BitSet) IsEmpty() bool {
	return bs.bitcnt == 0
}

func (bs *BitSet) Contains(x int) bool {
	return (bs.set[x>>3] & (1 << (x & 0x7))) != 0
}

func (bs *BitSet) Add(x int) {
	if x >= bs.size {
		panic(fmt.Errorf("set %d bit but size is %d", x, bs.size))
	}
	old := bs.set[x>>3] & (1 << (x & 0x7))
	bs.set[x>>3] |= 1 << (x & 0x7)
	if old == 0 {
		bs.bitcnt += 1
	}
}

// [start, end)
func (bs *BitSet) AddRange(start, end int) {
	if start >= end {
		return
	}
	if start >= bs.size || end > bs.size {
		panic(fmt.Errorf("add range [%d, %d) but size is %d", start, end, bs.size))
	}
	for x := start; x < end; x++ {
		old := bs.set[x>>3] & (1 << (x & 0x7))
		bs.set[x>>3] |= 1 << (x & 0x7)
		if old == 0 {
			bs.bitcnt += 1
		}
	}
}

func (bs *BitSet) Remove(x int) {
	if x >= bs.size {
		panic(fmt.Errorf("remove %d bit but size is %d", x, bs.size))
	}
	old := bs.set[x>>3] & (1 << (x & 0x7))
	bs.set[x>>3] &= ^(1 << (x & 0x7))
	if old != 0 {
		bs.bitcnt -= 1
	}
}

func (bs *BitSet) RemoveRange(start, end int) {
	if start >= end {
		return
	}
	if start >= bs.size || end > bs.size {
		panic(fmt.Errorf("remove range [%d, %d) but size is %d", start, end, bs.size))
	}
	for x := start; x < end; x++ {
		old := bs.set[x>>3] & (1 << (x & 0x7))
		bs.set[x>>3] &= ^(1 << (x & 0x7))
		if old != 0 {
			bs.bitcnt -= 1
		}
	}
}

func (bs *BitSet) Clear() {
	for i := range bs.set {
		bs.set[i] = 0
	}
	bs.bitcnt = 0
}

func (bs *BitSet) Data() []byte {
	return bs.set
}

func (bs *BitSet) String() string {
	w := new(bytes.Buffer)
	_, _ = w.WriteString(fmt.Sprintf("BitSet<%d>:[Cardinality=%d]", bs.size, bs.bitcnt))
	if bs.storage != nil {
		_, _ = w.WriteString("[Allocated]")
	}
	_ = w.WriteByte('[')
	cnt := 0
	for x := 0; x < bs.size; x++ {
		if cnt >= 20 || cnt >= bs.bitcnt {
			break
		}
		if bs.Contains(x) {
			cnt++
			_, _ = w.WriteString(fmt.Sprintf("%d", x))
			_ = w.WriteByte(',')
		}
	}
	if cnt < bs.bitcnt {
		_, _ = w.WriteString("...")
	}
	_ = w.WriteByte(']')
	return w.String()
}
