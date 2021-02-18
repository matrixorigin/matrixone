package nulls

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/pilosa/pilosa/roaring"
)

func (n *Nulls) Any() bool {
	if n.Np == nil {
		return false
	}
	return n.Np.Any()
}

func (n *Nulls) Size() int {
	if n.Np == nil {
		return 0
	}
	return n.Np.Size()
}

func (n *Nulls) Length() int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.Count())
}

func (n *Nulls) String() string {
	if n.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", n.Np.Slice())
}

func (n *Nulls) Contains(row uint64) bool {
	if n.Np != nil {
		return n.Np.Contains(row)
	}
	return false
}

func (n *Nulls) Add(rows ...uint64) {
	if n.Np == nil {
		n.Np = roaring.NewBitmap(rows...)
		return
	}
	n.Np.DirectAddN(rows...)
}

func (n *Nulls) Del(rows ...uint64) {
	if n.Np == nil {
		return
	}
	n.Np.DirectRemoveN(rows...)
}

func (n *Nulls) FilterCount(sels []int64) int {
	var cnt int

	if n.Np == nil {
		return cnt
	}
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&sels))
	sp := *(*[]uint64)(unsafe.Pointer(&hp))
	for _, sel := range sp {
		if n.Np.Contains(sel) {
			cnt++
		}
	}
	return cnt
}

func (n *Nulls) Range(start, end uint64) *Nulls {
	if n.Np == nil {
		return &Nulls{}
	}
	np := roaring.NewBitmap()
	for ; start < end; start++ {
		if n.Np.Contains(start) {
			np.DirectAdd(start)
		}
	}
	return &Nulls{np}
}

func (n *Nulls) Filter(sels []int64) *Nulls {
	if n.Np == nil {
		return &Nulls{}
	}
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&sels))
	sp := *(*[]uint64)(unsafe.Pointer(&hp))
	np := roaring.NewBitmap()
	for i, sel := range sp {
		if n.Np.Contains(sel) {
			np.DirectAdd(uint64(i))
		}
	}
	return &Nulls{np}
}

func (n *Nulls) Show() ([]byte, error) {
	var buf bytes.Buffer

	if n.Np == nil {
		return nil, nil
	}
	if _, err := n.Np.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (n *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	n.Np = roaring.NewBitmap()
	if err := n.Np.UnmarshalBinary(data); err != nil {
		n.Np = nil
		return err
	}
	return nil
}

func (n *Nulls) Or(m *Nulls) *Nulls {
	switch {
	case m == nil:
		return &Nulls{n.Np}
	case n.Np == nil || m.Np == nil:
		return &Nulls{}
	case n.Np != nil || m.Np == nil:
		return &Nulls{n.Np}
	case n.Np == nil || m.Np != nil:
		return &Nulls{m.Np}
	default:
		return &Nulls{n.Np.Union(m.Np)}
	}
}
