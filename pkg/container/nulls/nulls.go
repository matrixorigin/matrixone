package nulls

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func Or(n, m, r *Nulls) {
	if (n == nil || (n != nil && n.Np == nil)) && m != nil && m.Np != nil {
		if r.Np == nil {
			r.Np = roaring.NewBitmap()
		}
		r.Np.Or(n.Np)
		return
	}
	if (m == nil || m != nil && m.Np == nil) && n != nil && n.Np != nil {
		if r.Np == nil {
			r.Np = roaring.NewBitmap()
		}
		r.Np.Or(m.Np)
		return
	}
	if m != nil && m.Np != nil && n != nil && n.Np != nil {
		if r.Np == nil {
			r.Np = roaring.NewBitmap()
		}
		r.Np.Or(n.Np)
		r.Np.Or(m.Np)
	}
}

func (n *Nulls) Reset() {
	if n.Np != nil {
		n.Np.Clear()
	}
}

func (n *Nulls) Any() bool {
	if n.Np == nil {
		return false
	}
	return !n.Np.IsEmpty()
}

func (n *Nulls) Size() int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.GetSizeInBytes())
}

func (n *Nulls) Length() int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.GetCardinality())
}

func (n *Nulls) String() string {
	if n.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", n.Np.ToArray())
}

func (n *Nulls) Contains(row uint64) bool {
	if n.Np != nil {
		return n.Np.Contains(row)
	}
	return false
}

func (n *Nulls) Add(rows ...uint64) {
	if n.Np == nil {
		n.Np = roaring.BitmapOf(rows...)
		return
	}
	n.Np.AddMany(rows)
}

func (n *Nulls) Del(rows ...uint64) {
	if n.Np == nil {
		return
	}
	for _, row := range rows {
		n.Np.Remove(row)
	}
}

func (n *Nulls) Set(m *Nulls) {
	if m != nil || m.Np != nil {
		if n.Np == nil {
			n.Np = roaring.NewBitmap()
		}
		n.Np.Or(m.Np)
	}
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

func (n *Nulls) RemoveRange(start, end uint64) {
	if n.Np != nil {
		n.Np.RemoveRange(start, end)
	}
}

func (n *Nulls) Range(start, end uint64, m *Nulls) *Nulls {
	switch {
	case n.Np == nil && m.Np == nil:
		m.Np.Clear()
	case n.Np != nil && m.Np == nil:
		m.Np = roaring.NewBitmap()
		for ; start < end; start++ {
			if n.Np.Contains(start) {
				m.Np.Add(start)
			}
		}
	case n.Np != nil && m.Np != nil:
		m.Np.Clear()
		for ; start < end; start++ {
			if n.Np.Contains(start) {
				m.Np.Add(start)
			}
		}
	}
	return m
}

func (n *Nulls) Filter(sels []int64) *Nulls {
	if n.Np == nil {
		return n
	}
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&sels))
	sp := *(*[]uint64)(unsafe.Pointer(&hp))
	np := roaring.NewBitmap()
	for i, sel := range sp {
		if n.Np.Contains(sel) {
			np.Add(uint64(i))
		}
	}
	n.Np = np
	return n
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
		return n
	case n.Np == nil && m.Np == nil:
		return n
	case n.Np != nil && m.Np == nil:
		return n
	case n.Np == nil && m.Np != nil:
		return m
	default:
		n.Np.Or(m.Np)
		return n
	}
}
