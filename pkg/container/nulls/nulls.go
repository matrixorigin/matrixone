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

package nulls

import (
	"bytes"
	"fmt"
	"unsafe"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func Or(n, m, r *Nulls) {
	if (n == nil || (n != nil && n.Np == nil)) && m != nil && m.Np != nil {
		if r.Np == nil {
			r.Np = roaring.NewBitmap()
		}
		r.Np.Or(m.Np)
		return
	}
	if (m == nil || m != nil && m.Np == nil) && n != nil && n.Np != nil {
		if r.Np == nil {
			r.Np = roaring.NewBitmap()
		}
		r.Np.Or(n.Np)
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

func Reset(n *Nulls) {
	if n.Np != nil {
		n.Np.Clear()
	}
}

func Any(n *Nulls) bool {
	if n.Np == nil {
		return false
	}
	return !n.Np.IsEmpty()
}

func Size(n *Nulls) int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.GetSizeInBytes())
}

func Length(n *Nulls) int {
	if n.Np == nil {
		return 0
	}
	return int(n.Np.GetCardinality())
}

func String(n *Nulls) string {
	if n.Np == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", n.Np.ToArray())
}

func Contains(n *Nulls, row uint64) bool {
	if n.Np != nil {
		return n.Np.Contains(row)
	}
	return false
}

func Add(n *Nulls, rows ...uint64) {
	if n.Np == nil {
		n.Np = roaring.BitmapOf(rows...)
		return
	}
	n.Np.AddMany(rows)
}

func Del(n *Nulls, rows ...uint64) {
	if n.Np == nil {
		return
	}
	for _, row := range rows {
		n.Np.Remove(row)
	}
}

func Set(n, m *Nulls) {
	if m != nil && m.Np != nil {
		if n.Np == nil {
			n.Np = roaring.NewBitmap()
		}
		n.Np.Or(m.Np)
	}
}

func FilterCount(n *Nulls, sels []int64) int {
	var cnt int
	if n.Np == nil {
		return cnt
	}
	var sp []uint64
	if len(sels) > 0 {
		sp = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	}
	for _, sel := range sp {
		if n.Np.Contains(sel) {
			cnt++
		}
	}
	return cnt
}

func RemoveRange(n *Nulls, start, end uint64) {
	if n.Np != nil {
		n.Np.RemoveRange(start, end)
	}
}

func Range(n *Nulls, start, end uint64, m *Nulls) *Nulls {
	switch {
	case n.Np == nil && m.Np == nil:
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

func Filter(n *Nulls, sels []int64) *Nulls {
	if n.Np == nil {
		return n
	}
	var sp []uint64
	if len(sels) > 0 {
		sp = unsafe.Slice((*uint64)(unsafe.Pointer(&sels[0])), cap(sels))[:len(sels)]
	}
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
