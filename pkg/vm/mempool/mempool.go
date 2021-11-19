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

package mempool

import (
	"github.com/matrixorigin/matrixone/pkg/vm/malloc"
)

type Mempool struct {
	Buckets []*Bucket
}

type Bucket struct {
	Size int
	Page *Page
}

var sizes = func() (ret []int) {
	for size := 8; size <= PageSize; size *= 2 {
		ret = append(ret, size)
	}
	return
}()

func New() *Mempool {
	m := &Mempool{
		Buckets: make([]*Bucket, len(sizes)),
	}
	for i, size := range sizes {
		m.Buckets[i] = &Bucket{
			Size: size,
		}
	}
	return m
}

func (m *Mempool) Alloc(size int) (ret []byte) {
	if size > PageSize {
		return malloc.Malloc(size)
	}
	for _, bucket := range m.Buckets {
		if bucket.Size < size {
			continue
		}
		if bucket.Page == nil {
			bucket.Page = getPage()
		}
		newLen := len(bucket.Page.Buf) + size
		if newLen > cap(bucket.Page.Buf) {
			page := newPage()
			page.Next = bucket.Page
			bucket.Page = page
			newLen = size
		}
		ret = bucket.Page.Buf[len(bucket.Page.Buf):newLen]
		bucket.Page.Buf = bucket.Page.Buf[:newLen]
		return
	}
	panic("impossible")
}

func (m *Mempool) Free(data []byte) {
}

func (m *Mempool) Release() {
	for _, bucket := range m.Buckets {
		releasePages(bucket.Page)
	}
}

func Realloc(data []byte, size int64) int64 {
	if data == nil {
		return size
	}
	n := int64(cap(data))
	if size <= n {
		return n
	}
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(data) < 1024 {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	return newcap
}
