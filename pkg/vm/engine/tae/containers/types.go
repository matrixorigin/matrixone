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

package containers

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
)

type Options = containers.Options
type Bytes = stl.Bytes

// var DefaultAllocator = alloc.NewAllocator(int(common.G) * 100)

type ItOp = func(v any, row int) error

type VectorView interface {
	IsView() bool
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	Data() []byte
	Bytes() *Bytes
	Slice() any
	DataWindow(offset, length int) []byte
	Get(i int) any

	Length() int
	Capacity() int
	Allocated() int
	GetAllocator() *mpool.MPool
	GetType() types.Type
	String() string

	Foreach(op ItOp, sels *roaring.Bitmap) error
	ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error

	WriteTo(w io.Writer) (int64, error)
}

type Vector interface {
	VectorView
	Reset()
	ResetWithData(bs *Bytes, nulls *roaring64.Bitmap)
	GetView() VectorView
	Update(i int, v any)
	Delete(i int)
	DeleteBatch(*roaring.Bitmap)
	Append(v any)
	AppendMany(vs ...any)
	AppendNoNulls(s any)
	Extend(o Vector)
	ExtendWithOffset(src Vector, srcOff, srcLen int)
	Compact(deletes *roaring.Bitmap)
	CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector

	Equals(o Vector) bool
	Window(offset, length int) Vector
	WriteTo(w io.Writer) (int64, error)
	ReadFrom(r io.Reader) (int64, error)

	ReadFromFile(common.IVFile, *bytes.Buffer) error

	Close()
}

type Batch struct {
	Attrs   []string
	Vecs    []Vector
	Deletes *roaring.Bitmap
	nameidx map[string]int
	// refidx  map[int]int
}
