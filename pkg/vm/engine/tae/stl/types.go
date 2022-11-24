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

package stl

import (
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func Sizeof[T any]() int {
	var v T
	return int(unsafe.Sizeof(v))
}

func SizeOfMany[T any](cnt int) int {
	var v T
	return int(unsafe.Sizeof(v)) * cnt
}

type Bytes struct {
	// Specify type size
	// Positive if it is fixed type
	// Negtive if it is varlen type
	TypeSize int

	// Specify whether it retains a window
	AsWindow bool
	// Window offset and length
	WinOffset int
	WinLength int

	// Used only when IsFixedType is false
	// Header store data if the size is less than VarlenaSize
	Header []types.Varlena

	// When IsFixedType is true, here is the data storage
	// When IsFixedType is false, here is the data storage for big data
	// When AsWindow is true, here stores all big data
	Storage []byte
}

type Vector[T any] interface {
	// Close free the vector allocated memory
	// Caller must call Close() or a memory leak will occur
	Close()

	// Clone deep copy data from offset to offset+length and create a new vector
	Clone(offset, length int, allocator ...*mpool.MPool) Vector[T]

	// ReadBytes reads a serialized buffer and initializes the vector using the buf
	// as its initial contents.

	// If share is true, vector release allocated memory and use the buf and its data storage
	// If share is false, vector will copy the data from buf to its own data storage
	ReadBytes(data *Bytes, share bool)

	// Reset resets the buffer to be empty
	// but it retains the underlying storage for use by future writes
	Reset()

	// IsView returns true if the vector shares the data storage with external buffer
	IsView() bool

	// TODO
	Bytes() *Bytes
	WindowAsBytes(offset, length int) *Bytes

	// Data returns the underlying data storage buffer
	// For Vector[[]byte], it only returns the data buffer
	Data() []byte
	// DataWindow returns a data window [offset, offset+length)
	DataWindow(offset, length int) []byte
	// Slice returns the underlying data storage of type T
	Slice() []T
	SliceWindow(offset, length int) []T

	// Get returns the specified element at i
	// Note: If T is []byte, make sure not to use v after the vector is closed
	Get(i int) (v T)
	// Append appends a element into the vector
	// If the prediction length is large than Capacity, it will cause the underlying memory reallocation.
	// Reallocation:
	// 1. Apply a new memory node from allocator
	// 2. Copy existing data into new buffer
	// 3. Swap owned memory node
	// 4. Free old memory node
	Append(v T)
	// Append appends many elements into the vector
	AppendMany(vals ...T)
	// Append updates a element at i to a new value
	// For T=[]byte, Update may introduce a underlying memory reallocation
	Update(i int, v T)
	// Delete deletes a element at i
	Delete(i int) (deleted T)
	// BatchDelete delete rows from rowGen
	// cnt specifies the total delete count
	BatchDelete(rowGen common.RowGen, cnt int)
	BatchDeleteInts(sels ...int)
	BatchDeleteUint32s(sels ...uint32)

	// Returns the underlying memory allocator
	GetAllocator() *mpool.MPool
	// Returns the capacity, which is always >= Length().
	// It is related to the number of elements. Same as C++ std::vector::capacity
	Capacity() int
	// Returns the number of elements in the vertor
	Length() int
	// Return the space allocted
	Allocated() int

	String() string
	Desc() string

	// WriteTo writes data to w until the buffer is drained or an error occurs
	WriteTo(io.Writer) (int64, error)
	// ReadFrom reads data from r until EOF and appends it to the buffer, growing
	// the buffer as needed.
	ReadFrom(io.Reader) (int64, error)
	InitFromSharedBuf(buf []byte) (int64, error)
}
