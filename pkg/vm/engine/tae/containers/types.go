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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Options struct {
	Capacity  int
	Allocator *mpool.MPool
}

type ItOp = func(v any, isNull bool, row int) error
type ItOpT[T any] func(v T, isNull bool, row int) error

// type ItBytesOp func(v []byte, isNull bool, row int) error

// Vector is the project-wide vector; TAE used to wrap it. The functions in
// vector.go are what the wrapper's methods were.
type Vector = *cnVector.Vector

type Batch struct {
	Attrs   []string
	Vecs    []Vector
	Deletes *nulls.Nulls
	Nameidx map[string]int
	Pool    *VectorPool
	// DataRelease is called to free external data buffers (e.g. fileservice IOVectors)
	// when the batch was loaded with zero-copy (needCopy=false).
	DataRelease func()
}

// BatchSplitter is used to split a batch into several batches
// with the same size.
type BatchSplitter struct {
	internal  *Batch
	sliceSize int
	offset    int
}

type BatchWithVersion struct {
	*Batch
	Seqnums    []uint16
	NextSeqnum uint16
	Version    uint32
}
