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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Options struct {
	Capacity  int
	Allocator *mpool.MPool
	Data      *stl.Bytes
}

func (opts *Options) HasData() bool { return opts.Data != nil }
func (opts *Options) DataSize() int {
	if opts.Data == nil {
		return 0
	}
	return opts.Data.StorageSize()
}

type StdVector[T any] struct {
	alloc    *mpool.MPool
	node     []byte
	buf      []byte
	slice    []T
	capacity int
}

type StrVector[T any] struct {
	vdata *StdVector[types.Varlena]
	area  *StdVector[byte]
}

type Vector[T any] struct {
	stl.Vector[T]
}
