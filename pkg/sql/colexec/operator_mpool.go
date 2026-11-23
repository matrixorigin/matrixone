// Copyright 2026 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// NewOperatorMPool returns the private pool of an operator that buffers a
// large amount of data (sort runs, hash tables, ...). Every vector of that
// buffer is created with, and operated through, this pool; its stats account
// the operator's buffered memory. It takes the process pool's cap. Other
// operators use proc.Mp().
func NewOperatorMPool(proc *process.Process, tag string) (*mpool.MPool, error) {
	var capacity int64
	if base := proc.Mp(); base != nil {
		if c := base.Cap(); c >= mpool.MB && c <= mpool.GlobalCap() {
			capacity = c
		}
	}
	return mpool.NewMPool(tag, capacity, mpool.NoFixed)
}

// FreeOperatorMPool deletes an operator pool after every vector of its buffer
// has been freed.
func FreeOperatorMPool(mp **mpool.MPool) {
	if *mp != nil {
		mpool.DeleteMPool(*mp)
		*mp = nil
	}
}
