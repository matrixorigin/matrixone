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

package common

import "sync/atomic"

type IdAllocator struct {
	id uint64
}

func (id *IdAllocator) Get() uint64 {
	return atomic.LoadUint64(&id.id)
}

func (id *IdAllocator) Alloc() uint64 {
	return atomic.AddUint64(&id.id, uint64(1))
}

func (id *IdAllocator) Set(val uint64) {
	atomic.StoreUint64(&id.id, val)
}
