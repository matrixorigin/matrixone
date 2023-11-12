// Copyright 2021 - 2022 Matrix Origin
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

// A few allocators for TAE
package common

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// A few allocators for TAE
var DefaultAllocator *mpool.MPool
var MutMemAllocator *mpool.MPool
var SmallAllocator *mpool.MPool
var LogtailAllocator *mpool.MPool
var CheckpointAllocator *mpool.MPool
var MergeAllocator *mpool.MPool
var WorkspaceAllocator *mpool.MPool
var DebugAllocator *mpool.MPool

// init with zero fixed pool, for test.
func init() {
	InitTAEMPool()
}

// tn service call this during start up, to get a real cached pool.
var once sync.Once

func InitTAEMPool() {
	onceBody := func() {
		var err error
		mpool.DeleteMPool(DefaultAllocator)
		if DefaultAllocator, err = mpool.NewMPool("tae_default", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(MutMemAllocator)
		if MutMemAllocator, err = mpool.NewMPool("tae_mutable", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(SmallAllocator)
		if SmallAllocator, err = mpool.NewMPool("tae_small", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(LogtailAllocator)
		if LogtailAllocator, err = mpool.NewMPool("tae_logtail", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(CheckpointAllocator)
		if CheckpointAllocator, err = mpool.NewMPool("tae_checkpoint", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(MergeAllocator)
		if MergeAllocator, err = mpool.NewMPool("tae_merge", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(WorkspaceAllocator)
		if WorkspaceAllocator, err = mpool.NewMPool("tae_workspace", 0, mpool.NoFixed); err != nil {
			panic(err)
		}

		mpool.DeleteMPool(DebugAllocator)
		if DebugAllocator, err = mpool.NewMPool("tae_debug", 0, mpool.NoFixed); err != nil {
			panic(err)
		}
	}
	once.Do(onceBody)
}
