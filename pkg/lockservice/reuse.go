// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func init() {
	reuse.CreatePool[waiter](
		newWaiter,
		func(w *waiter) {
			w.reset()
		},
		reuse.DefaultOptions[waiter]().
			WithReleaseFunc(func(w *waiter) {
				close(w.c)
			}).
			WithEnableChecker())

	reuse.CreatePool[lockContext](
		func() *lockContext { return &lockContext{} },
		func(c *lockContext) { *c = lockContext{} },
		reuse.DefaultOptions[lockContext]().
			WithEnableChecker())

	reuse.CreatePool[activeTxn](
		func() *activeTxn {
			txn := &activeTxn{
				lockHolders: make(map[uint32]*tableLockHolder),
			}
			txn.RWMutex = &sync.RWMutex{}
			return txn
		},
		func(txn *activeTxn) { txn.reset() },
		reuse.DefaultOptions[activeTxn]().
			WithEnableChecker())

	reuse.CreatePool[pb.Request](
		func() *pb.Request {
			return &pb.Request{}
		},
		func(v *pb.Request) { v.Reset() },
		reuse.DefaultOptions[pb.Request]())

	reuse.CreatePool[pb.Response](
		func() *pb.Response {
			return &pb.Response{}
		},
		func(v *pb.Response) { v.Reset() },
		reuse.DefaultOptions[pb.Response]())

	reuse.CreatePool[cowSlice](
		func() *cowSlice {
			return &cowSlice{
				v:  &atomic.Uint64{},
				fs: &atomic.Value{},
			}
		},
		func(cs *cowSlice) {
			cs.v.Store(0)
			cs.mustGet().unref()
		},
		reuse.DefaultOptions[cowSlice]().
			WithEnableChecker())
}
