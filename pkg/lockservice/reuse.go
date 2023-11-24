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
		resetWaiter,
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
			txn := &activeTxn{holdLocks: make(map[uint64]*cowSlice)}
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
		reuse.DefaultOptions[pb.Request]().
			WithEnableChecker())

	reuse.CreatePool[pb.Response](
		func() *pb.Response {
			return &pb.Response{}
		},
		func(v *pb.Response) { v.Reset() },
		reuse.DefaultOptions[pb.Response]().
			WithEnableChecker())

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
