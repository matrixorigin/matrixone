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

package queryservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

// #28985: EvictVectorIndexCache must be dispatched OFF the connection's read loop. Its owned teardown
// (evictEntry/Destroy) completes synchronously and can outlive the caller's deadline; run inline
// (async=false) it would block goetty's doConnection, so an unrelated request on the same (default
// single) connection would queue behind a timed-out Evict. With async dispatch the connection stays
// readable while the teardown runs and the response is still written after cleanup.
//
// This exercises the RPC queueing edge the direct-cache cancellation test cannot: an owner outliving
// its caller deadline, and an unrelated request progressing before that owner is released.
func TestEvictVectorIndexCacheAsyncFreesConnection(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce, startOnce sync.Once
	// Always release the server-side teardown so the async handler goroutine cannot leak.
	defer releaseOnce.Do(func() { close(release) })

	runtime.RegisterVectorIndexCacheEvictor(func(ctx context.Context, key string) int64 {
		startOnce.Do(func() { close(started) })
		<-release // model an owned teardown that ignores the caller deadline
		return 1
	})
	defer runtime.RegisterVectorIndexCacheEvictor(nil)

	getProtocol := func(cli client.QueryClient) *pb.Request {
		req := cli.NewRequest(pb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &pb.GetProtocolVersionRequest{}
		return req
	}

	runTestWithQueryService(t, metadata.CNService{ServiceID: "s1"}, nil, func(cli client.QueryClient, addr string) {
		// Warm the single connection.
		wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer wcancel()
		_, err := cli.SendMessage(wctx, addr, getProtocol(cli))
		require.NoError(t, err)

		// Start Evict; the async handler blocks in the owner teardown well past its caller deadline.
		evictDone := make(chan struct{})
		go func() {
			defer close(evictDone)
			ectx, ecancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer ecancel()
			ereq := cli.NewRequest(pb.CmdMethod_EvictVectorIndexCache)
			ereq.EvictVectorIndexCache = pb.EvictVectorIndexCacheRequest{Key: "k"}
			// The caller times out (the teardown holds the response); we only care that this does
			// not wedge the connection for other requests.
			_, _ = cli.SendMessage(ectx, addr, ereq)
		}()
		<-started // the teardown is running and blocked

		// An unrelated request on the SAME connection must complete while Evict is still blocked.
		gctx, gcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer gcancel()
		_, err = cli.SendMessage(gctx, addr, getProtocol(cli))
		require.NoError(t, err, "unrelated request must not queue behind a blocked async Evict (#28985)")

		// The Evict caller deadline already fired; release the owner teardown and drain.
		releaseOnce.Do(func() { close(release) })
		<-evictDone
	})
}
