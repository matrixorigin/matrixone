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

package mongodb

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

// ClientRetirement describes a committed catalog change. ConnectionID zero
// means the entire account. For one connection, VersionExclusive zero means
// DROP and a non-zero value means ALTER (retire older generations only).
type ClientRetirement struct {
	AccountID        uint32
	ConnectionID     uint64
	VersionExclusive uint64
}

func (r ClientRetirement) Apply(pool *ClientPool) error {
	if pool == nil {
		return nil
	}
	if r.ConnectionID == 0 {
		return pool.RetireAccount(r.AccountID)
	}
	if r.VersionExclusive == 0 {
		return pool.RetireConnection(r.AccountID, r.ConnectionID)
	}
	return pool.RetireBefore(r.AccountID, r.ConnectionID, r.VersionExclusive)
}

type QueryMessageClient interface {
	ServiceID() string
	NewRequest(query.CmdMethod) *query.Request
	SendMessage(context.Context, string, *query.Request) (*query.Response, error)
	Release(*query.Response)
}

// ClusterRemoteClientRetirer broadcasts a committed retirement to every
// other CN. Delivery is best effort: catalog validation remains authoritative
// if one old CN is unavailable, and a remote failure must not turn a committed
// DDL into a client-visible failure.
type ClusterRemoteClientRetirer struct {
	Cluster     clusterservice.MOCluster
	QueryClient QueryMessageClient
	Timeout     time.Duration
}

type RemoteClientRetirer interface {
	Retire(context.Context, ClientRetirement)
}

const DefaultClientRetirementQueueCapacity = 256

// ClientRetirementQueue moves best-effort local disconnects and cluster fanout
// off the post-commit path. Its bounded channel prevents a slow/unavailable CN
// from turning restore or DDL churn into unbounded goroutines or memory.
type ClientRetirementQueue struct {
	pool   *ClientPool
	remote RemoteClientRetirer
	jobs   chan ClientRetirement
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func NewClientRetirementQueue(pool *ClientPool, remote RemoteClientRetirer, capacity int) *ClientRetirementQueue {
	if capacity <= 0 {
		capacity = DefaultClientRetirementQueueCapacity
	}
	ctx, cancel := context.WithCancel(context.Background())
	queue := &ClientRetirementQueue{
		pool: pool, remote: remote, jobs: make(chan ClientRetirement, capacity),
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	go queue.run()
	return queue
}

// Submit never waits for remote I/O or client Disconnect. False means the
// best-effort queue is stopping or saturated; catalog generation validation
// remains the correctness authority in either case.
func (q *ClientRetirementQueue) Submit(retirement ClientRetirement) bool {
	if q == nil {
		return false
	}
	select {
	case <-q.ctx.Done():
		return false
	default:
	}
	select {
	case q.jobs <- retirement:
		return true
	case <-q.ctx.Done():
		return false
	default:
		return false
	}
}

func (q *ClientRetirementQueue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.ctx.Done():
			return
		case retirement := <-q.jobs:
			_ = retirement.Apply(q.pool)
			if q.remote != nil {
				q.remote.Retire(q.ctx, retirement)
			}
		}
	}
}

func (q *ClientRetirementQueue) Close(ctx context.Context) error {
	if q == nil {
		return nil
	}
	q.once.Do(q.cancel)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (r ClusterRemoteClientRetirer) Retire(ctx context.Context, retirement ClientRetirement) {
	if r.Cluster == nil || r.QueryClient == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	self := r.QueryClient.ServiceID()
	targets := make([]string, 0)
	r.Cluster.GetCNService(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		if strings.TrimSpace(cn.QueryAddress) != "" && (self == "" || cn.ServiceID != self) {
			targets = append(targets, cn.QueryAddress)
		}
		return true
	})

	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, address := range targets {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			request := r.QueryClient.NewRequest(query.CmdMethod_MongoDBClientRetire)
			request.MongoDBClientRetireRequest = query.MongoDBClientRetireRequest{
				AccountID: retirement.AccountID, ConnectionID: retirement.ConnectionID,
				VersionExclusive: retirement.VersionExclusive,
			}
			resp, err := r.QueryClient.SendMessage(sendCtx, address, request)
			if err == nil && resp != nil {
				r.QueryClient.Release(resp)
			}
		}(address)
	}
	wg.Wait()
}
