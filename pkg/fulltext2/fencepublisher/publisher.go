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

package fencepublisher

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
)

const (
	queueCapacity = 1024
	workerCount   = 4
	rpcParallel   = 16
	rpcTimeout    = 2 * time.Second
)

var retryDelays = [...]time.Duration{0, 100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second, 10 * time.Second, 30 * time.Second}

type pendingFence struct {
	identity   fulltext2.CacheIdentity
	generation fulltext2.Generation
}

type Publisher struct {
	serviceID string
	client    qclient.QueryClient
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	rpcSem    chan struct{}

	mu      sync.Mutex
	pending map[string]pendingFence
	active  map[string]fulltext2.Generation
	closed  bool
	wg      sync.WaitGroup
	nodesFn func(context.Context) ([]metadata.CNService, error)
	sendFn  func(pendingFence, metadata.CNService) bool
	delays  []time.Duration
}

func New(serviceID string, client qclient.QueryClient) *Publisher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		serviceID: serviceID,
		client:    client,
		ctx:       ctx,
		cancel:    cancel,
		wake:      make(chan struct{}, workerCount),
		rpcSem:    make(chan struct{}, rpcParallel),
		pending:   make(map[string]pendingFence),
		active:    make(map[string]fulltext2.Generation),
		delays:    retryDelays[:],
	}
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Publisher) Enqueue(identity fulltext2.CacheIdentity, generation fulltext2.Generation) {
	key := identity.Key()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if current, ok := p.pending[key]; ok {
		if generation.AtLeast(current.generation) {
			p.pending[key] = pendingFence{identity: identity, generation: generation}
		}
		p.mu.Unlock()
		p.signal()
		return
	}
	if current, ok := p.active[key]; ok {
		if generation.AtLeast(current) && generation != current {
			p.pending[key] = pendingFence{identity: identity, generation: generation}
		}
		p.mu.Unlock()
		p.signal()
		return
	}
	if len(p.pending)+len(p.active) >= queueCapacity {
		p.mu.Unlock()
		logutil.Warnf("[ftv2-fence] publisher queue full; pull fallback required for index=%s", identity.StorageTable)
		return
	}
	p.pending[key] = pendingFence{identity: identity, generation: generation}
	p.mu.Unlock()
	p.signal()
}

func (p *Publisher) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Publisher) pop() (pendingFence, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, item := range p.pending {
		if _, active := p.active[key]; active {
			continue
		}
		delete(p.pending, key)
		p.active[key] = item.generation
		return item, true
	}
	return pendingFence{}, false
}

func (p *Publisher) finish(item pendingFence) {
	key := item.identity.Key()
	p.mu.Lock()
	if p.active[key] == item.generation {
		delete(p.active, key)
	}
	_, pending := p.pending[key]
	p.mu.Unlock()
	if pending {
		p.signal()
	}
}

func (p *Publisher) superseded(item pendingFence) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	newer, ok := p.pending[item.identity.Key()]
	return ok && newer.generation.AtLeast(item.generation) && newer.generation != item.generation
}

func (p *Publisher) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.wake:
			item, ok := p.pop()
			if !ok {
				continue
			}
			p.signal()
			p.broadcast(item)
			p.finish(item)
		}
	}
}

func (p *Publisher) nodes(ctx context.Context) ([]metadata.CNService, error) {
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, p.serviceID)
	if err != nil {
		return nil, err
	}
	nodes := make([]metadata.CNService, 0, 4)
	err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster, clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		if cn.WorkState != metadata.WorkState_Working && cn.WorkState != metadata.WorkState_Unknown {
			return true
		}
		if cn.QueryAddress != "" {
			nodes = append(nodes, cn)
		}
		return true
	})
	return nodes, err
}

func (p *Publisher) request(item pendingFence) *querypb.Request {
	req := p.client.NewRequest(querypb.CmdMethod_Fulltext2CacheFence)
	req.Fulltext2CacheFenceRequest = querypb.Fulltext2CacheFenceRequest{
		AccountID:     item.identity.AccountID,
		Database:      item.identity.Database,
		StorageTable:  item.identity.StorageTable,
		MetadataTable: item.identity.MetadataTable,
		BaseTimestamp: item.generation.BaseTimestamp,
		TailChunk:     item.generation.TailChunk,
	}
	return req
}

func (p *Publisher) send(item pendingFence, cn metadata.CNService) bool {
	ctx, cancel := context.WithTimeout(p.ctx, rpcTimeout)
	defer cancel()
	resp, err := p.client.SendMessage(ctx, cn.QueryAddress, p.request(item))
	if err != nil {
		return false
	}
	defer p.client.Release(resp)
	return ackAccepts(resp.Fulltext2CacheFenceResponse, item.generation)
}

func ackAccepts(ack querypb.Fulltext2CacheFenceResponse, generation fulltext2.Generation) bool {
	if !ack.EvictionClaimed {
		return false
	}
	return (fulltext2.Generation{
		BaseTimestamp: ack.RequiredBaseTimestamp,
		TailChunk:     ack.RequiredTailChunk,
	}).AtLeast(generation)
}

func (p *Publisher) broadcast(item pendingFence) {
	acked := make(map[string]struct{})
	var ackMu sync.Mutex
	for _, delay := range p.delays {
		if p.superseded(item) {
			return
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-p.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if p.superseded(item) {
				return
			}
		}
		nodesFn := p.nodesFn
		if nodesFn == nil {
			nodesFn = p.nodes
		}
		inventoryCtx, inventoryCancel := context.WithTimeout(p.ctx, rpcTimeout)
		nodes, err := nodesFn(inventoryCtx)
		inventoryCancel()
		if err != nil {
			continue
		}
		var wg sync.WaitGroup
		for _, cn := range nodes {
			ackMu.Lock()
			if _, ok := acked[cn.ServiceID]; ok {
				ackMu.Unlock()
				continue
			}
			ackMu.Unlock()
			select {
			case <-p.ctx.Done():
				wg.Wait()
				return
			case p.rpcSem <- struct{}{}:
			}
			wg.Add(1)
			go func(cn metadata.CNService) {
				defer wg.Done()
				defer func() { <-p.rpcSem }()
				sendFn := p.sendFn
				if sendFn == nil {
					sendFn = p.send
				}
				if sendFn(item, cn) {
					ackMu.Lock()
					acked[cn.ServiceID] = struct{}{}
					ackMu.Unlock()
				}
			}(cn)
		}
		wg.Wait()
		allAcked := true
		for _, cn := range nodes {
			ackMu.Lock()
			if _, ok := acked[cn.ServiceID]; !ok {
				allAcked = false
				ackMu.Unlock()
				break
			}
			ackMu.Unlock()
		}
		if allAcked {
			return
		}
	}
	if p.ctx.Err() == nil {
		logutil.Warnf("[ftv2-fence] broadcast exhausted; pull fallback required for index=%s", item.identity.StorageTable)
	}
}

func (p *Publisher) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
}

var _ fulltext2.FencePublisher = (*Publisher)(nil)
