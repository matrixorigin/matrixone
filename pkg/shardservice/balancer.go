// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

var (
	defaultBalanceInterval = time.Second
)

type balancer struct {
	env       Env
	allocateC chan uint64
	stopper   *stopper.Stopper

	mu struct {
		sync.RWMutex
		shards    map[uint64]*shards
		operators map[string][]Operator
		cns       map[string]*cn
	}

	options struct {
		maxDownTime time.Duration
	}
}

func (b *balancer) Add(table uint64) error {
	v, err := b.env.GetTableShards(table)
	if err != nil {
		return err
	}
	if v.Policy == pb.Policy_None {
		return nil
	}
	if v.ShardsCount == 0 {
		panic("shards count is 0")
	}

	shards := &shards{metadata: v}
	shards.binds = make([]pb.TableShardBind, 0, v.ShardsCount)
	for i := uint32(0); i < v.ShardsCount; i++ {
		shards.binds = append(shards.binds,
			pb.TableShardBind{
				BindVersion:   0,
				ShardsVersion: v.Version,
				ShardID:       uint64(i),
			})
	}
	if v.Policy == pb.Policy_Partition {
		ids, err := b.env.GetPartitionIDs(table)
		if err != nil {
			return err
		}
		if len(ids) != len(shards.binds) {
			panic("partition and shard count not match")
		}
		for i, id := range ids {
			shards.binds[i].ShardID = id
		}
	}

	add := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		old, ok := b.mu.shards[table]
		if !ok {
			b.mu.shards[table] = shards
			return true
		}
		if old.metadata.Version >= v.Version {
			return false
		}

		b.deleteShardsLocked(table, old)
		b.mu.shards[table] = shards
		return true
	}

	if add() {
		getLogger().Info("shards added",
			zap.Uint64("table", table),
			tableShardsField("shards", v))
		b.addToAllocate(table)
	}
	return nil
}

func (b *balancer) Delete(table uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	old, ok := b.mu.shards[table]
	if !ok {
		return nil
	}
	b.deleteShardsLocked(table, old)
	return nil
}

func (b *balancer) GetBinds(table uint64) ([]pb.TableShardBind, error) {
	return nil, nil
}

func (b *balancer) Balance(table uint64) error {
	return nil
}

func (b *balancer) deleteShardsLocked(
	table uint64,
	value *shards,
) {
	getLogger().Info("remove shard binds",
		zap.Uint64("table", table),
		tableShardsField("shards", value.metadata),
		tableShardBindsField("binds", value.binds))

	for _, bind := range value.binds {
		if bind.CN == "" {
			continue
		}
		b.addOperatorLocked(bind.CN, newDeleteOp(bind))
	}
}

func (b *balancer) addOperatorLocked(
	cn string,
	ops ...Operator,
) {
	b.mu.operators[cn] = append(b.mu.operators[cn], ops...)
}

func (b *balancer) addToAllocate(table uint64) {
	b.allocateC <- table
}

func (b *balancer) handleEvents(ctx context.Context) {
	timer := time.NewTimer(defaultBalanceInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case table := <-b.allocateC:
			b.doAllocate(table)
		case <-timer.C:
			b.checkDownCN()
			b.allocate()
			b.balance()
			timer.Reset(defaultBalanceInterval)
		}
	}
}

func (b *balancer) checkDownCN() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for _, cn := range b.mu.cns {
		if now.Sub(cn.last) < b.options.maxDownTime {
			continue
		}
		if b.env.HasCN(cn.metadata.ServiceID) {
			getLogger().Warn("CN is down too long, but still in hakeeper",
				zap.Duration("down-time", now.Sub(cn.last)),
				zap.String("serviceID", cn.metadata.ServiceID))
			continue
		}
		cn.state = down
	}
}

func (b *balancer) moveDownShards() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, cn := range b.mu.cns {
		if cn.state == down {

		}
	}
}

func (b *balancer) allocate() {
	var targets []uint64
	b.mu.RLock()
	for table, shards := range b.mu.shards {
		if !shards.allocated {
			targets = append(targets, table)
		}
	}
	b.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	for _, table := range targets {
		b.doAllocate(table)
	}
}

func (b *balancer) doAllocate(table uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	shards, ok := b.mu.shards[table]
	if !ok || shards.allocated {
		return
	}

	i := 0
	cns := b.getAvailableCNsLocked(shards)
	if len(cns) == 0 {
		return
	}
	for _, bind := range shards.binds {
		if bind.CN == "" {
			bind.CN = cns[i%len(cns)].metadata.ServiceID
			b.addOperatorLocked(bind.CN, newAddOp(bind))
			i++
		}
	}
	shards.allocated = true
}

func (b *balancer) balance() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// currentCNs := make(map[string]struct{})
	// clean := func() {
	// 	for k := range currentCNs {
	// 		delete(currentCNs, k)
	// 	}
	// }
	// for table, shards := range b.mu.shards {
	// 	cns := b.getAvailableCNsLocked(shards)
	// 	clean()
	// 	for _, cn := range cns {
	// 		currentCNs[cn] = struct{}{}
	// 	}
	// }
}

func (b *balancer) getAvailableCNsLocked(shards *shards) []*cn {
	var cns []*cn
	for _, cn := range b.mu.cns {
		if cn.available(shards.metadata.TenantID) {
			cns = append(cns, cn)
		}
	}
	sort.Slice(cns, func(i, j int) bool {
		return cns[i].metadata.ServiceID < cns[j].metadata.ServiceID
	})
	return cns
}

func newDeleteOp(bind pb.TableShardBind) Operator {
	return Operator{
		CreateAt: time.Now(),
		Cmd: pb.Cmd{
			Type:           pb.CmdType_DeleteShard,
			TableShardBind: bind,
		},
	}
}

func newAddOp(bind pb.TableShardBind) Operator {
	return Operator{
		CreateAt: time.Now(),
		Cmd: pb.Cmd{
			Type:           pb.CmdType_AddShard,
			TableShardBind: bind,
		},
	}
}
