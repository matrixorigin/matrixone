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
	"bytes"
	"context"
	"sort"
	"sync"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

type txnClosureAdmission struct {
	token         chan struct{}
	refs          int
	txnID         []byte
	hash          uint64
	collisionNext *txnClosureAdmission
}

var txnClosureAdmissionPool = sync.Pool{
	New: func() any {
		return &txnClosureAdmission{token: make(chan struct{}, 1)}
	},
}

type txnClosureAdmissionGuard struct {
	service  *service
	single   *txnClosureAdmission
	refs     []*txnClosureAdmission
	acquired int
}

func (g *txnClosureAdmissionGuard) release() {
	if g.service == nil {
		return
	}
	if g.refs == nil {
		if g.acquired == 1 {
			<-g.single.token
		}
		g.service.unrefTxnClosureAdmission(g.single)
	} else {
		for idx := g.acquired - 1; idx >= 0; idx-- {
			<-g.refs[idx].token
		}
		for idx := len(g.refs) - 1; idx >= 0; idx-- {
			g.service.unrefTxnClosureAdmission(g.refs[idx])
		}
	}
	*g = txnClosureAdmissionGuard{}
}

type txnClosureAdmissionShard struct {
	sync.Mutex
	entries map[uint64]*txnClosureAdmission
}

// acquireTxnClosureAdmission serializes async, absent-generation and handoff
// closes before a source transaction mutex is acquired. Handoffs also acquire
// replacement transaction IDs; without that ordering, concurrent A -> B and
// B -> A transfers on different tables can each hold a source mutex while
// waiting for the other's replacement mutex. IDs are acquired in byte order,
// so overlapping closures serialize while unrelated transactions remain fully
// concurrent. A local synchronous close instead keeps its sole transaction
// mutex throughout. The fixed shards guard only short map operations; wait
// duration never occupies a shard-wide token.
const txnClosureAdmissionShards = 64

func txnClosureAdmissionHash(txnID []byte) uint64 {
	hash := uint64(14695981039346656037)
	for _, value := range txnID {
		hash ^= uint64(value)
		hash *= 1099511628211
	}
	return hash
}

func (s *service) refTxnClosureAdmission(txnID []byte) *txnClosureAdmission {
	hash := txnClosureAdmissionHash(txnID)
	shard := &s.txnClosureAdmissions[hash%txnClosureAdmissionShards]
	shard.Lock()
	defer shard.Unlock()
	if shard.entries == nil {
		shard.entries = make(map[uint64]*txnClosureAdmission)
	}
	entry := shard.entries[hash]
	for entry != nil && !bytes.Equal(entry.txnID, txnID) {
		entry = entry.collisionNext
	}
	if entry == nil {
		entry = txnClosureAdmissionPool.Get().(*txnClosureAdmission)
		if entry.refs != 0 || len(entry.token) != 0 ||
			len(entry.txnID) != 0 || entry.hash != 0 ||
			entry.collisionNext != nil {
			panic("BUG: dirty transaction closure admission from pool")
		}
		entry.txnID = append(entry.txnID[:0], txnID...)
		entry.hash = hash
		entry.collisionNext = shard.entries[hash]
		shard.entries[hash] = entry
	}
	entry.refs++
	return entry
}

func (s *service) unrefTxnClosureAdmission(
	entry *txnClosureAdmission,
) {
	shard := &s.txnClosureAdmissions[entry.hash%txnClosureAdmissionShards]
	shard.Lock()
	defer shard.Unlock()
	current := shard.entries[entry.hash]
	var previous *txnClosureAdmission
	for current != nil && current != entry {
		previous = current
		current = current.collisionNext
	}
	if current == nil || entry.refs <= 0 {
		panic("BUG: invalid transaction closure admission reference")
	}
	entry.refs--
	if entry.refs == 0 {
		if previous == nil {
			if entry.collisionNext == nil {
				delete(shard.entries, entry.hash)
			} else {
				shard.entries[entry.hash] = entry.collisionNext
			}
		} else {
			previous.collisionNext = entry.collisionNext
		}
		entry.txnID = entry.txnID[:0]
		entry.hash = 0
		entry.collisionNext = nil
		txnClosureAdmissionPool.Put(entry)
	}
}

func (s *service) acquireTxnClosureAdmission(
	ctx context.Context,
	sourceTxnID []byte,
	mutations []pb.ExtraMutation,
) (txnClosureAdmissionGuard, error) {
	if err := ctx.Err(); err != nil {
		return txnClosureAdmissionGuard{}, err
	}
	if len(mutations) == 0 {
		// Ordinary commit/rollback closes are frequent, while cross-transaction
		// handoffs are exceptional. Keep the same cancellable pre-mutex admission
		// contract without building maps, slices or closure objects on that hot
		// path. The pooled entry owns a copy of the ID while any admission waiter
		// still references it, so caller buffer reuse cannot corrupt the map key.
		entry := s.refTxnClosureAdmission(sourceTxnID)
		guard := txnClosureAdmissionGuard{
			service: s,
			single:  entry,
		}
		// The ordinary close path is overwhelmingly uncontended. Avoid the
		// general select machinery unless another close already owns this exact
		// transaction admission; cancellation is checked immediately after the
		// fast acquisition and remains part of the contended wait below.
		select {
		case entry.token <- struct{}{}:
			guard.acquired = 1
			if err := ctx.Err(); err != nil {
				guard.release()
				return txnClosureAdmissionGuard{}, err
			}
			return guard, nil
		default:
		}
		select {
		case entry.token <- struct{}{}:
			guard.acquired = 1
			if err := ctx.Err(); err != nil {
				guard.release()
				return txnClosureAdmissionGuard{}, err
			}
			return guard, nil
		case <-ctx.Done():
			guard.release()
			return txnClosureAdmissionGuard{}, ctx.Err()
		}
	}
	selected := make(map[string]struct{}, len(mutations)+1)
	selected[string(sourceTxnID)] = struct{}{}
	for idx := range mutations {
		if len(mutations[idx].ReplaceTo) > 0 {
			selected[string(mutations[idx].ReplaceTo)] = struct{}{}
		}
	}
	ids := make([]string, 0, len(selected))
	for txnID := range selected {
		ids = append(ids, txnID)
	}
	sort.Strings(ids)
	refs := make([]*txnClosureAdmission, 0, len(ids))
	for _, txnID := range ids {
		refs = append(refs, s.refTxnClosureAdmission([]byte(txnID)))
	}
	guard := txnClosureAdmissionGuard{
		service: s,
		refs:    refs,
	}
	for idx := range refs {
		if err := ctx.Err(); err != nil {
			guard.release()
			return txnClosureAdmissionGuard{}, err
		}
		select {
		case refs[idx].token <- struct{}{}:
			guard.acquired++
			continue
		default:
		}
		select {
		case refs[idx].token <- struct{}{}:
			guard.acquired++
		case <-ctx.Done():
			guard.release()
			return txnClosureAdmissionGuard{}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		guard.release()
		return txnClosureAdmissionGuard{}, err
	}
	return guard, nil
}
