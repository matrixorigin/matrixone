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

package lockservice

import (
	"encoding/hex"
	"strings"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

type ownerLocalTxnKey struct {
	txnID     string
	createdOn string
}

type ownerLocalWaitEdge struct {
	waiter  *waiter
	waitFor []ownerLocalTxnKey
}

func newOwnerLocalTxnKey(txn pb.WaitTxn) ownerLocalTxnKey {
	return ownerLocalTxnKey{
		txnID:     string(txn.TxnID),
		createdOn: txn.CreatedOn,
	}
}

func (k ownerLocalTxnKey) equal(other ownerLocalTxnKey) bool {
	return k.txnID == other.txnID && k.createdOn == other.createdOn
}

func (k ownerLocalTxnKey) debugString() string {
	if k.createdOn == "" {
		return hex.EncodeToString([]byte(k.txnID))
	}
	return hex.EncodeToString([]byte(k.txnID)) + "@" + k.createdOn
}

func (e ownerLocalWaitEdge) activeFor(txn ownerLocalTxnKey) bool {
	return e.waiter != nil &&
		e.waiter.getStatus() == blocking &&
		newOwnerLocalTxnKey(e.waiter.txn).equal(txn)
}

// detectOwnerLocalDeadlockLocked checks cycles fully visible on this owner CN.
// Caller must hold localLockTable.mu and the current txn mutex. The fast path
// is intentionally scoped to remote-owner row/exclusive locks; all other cases
// keep using the existing distributed detector.
func (l *localLockTable) detectOwnerLocalDeadlockLocked(
	c *lockContext,
	conflictWith Lock,
) bool {
	if !c.opts.async ||
		c.opts.Granularity != pb.Granularity_Row ||
		c.opts.Mode != pb.LockMode_Exclusive ||
		conflictWith.isShared() {
		return false
	}

	current := newOwnerLocalTxnKey(c.waitTxn)
	path, found := findOwnerLocalDeadlockPath(
		l.mu.ownerLocalWaits,
		current,
		ownerLocalHolderKeys(conflictWith))
	if !found {
		return false
	}

	c.txn.deadlockFound = true
	v2.TxnDeadlockOwnerLocalCounter.WithLabelValues("aborted").Inc()
	l.logger.Warn("owner_local_deadlock_found",
		zap.Uint64("table", l.bind.OriginTable),
		zap.String("bind", l.bind.DebugString()),
		zap.String("victim", hex.EncodeToString(c.txn.txnID)),
		zap.Int("path-length", len(path)),
		zap.String("path", formatOwnerLocalDeadlockPath(path)))
	return true
}

func ownerLocalHolderKeys(lock Lock) []ownerLocalTxnKey {
	holders := make([]ownerLocalTxnKey, 0, lock.holders.size())
	for _, holder := range lock.holders.txns {
		holders = append(holders, newOwnerLocalTxnKey(holder))
	}
	return holders
}

func (l *localLockTable) addOwnerLocalWaitEdgeLocked(
	c *lockContext,
	conflictWith Lock,
) {
	if c == nil ||
		c.w == nil ||
		!c.opts.async ||
		c.opts.Granularity != pb.Granularity_Row ||
		c.opts.Mode != pb.LockMode_Exclusive ||
		conflictWith.isShared() ||
		conflictWith.holders.size() == 0 {
		return
	}

	waitFor := ownerLocalHolderKeys(conflictWith)
	if len(waitFor) == 0 {
		return
	}
	waiter := newOwnerLocalTxnKey(c.w.txn)
	l.mu.ownerLocalWaits[waiter] = append(
		l.mu.ownerLocalWaits[waiter],
		ownerLocalWaitEdge{
			waiter:  c.w,
			waitFor: waitFor,
		})
}

func (l *localLockTable) removeOwnerLocalWaitEdgeLocked(w *waiter) {
	if w == nil {
		return
	}
	l.removeOwnerLocalWaitEdgeByTxnLocked(newOwnerLocalTxnKey(w.txn), w)
}

func (l *localLockTable) removeOwnerLocalWaitEdgeByTxnLocked(
	txn ownerLocalTxnKey,
	w *waiter,
) {
	edges := l.mu.ownerLocalWaits[txn]
	if len(edges) == 0 {
		return
	}
	n := 0
	for _, edge := range edges {
		if edge.waiter == w {
			continue
		}
		edges[n] = edge
		n++
	}
	if n == 0 {
		delete(l.mu.ownerLocalWaits, txn)
		return
	}
	clear(edges[n:])
	l.mu.ownerLocalWaits[txn] = edges[:n]
}

func (l *localLockTable) removeInactiveOwnerLocalWaitEdgesLocked(lock Lock) {
	lock.waiters.iter(func(w *waiter) bool {
		if w.getStatus() != blocking {
			l.removeOwnerLocalWaitEdgeLocked(w)
		}
		return true
	})
}

func findOwnerLocalDeadlockPath(
	graph map[ownerLocalTxnKey][]ownerLocalWaitEdge,
	start ownerLocalTxnKey,
	startWaitFor []ownerLocalTxnKey,
) ([]ownerLocalTxnKey, bool) {
	visited := make(map[ownerLocalTxnKey]struct{}, len(graph))
	path := []ownerLocalTxnKey{start}
	visited[start] = struct{}{}

	var dfs func(ownerLocalTxnKey, []ownerLocalTxnKey) bool
	dfs = func(txn ownerLocalTxnKey, explicit []ownerLocalTxnKey) bool {
		nexts := explicit
		for _, edge := range graph[txn] {
			if !edge.activeFor(txn) {
				continue
			}
			nexts = append(nexts, edge.waitFor...)
		}
		for _, next := range nexts {
			if next.equal(start) {
				path = append(path, next)
				return true
			}
			if _, ok := visited[next]; ok {
				continue
			}
			visited[next] = struct{}{}
			path = append(path, next)
			if dfs(next, nil) {
				return true
			}
			path = path[:len(path)-1]
		}
		return false
	}

	if dfs(start, startWaitFor) {
		return path, true
	}
	return nil, false
}

func formatOwnerLocalDeadlockPath(path []ownerLocalTxnKey) string {
	var b strings.Builder
	for i, txn := range path {
		if i > 0 {
			b.WriteString(" <= ")
		}
		b.WriteString(txn.debugString())
	}
	return b.String()
}
