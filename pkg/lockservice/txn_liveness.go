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

import "sync"

// externalTxnLiveness tracks non-transaction-client owners for the lifetime
// of their lock transactions.
type externalTxnLiveness struct {
	mu     sync.RWMutex
	txnIDs map[string]struct{}
}

var _ ExternalTxnLivenessRegistry = (*service)(nil)

func (l *externalTxnLiveness) register(txnID []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.txnIDs == nil {
		l.txnIDs = make(map[string]struct{})
	}
	l.txnIDs[string(txnID)] = struct{}{}
}

func (l *externalTxnLiveness) unregister(txnID []byte) {
	l.mu.Lock()
	delete(l.txnIDs, string(txnID))
	l.mu.Unlock()
}

func (l *externalTxnLiveness) contains(txnID []byte) bool {
	l.mu.RLock()
	_, ok := l.txnIDs[string(txnID)]
	l.mu.RUnlock()
	return ok
}

func (l *externalTxnLiveness) iter(fn func([]byte) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for txnID := range l.txnIDs {
		if !fn([]byte(txnID)) {
			return
		}
	}
}

func (l *externalTxnLiveness) clear() {
	l.mu.Lock()
	clear(l.txnIDs)
	l.mu.Unlock()
}
