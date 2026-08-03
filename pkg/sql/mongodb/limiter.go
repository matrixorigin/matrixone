// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mongodb

import (
	"context"
	"sync"
)

type sourceLimitKey struct {
	accountID    uint32
	connectionID uint64
}

// SourceLimiter bounds concurrent statements per tenant/source. Waiting is
// cancellation-aware, so a saturated source cannot pin a CN pipeline forever.
type SourceLimiter struct {
	mu      sync.Mutex
	limit   int
	sources map[sourceLimitKey]*sourceSemaphore
}

type sourceSemaphore struct {
	ch    chan struct{}
	users int
}

func NewSourceLimiter(limit int) *SourceLimiter {
	if limit <= 0 {
		limit = DefaultRuntimeConfig().MaxSourceConcurrency
	}
	return &SourceLimiter{limit: limit, sources: make(map[sourceLimitKey]*sourceSemaphore)}
}

func (l *SourceLimiter) Acquire(ctx context.Context, accountID uint32, connectionID uint64) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	key := sourceLimitKey{accountID: accountID, connectionID: connectionID}
	l.mu.Lock()
	semaphore := l.sources[key]
	if semaphore == nil {
		semaphore = &sourceSemaphore{ch: make(chan struct{}, l.limit)}
		l.sources[key] = semaphore
	}
	semaphore.users++
	l.mu.Unlock()
	select {
	case semaphore.ch <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { l.release(key, semaphore, true) }) }, nil
	case <-ctx.Done():
		l.release(key, semaphore, false)
		return nil, context.Cause(ctx)
	}
}

func (l *SourceLimiter) release(key sourceLimitKey, semaphore *sourceSemaphore, held bool) {
	if held {
		<-semaphore.ch
	}
	l.mu.Lock()
	semaphore.users--
	if semaphore.users == 0 && len(semaphore.ch) == 0 && l.sources[key] == semaphore {
		delete(l.sources, key)
	}
	l.mu.Unlock()
}
