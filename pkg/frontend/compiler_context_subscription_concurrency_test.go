// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	txnclient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// overlapWaitBound bounds how long the first resolution arriving in the
// cached-executor use region waits for a peer resolution before releasing.
// It is a hang guard: sibling scopes of one statement are scheduled within
// microseconds, while a serialized second branch must be released after the
// bound even though the peer can no longer overlap it.
const overlapWaitBound = 2 * time.Second

// peerReleaseBarrier makes region overlap deterministic instead of relying
// on scheduler luck: the first arrival parks until a second resolution
// enters the use region, or until the bounded wait expires.
type peerReleaseBarrier struct {
	mu      sync.Mutex
	waiting int
	release chan struct{}
}

func (b *peerReleaseBarrier) arriveAndWait(bound time.Duration) {
	b.mu.Lock()
	b.waiting++
	if b.waiting >= 2 {
		if b.release != nil {
			close(b.release)
			b.release = nil
		}
		b.waiting--
		b.mu.Unlock()
		return
	}
	if b.release == nil {
		b.release = make(chan struct{})
	}
	release := b.release
	b.mu.Unlock()
	select {
	case <-release:
	case <-time.After(bound):
	}
	b.mu.Lock()
	b.waiting--
	b.mu.Unlock()
}

type nonSubscriptionDatabase struct {
	engine.Database
}

func (nonSubscriptionDatabase) IsSubscription(context.Context) bool { return false }

// resolutionGateEngine instruments the subscription-resolution step inside
// the cached-executor use region: Database is the first unguarded catalog
// resolution getSubscriptionMeta performs, after the caller has taken the
// session-cached back executor and cleared its result sets.
type resolutionGateEngine struct {
	engine.Engine
	db nonSubscriptionDatabase

	barrier   *peerReleaseBarrier
	namesMu   sync.Mutex
	names     []string
	inflight  atomic.Int32
	maxInside atomic.Int32
}

func (e *resolutionGateEngine) Database(
	ctx context.Context,
	databaseName string,
	op txnclient.TxnOperator,
) (engine.Database, error) {
	e.namesMu.Lock()
	e.names = append(e.names, databaseName)
	e.namesMu.Unlock()

	inside := e.inflight.Add(1)
	for {
		observed := e.maxInside.Load()
		if inside <= observed || e.maxInside.CompareAndSwap(observed, inside) {
			break
		}
	}
	e.barrier.arriveAndWait(overlapWaitBound)
	e.inflight.Add(-1)
	return e.db, nil
}

// TestDirectEstimatorSubscriptionMetaResolutionsSerializeTheCachedBackExec
// mirrors the runtime shape of the direct statistics estimators
// (mo_table_rows/mo_table_size/mo_table_col_ndv/mo_table_col_nullcnt
// subscription branches in pkg/sql/plan/function/func_mo.go): volatile calls
// stay runtime expressions, so sibling scopes of one statement - for example
// the arms of SELECT mo_table_col_ndv('sub_a','t','v') UNION SELECT
// mo_table_col_ndv('sub_b','t','v') - share one session and may resolve
// their aliases concurrently. The session-cached subscription executor's
// internal result sets are cleared and repopulated per resolution, so
// overlapping resolutions would let one branch clear or read another
// branch's catalog results. Resolutions must therefore be serialized.
func TestDirectEstimatorSubscriptionMetaResolutionsSerializeTheCachedBackExec(t *testing.T) {
	const service = "frontend-submeta-overlap-ut"

	gate := &resolutionGateEngine{
		db:      nonSubscriptionDatabase{},
		barrier: &peerReleaseBarrier{},
	}
	InitServerLevelVars(service)
	SetPUForExternalUT(service, &config.ParameterUnit{StorageEngine: gate})

	tcc := InitTxnCompilerContext("subdb")
	ses := &Session{}
	ses.service = service
	ses.txnHandler = &TxnHandler{}
	ses.txnCompileCtx = tcc
	tcc.execCtx = &ExecCtx{reqCtx: context.Background(), ses: ses}

	// Seed the session-cached executor exactly as the first resolution leaves
	// it, so both branches take the cached path through getOrCreateBackExec.
	// A real backExec keeps getOrCreateBackExec's type assertion and
	// transaction comparison on their production paths.
	backSes := &backSession{}
	backSes.txnHandler = &TxnHandler{}
	tcc.cachedBackExec = &backExec{backSes: backSes}

	helper := &SqlHelper{ses: ses}

	errs := make(chan error, 2)
	resolve := func(dbName string) {
		meta, err := helper.GetSubscriptionMeta(dbName)
		switch {
		case err != nil:
			errs <- fmt.Errorf("resolving %q: %w", dbName, err)
		case meta != nil:
			errs <- fmt.Errorf("resolving %q: unexpected subscription meta %+v", dbName, meta)
		default:
			errs <- nil
		}
	}
	go resolve("sub_a")
	go resolve("sub_b")

	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(4 * overlapWaitBound):
			t.Fatalf("subscription resolutions did not complete within %s", 4*overlapWaitBound)
		}
	}

	if got := gate.maxInside.Load(); got != 1 {
		t.Fatalf(
			"overlapping subscription resolutions observed on the session-cached back executor: peak concurrent resolutions = %d, want 1",
			got,
		)
	}

	got := append([]string(nil), gate.names...)
	sort.Strings(got)
	if want := []string{"sub_a", "sub_b"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("resolved databases = %v, want %v", got, want)
	}
}
