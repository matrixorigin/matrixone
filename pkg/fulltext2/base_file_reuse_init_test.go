//go:build fulltext2_base_file_reuse

package fulltext2

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// withExperimentalOwnerState isolates tests from the service-scoped owners
// used by the tagged SQL factory. The real lifecycle dispatcher remains
// installed; only the test's synthetic owner state is restored after the test.
func withExperimentalOwnerState(t *testing.T, owner *baseFileOwner, service string, closed bool) {
	t.Helper()
	experimentalOwnerState.Lock()
	previousOwners := experimentalOwnerState.owners
	previousGlobalClosed := experimentalOwnerState.globalClosed
	previousShutdownInProgress := experimentalOwnerState.shutdownInProgress
	owners := make(map[string]*experimentalOwnerEntry)
	if service != "" {
		owners[service] = &experimentalOwnerEntry{
			owner:  owner,
			closed: closed,
		}
	}
	experimentalOwnerState.owners = owners
	experimentalOwnerState.globalClosed = false
	experimentalOwnerState.shutdownInProgress = false
	if owner == nil && closed {
		experimentalOwnerState.globalClosed = true
	}
	experimentalOwnerState.Unlock()
	t.Cleanup(func() {
		experimentalOwnerState.Lock()
		experimentalOwnerState.owners = previousOwners
		experimentalOwnerState.globalClosed = previousGlobalClosed
		experimentalOwnerState.shutdownInProgress = previousShutdownInProgress
		experimentalOwnerState.Unlock()
	})
}

// fillingOwnerSearch is a real VectorIndexSearch wrapper whose Load owns a
// base-file-pool fill. It lets the shutdown test exercise the same lock order
// as the production cache drain without requiring a SQL cluster.
type fillingOwnerSearch struct {
	owner   *baseFileOwner
	service string
	started chan struct{}
	once    sync.Once
}

func (s *fillingOwnerSearch) CacheServiceID() string { return s.service }

func (s *fillingOwnerSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, nil
}

func (s *fillingOwnerSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}

func (s *fillingOwnerSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

func (s *fillingOwnerSearch) Preload(*sqlexec.SqlProcess) error { return nil }

func (s *fillingOwnerSearch) Load(*sqlexec.SqlProcess) error {
	pool, err := s.owner.poolForSearch()
	if err != nil {
		return err
	}
	_, err = s.owner.run(context.Background(), func(context.Context) (*Segment, error) {
		lease, acquireErr := pool.acquire(context.Background(), testPoolKey("close-order", 8, "abcdefgh"), func(ctx context.Context) (*baseFileHandle, error) {
			s.once.Do(func() { close(s.started) })
			<-ctx.Done()
			return nil, ctx.Err()
		})
		if lease != nil {
			lease.Release()
		}
		return nil, acquireErr
	})
	return err
}

func (s *fillingOwnerSearch) GetIndexSize() (int64, int64) { return 0, 0 }
func (s *fillingOwnerSearch) BuildTS() int64               { return 0 }
func (s *fillingOwnerSearch) Destroy()                     {}

func TestExperimentalOwnerCloseCancelsInFlightFillingSearch(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	const service = "cn-filling-close"
	token, err := InitializeBaseFileReuseOwner(service)
	require.NoError(t, err)
	owner := experimentalOwnerForSQL(service).owner

	algo := &fillingOwnerSearch{owner: owner, service: service, started: make(chan struct{})}
	entry := &veccache.VectorIndexSearch{Algo: algo}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	const key = "filling-close-entry"
	veccache.Cache.IndexMap.Store(key, entry)
	t.Cleanup(func() {
		veccache.Cache.IndexMap.Delete(key)
		_ = owner.close()
	})

	loadDone := make(chan error, 1)
	go func() { loadDone <- entry.Load(nil) }()
	select {
	case <-algo.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the test Search did not enter a pool fill")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- CloseBaseFileReuseOwner(token) }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		// Keep the failed test from leaving the fill and cache-drain goroutines
		// behind; the production path must have issued this cancellation first.
		owner.pool.Close()
		t.Fatal("owner shutdown waited for a filling Search without canceling the fill")
	}
	require.ErrorIs(t, <-loadDone, errBaseFilePoolClosed)
	_, present := veccache.Cache.IndexMap.Load(key)
	require.False(t, present)
}

func TestExperimentalSQLFactoryRejectsClosedOwner(t *testing.T) {
	owner := newBaseFileOwner(1024, 2)
	withExperimentalOwnerState(t, owner, "cn-closed", false)
	require.NoError(t, owner.close())

	search := NewFulltext2SearchForExecution(TableConfig{IndexTable: "idx"}, "cn-closed")
	require.Same(t, owner, search.baseOwner, "a closed owner must remain attached to the SQL handle")
	require.Nil(t, search.basePool)
	require.ErrorIs(t, search.Preload(nil), errBaseFileOwnerClosed,
		"preload must also stop at the closed owner boundary")
	require.ErrorIs(t, search.Load(nil), errBaseFileOwnerClosed,
		"the factory must not turn a closed service owner into an ordinary load")
	search.Destroy()
}

func TestExperimentalOwnerShutdownDispatcherClosesCurrentOwner(t *testing.T) {
	owner := newBaseFileOwner(1024, 2)
	withExperimentalOwnerState(t, owner, "cn-shutdown", false)

	experimentalOwnerLifecycleHook(true)
	_, err := owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
	search := NewFulltext2SearchForExecution(TableConfig{IndexTable: "idx"}, "cn-shutdown")
	require.Nil(t, search.baseOwner, "fully cleaned owners must leave the registry")
	require.ErrorIs(t, search.Load(nil), errBaseFileOwnerClosed)
	search.Destroy()
}

func TestExperimentalOwnerAllowsOnlyAnExplicitNewServiceAfterClose(t *testing.T) {
	oldOwner := newBaseFileOwner(1024, 2)
	withExperimentalOwnerState(t, oldOwner, "cn-old", true)
	require.NoError(t, oldOwner.close())

	closedSelection := experimentalOwnerForSQL("cn-new")
	require.True(t, closedSelection.closed)
	require.NoError(t, initializeExperimentalOwnerService("cn-new"))
	newOwner := experimentalOwnerForSQL("cn-new").owner
	require.NotNil(t, newOwner)
	require.NotSame(t, oldOwner, newOwner,
		"a new service identity is the explicit lifecycle boundary for a new owner")
	require.NoError(t, newOwner.close())
}

func TestExperimentalOwnerServiceLifecycleEntryPoints(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	token, err := InitializeBaseFileReuseOwner("cn-entrypoint")
	require.NoError(t, err)
	selection := experimentalOwnerForSQL("cn-entrypoint")
	require.NotNil(t, selection.owner)

	require.NoError(t, CloseBaseFileReuseOwner(token))
	closed := experimentalOwnerForSQL("cn-entrypoint")
	require.True(t, closed.closed)
	require.Nil(t, closed.owner)
	_, err = selection.owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)

	// A later service generation must use the explicit initialization boundary;
	// changing the query's service string is not sufficient to reopen the old
	// owner.
	other := experimentalOwnerForSQL("cn-entrypoint-new")
	require.True(t, other.closed)
	newToken, err := InitializeBaseFileReuseOwner("cn-entrypoint-new")
	require.NoError(t, err)
	newSelection := experimentalOwnerForSQL("cn-entrypoint-new")
	require.NotNil(t, newSelection.owner)
	require.NotSame(t, selection.owner, newSelection.owner)
	require.NoError(t, CloseBaseFileReuseOwner(newToken))
}

func TestExperimentalOwnerIsolatedPerService(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	aToken, err := InitializeBaseFileReuseOwner("cn-a")
	require.NoError(t, err)
	bToken, err := InitializeBaseFileReuseOwner("cn-b")
	require.NoError(t, err)
	a := experimentalOwnerForSQL("cn-a")
	b := experimentalOwnerForSQL("cn-b")
	require.NotNil(t, a.owner)
	require.NotNil(t, b.owner)
	require.NotSame(t, a.owner, b.owner,
		"service generations must not share a mutable owner or pool")
	require.NoError(t, CloseBaseFileReuseOwner(aToken))
	require.NotNil(t, experimentalOwnerForSQL("cn-b").owner,
		"closing one service must not close another service owner")
	require.NoError(t, CloseBaseFileReuseOwner(bToken))
}

func TestExperimentalOwnerStaleTokenCannotCloseNewGeneration(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	oldToken, err := InitializeBaseFileReuseOwner("cn-reused")
	require.NoError(t, err)
	oldOwner := experimentalOwnerForSQL("cn-reused").owner
	require.NoError(t, CloseBaseFileReuseOwner(oldToken))

	newToken, err := InitializeBaseFileReuseOwner("cn-reused")
	require.NoError(t, err)
	newOwner := experimentalOwnerForSQL("cn-reused").owner
	require.NotSame(t, oldOwner, newOwner)

	// A second Close from the old CN object is a stale no-op.  It must not
	// reach the newly initialized owner that reused the same service UUID.
	require.NoError(t, CloseBaseFileReuseOwner(oldToken))
	_, err = newOwner.poolForSearch()
	require.NoError(t, err)
	require.NoError(t, CloseBaseFileReuseOwner(newToken))
}

func TestExperimentalOwnerStaleCloseEntryCannotDrainNewGeneration(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	const service = "cn-delayed-close"
	oldToken, err := InitializeBaseFileReuseOwner(service)
	require.NoError(t, err)
	experimentalOwnerState.Lock()
	oldEntry := experimentalOwnerState.owners[service]
	experimentalOwnerState.Unlock()
	require.NoError(t, CloseBaseFileReuseOwner(oldToken))
	newToken, err := InitializeBaseFileReuseOwner(service)
	require.NoError(t, err)
	search := newFulltext2SearchWithBaseOwnerForService(TableConfig{IndexTable: "delayed-close"}, newToken.owner, service)
	entry := &veccache.VectorIndexSearch{Algo: search}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	entry.Status.Store(veccache.STATUS_LOADED)
	const key = "delayed-close-entry"
	veccache.Cache.IndexMap.Store(key, entry)
	t.Cleanup(func() { _ = CloseBaseFileReuseOwner(newToken); veccache.Cache.IndexMap.Delete(key) })
	// Model a close/dispatcher that already captured the old entry before
	// another closer reclaimed it. UUID equality alone is insufficient here.
	require.NoError(t, closeExperimentalOwnerEntry(service, oldEntry))
	resident, ok := veccache.Cache.IndexMap.Load(key)
	require.True(t, ok)
	require.Same(t, entry, resident)
	_, err = newToken.owner.poolForSearch()
	require.NoError(t, err)
	require.NoError(t, CloseBaseFileReuseOwner(newToken))
}

func TestExperimentalOwnerCloseDrainsServiceCacheSearch(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	const service = "cn-cache-drain"
	token, err := InitializeBaseFileReuseOwner(service)
	require.NoError(t, err)
	owner := experimentalOwnerForSQL(service).owner

	source := loadedSearch(t)
	search := newFulltext2SearchWithBaseOwnerForService(TableConfig{IndexTable: "cache-drain"}, owner, service)
	search.idx = source.idx
	search.loaded = true
	source.idx = nil
	source.Destroy()

	entry := &veccache.VectorIndexSearch{Algo: search}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	entry.Status.Store(veccache.STATUS_LOADED)
	const key = "cache-drain-entry"
	veccache.Cache.IndexMap.Store(key, entry)
	t.Cleanup(func() { veccache.Cache.IndexMap.Delete(key) })

	require.NoError(t, CloseBaseFileReuseOwner(token))
	_, present := veccache.Cache.IndexMap.Load(key)
	require.False(t, present, "service close must evict the resident cache wrapper before closing its owner")
	require.Nil(t, search.idx, "the resident Search must release its Segment before owner.Close")
	require.True(t, search.baseOwnerClosed, "a retry racing service close must fail closed")
	require.ErrorIs(t, search.Preload(nil), errBaseFileOwnerClosed,
		"a cache retry of the same Search must fail closed after owner shutdown")
}

func TestExperimentalOwnerRegistersOneStableLifecycleDispatcher(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	before := atomic.LoadUint32(&experimentalOwnerHookInstallCount)

	require.NoError(t, initializeExperimentalOwnerService("cn-hook"))
	first := experimentalOwnerForSQL("cn-hook")
	second := experimentalOwnerForSQL("cn-hook")
	require.Same(t, first.owner, second.owner)
	hookToken, err := InitializeBaseFileReuseOwner("cn-hook")
	require.NoError(t, err)
	require.NoError(t, CloseBaseFileReuseOwner(hookToken))
	thirdSelection := experimentalOwnerForSQL("cn-hook-new-service")
	require.True(t, thirdSelection.closed)
	require.NoError(t, initializeExperimentalOwnerService("cn-hook-new-service"))
	third := experimentalOwnerForSQL("cn-hook-new-service").owner
	require.NotSame(t, first.owner, third)
	require.NoError(t, third.close())

	after := atomic.LoadUint32(&experimentalOwnerHookInstallCount)
	require.LessOrEqual(t, after-before, uint32(1),
		"owner generations must not register one permanent hook each")
}

func TestExperimentalFactoryShutdownRaceCannotLeaveOpenOwner(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	require.NoError(t, initializeExperimentalOwnerService("cn-race"))
	start := make(chan struct{})
	selectionCh := make(chan experimentalOwnerSelection, 1)
	factoryDone := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(factoryDone)
		<-start
		selectionCh <- experimentalOwnerForSQL("cn-race")
	}()
	go func() {
		defer close(shutdownDone)
		<-start
		experimentalOwnerLifecycleHook(true)
	}()
	close(start)
	selection := <-selectionCh
	<-factoryDone
	<-shutdownDone

	experimentalOwnerState.Lock()
	entry := experimentalOwnerState.owners["cn-race"]
	var owner *baseFileOwner
	var ownerEntryClosed bool
	if entry != nil {
		owner = entry.owner
		ownerEntryClosed = entry.closed
		// Only a pending owner may still occupy the registry after shutdown.
	}
	experimentalOwnerState.Unlock()
	if owner != nil {
		require.True(t, ownerEntryClosed)
		_, err := owner.poolForSearch()
		require.ErrorIs(t, err, errBaseFileOwnerClosed)
	}
	if selection.owner != nil {
		_, err := selection.owner.poolForSearch()
		require.ErrorIs(t, err, errBaseFileOwnerClosed)
	} else {
		require.True(t, selection.closed)
	}
}

func TestExperimentalOwnerRejectsNewServiceDuringShutdown(t *testing.T) {
	owner := newBaseFileOwner(1024, 2)
	withExperimentalOwnerState(t, owner, "cn-existing", false)

	operationEntered := make(chan struct{})
	releaseOperation := make(chan struct{})
	operationDone := make(chan struct{})
	shutdownDone := make(chan struct{})
	var releaseOnce sync.Once
	var workers sync.WaitGroup
	t.Cleanup(func() {
		// Rescue does not depend on owner cancellation, the behavior under test.
		releaseOnce.Do(func() { close(releaseOperation) })
		workers.Wait()
	})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(operationDone)
		_, _ = owner.run(context.Background(), func(context.Context) (*Segment, error) {
			close(operationEntered)
			<-releaseOperation
			return nil, nil
		})
	}()
	select {
	case <-operationEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("owner operation did not start")
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(shutdownDone)
		experimentalOwnerLifecycleHook(true)
	}()

	// The admitted operation keeps owner.close blocked after the dispatcher
	// publishes the shutdown state. Spin only on the protected state; no sleep
	// or timing guess is used to establish the interleaving.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		experimentalOwnerState.Lock()
		inProgress := experimentalOwnerState.shutdownInProgress
		experimentalOwnerState.Unlock()
		if inProgress {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("dispatcher did not publish its shutdown admission barrier")
		default:
		}
		runtime.Gosched()
	}
	require.ErrorIs(t, initializeExperimentalOwnerService("cn-new"), errExperimentalOwnerShutdown)

	releaseOnce.Do(func() { close(releaseOperation) })
	for _, done := range []<-chan struct{}{operationDone, shutdownDone} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown workers did not finish after rescue")
		}
	}
	selection := experimentalOwnerForSQL("cn-new")
	require.True(t, selection.closed)
}

func TestExperimentalFactoryCompletesBeforeShutdown(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	require.NoError(t, initializeExperimentalOwnerService("cn-factory-first"))
	selection := experimentalOwnerForSQL("cn-factory-first")
	require.NotNil(t, selection.owner)

	experimentalOwnerLifecycleHook(true)
	experimentalOwnerState.Lock()
	entry := experimentalOwnerState.owners["cn-factory-first"]
	experimentalOwnerState.Unlock()
	require.Nil(t, entry, "successful dispatcher cleanup must release its registry reference")
	_, err := selection.owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
}

func TestExperimentalOwnerRegistryReclaimsClosedServices(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	for i := 0; i < 100; i++ {
		service := fmt.Sprintf("cn-history-%d", i)
		token, err := InitializeBaseFileReuseOwner(service)
		require.NoError(t, err)
		require.NoError(t, CloseBaseFileReuseOwner(token))
		experimentalOwnerState.Lock()
		count := len(experimentalOwnerState.owners)
		experimentalOwnerState.Unlock()
		require.Zero(t, count, "closed UUIDs must not accumulate owner/token references")
		search := NewFulltext2SearchForExecution(TableConfig{IndexTable: "idx"}, service)
		search.Destroy()
		require.ErrorIs(t, search.Preload(nil), errBaseFileOwnerClosed)
		require.ErrorIs(t, search.Load(nil), errBaseFileOwnerClosed)
	}
	// Even an empty, initially open registry cannot bootstrap from a query.
	require.True(t, experimentalOwnerForSQL("cn-never-started").closed)
}

func TestExperimentalOwnerRegistryRetainsPendingResources(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	const service = "cn-pending"
	token, err := InitializeBaseFileReuseOwner(service)
	require.NoError(t, err)
	pool, err := token.owner.poolForSearch()
	require.NoError(t, err)
	lease, err := pool.acquire(context.Background(), testPoolKey("pinned", 8, "abcdefgh"), testPoolFill(t, new(atomic.Int32), "abcdefgh"))
	require.NoError(t, err)
	t.Cleanup(func() { lease.Release(); _ = CloseBaseFileReuseOwner(token) })
	require.ErrorIs(t, CloseBaseFileReuseOwner(token), errBaseFileOwnerPending)
	experimentalOwnerState.Lock()
	entry := experimentalOwnerState.owners[service]
	experimentalOwnerState.Unlock()
	require.NotNil(t, entry)
	require.Same(t, token.owner, entry.owner)
	require.ErrorIs(t, initializeExperimentalOwnerService(service), errBaseFileOwnerPending)
	require.True(t, experimentalOwnerForSQL(service).closed)
	lease.Release()
	require.NoError(t, CloseBaseFileReuseOwner(token))
	experimentalOwnerState.Lock()
	entry = experimentalOwnerState.owners[service]
	experimentalOwnerState.Unlock()
	require.Nil(t, entry)
}

func TestExperimentalDispatcherRetainsPendingAndReclaimsCleanOwners(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	pending, err := InitializeBaseFileReuseOwner("cn-dispatch-pending")
	require.NoError(t, err)
	clean, err := InitializeBaseFileReuseOwner("cn-dispatch-clean")
	require.NoError(t, err)
	pool, err := pending.owner.poolForSearch()
	require.NoError(t, err)
	lease, err := pool.acquire(context.Background(), testPoolKey("dispatch-pinned", 8, "abcdefgh"), testPoolFill(t, new(atomic.Int32), "abcdefgh"))
	require.NoError(t, err)
	t.Cleanup(func() { lease.Release(); _ = CloseBaseFileReuseOwner(pending) })
	experimentalOwnerLifecycleHook(true)
	experimentalOwnerState.Lock()
	pendingEntry := experimentalOwnerState.owners["cn-dispatch-pending"]
	cleanEntry := experimentalOwnerState.owners["cn-dispatch-clean"]
	experimentalOwnerState.Unlock()
	require.NotNil(t, pendingEntry)
	require.Nil(t, cleanEntry)
	_, err = clean.owner.poolForSearch()
	require.ErrorIs(t, err, errBaseFileOwnerClosed)
	lease.Release()
	experimentalOwnerLifecycleHook(true)
	experimentalOwnerState.Lock()
	count := len(experimentalOwnerState.owners)
	experimentalOwnerState.Unlock()
	require.Zero(t, count)
}

func TestExperimentalFactoryRejectsFirstUseAfterShutdown(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", false)
	experimentalOwnerLifecycleHook(true)

	selection := experimentalOwnerForSQL("cn-late")
	require.True(t, selection.closed)
	require.Nil(t, selection.owner)
	search := NewFulltext2SearchForExecution(TableConfig{IndexTable: "idx"}, "cn-late")
	require.True(t, search.baseOwnerClosed)
	require.ErrorIs(t, search.Load(nil), errBaseFileOwnerClosed)
	search.Destroy()

	// Reopening is an explicit service-start action, not a query-side service
	// string change.
	require.NoError(t, initializeExperimentalOwnerService("cn-late"))
	owner := experimentalOwnerForSQL("cn-late").owner
	require.NotNil(t, owner)
	require.NoError(t, owner.close())
}

func TestExperimentalClosedOwnerlessSearchStaysClosedAfterDestroy(t *testing.T) {
	withExperimentalOwnerState(t, nil, "", true)
	search := NewFulltext2SearchForExecution(TableConfig{IndexTable: "idx"}, "cn-ownerless-closed")
	require.True(t, search.baseOwnerClosed)
	require.Nil(t, search.baseOwner)

	search.Destroy()
	search.Destroy()
	require.True(t, search.baseOwnerClosed,
		"Destroy must not erase the closed marker when no owner pointer was available")
	require.ErrorIs(t, search.Preload(nil), errBaseFileOwnerClosed)
	require.ErrorIs(t, search.Load(nil), errBaseFileOwnerClosed)
}
