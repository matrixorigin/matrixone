//go:build fulltext2_base_file_reuse

package fulltext2

import (
	"errors"
	"sync"
	"sync/atomic"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

const experimentalBaseFileReuseEnabled = true

const (
	experimentalBaseFileReuseMaxBytes = int64(4 << 30)
	experimentalBaseFileReuseMaxFiles = 64
)

type experimentalOwnerEntry struct {
	closeMu            sync.Mutex
	owner              *baseFileOwner
	token              *BaseFileReuseOwnerToken
	generation         uint64
	closed             bool
	shutdownInProgress bool
}

// BaseFileReuseOwnerToken is an opaque service-generation capability.  CN
// keeps the token returned at Start and presents that same token at Close;
// an old service object therefore cannot close a newer owner that reused the
// same UUID.
type BaseFileReuseOwnerToken struct {
	service    string
	generation uint64
	owner      *baseFileOwner
}

var experimentalOwnerState struct {
	sync.Mutex
	owners             map[string]*experimentalOwnerEntry
	globalClosed       bool
	shutdownInProgress bool
}

var experimentalOwnerHookOnce sync.Once
var experimentalOwnerHookInstallCount uint32

var (
	errExperimentalOwnerServiceMismatch = errors.New("fulltext2 experimental owner service mismatch")
	errExperimentalOwnerShutdown        = errors.New("fulltext2 experimental owner is shutting down")
)

type experimentalOwnerSelection struct {
	owner  *baseFileOwner
	closed bool
}

func init() {
	// The lifecycle hook must exist before the first VectorIndexCache can take a
	// shutdown snapshot. Lazy registration from the first SQL query has a race:
	// shutdown may copy the old hook list, then the query can publish an owner
	// that will never receive the shutdown callback.
	installExperimentalOwnerHook()
}

// installExperimentalOwnerHook installs one stable dispatcher for the process.
// RegisterLifecycleHook has no unregister operation, so registering a closure for
// every owner generation would retain every old owner forever. The dispatcher
// instead looks up the current service owner when shutdown is delivered.
func installExperimentalOwnerHook() {
	experimentalOwnerHookOnce.Do(func() {
		atomic.AddUint32(&experimentalOwnerHookInstallCount, 1)
		veccache.RegisterPreShutdownHook(experimentalOwnerBeforeShutdown)
		veccache.RegisterLifecycleHook(experimentalOwnerLifecycleHook)
	})
}

// experimentalOwnerBeforeShutdown publishes the registry barrier before
// invoking cancellations, outside the registry lock. No entry close mutex,
// cache drain, pool retry, or OS cleanup may run in this phase.
func experimentalOwnerBeforeShutdown() {
	owners := markExperimentalOwnersClosing()
	for _, entry := range owners {
		entry.owner.cancelOperations()
	}
}

func markExperimentalOwnersClosing() map[string]*experimentalOwnerEntry {
	experimentalOwnerState.Lock()
	experimentalOwnerState.globalClosed = true
	experimentalOwnerState.shutdownInProgress = true
	owners := make(map[string]*experimentalOwnerEntry, len(experimentalOwnerState.owners))
	for service, entry := range experimentalOwnerState.owners {
		if entry == nil {
			continue
		}
		entry.closed = true
		entry.shutdownInProgress = true
		owners[service] = entry
	}
	experimentalOwnerState.Unlock()
	return owners
}

func experimentalOwnerLifecycleHook(shutdown bool) {
	if !shutdown {
		return
	}
	// Direct service/test callers retain the complete shutdown contract. In
	// Cache.Destroy the prephase has already sealed the registry, so replacement
	// generations cannot be admitted between this snapshot and finalization.
	experimentalOwnerBeforeShutdown()
	experimentalOwnerState.Lock()
	owners := make(map[string]*experimentalOwnerEntry, len(experimentalOwnerState.owners))
	for service, entry := range experimentalOwnerState.owners {
		if entry != nil {
			owners[service] = entry
		}
	}
	experimentalOwnerState.Unlock()
	for service, entry := range owners {
		var err error
		if entry.owner != nil {
			err = closeExperimentalOwnerEntry(service, entry)
		}
		experimentalOwnerState.Lock()
		if current := experimentalOwnerState.owners[service]; current == entry {
			current.closed = true
			current.shutdownInProgress = false
			if err == nil {
				delete(experimentalOwnerState.owners, service)
			}
		}
		experimentalOwnerState.Unlock()
	}
	experimentalOwnerState.Lock()
	experimentalOwnerState.shutdownInProgress = false
	experimentalOwnerState.Unlock()
}

// initializeExperimentalOwnerService is the explicit lifecycle boundary for a
// new experiment service. The first service may be initialized while the
// process is open; after shutdown, a query cannot reopen the experiment merely
// by passing a different service string.
func initializeExperimentalOwnerService(service string) error {
	if service == "" {
		return errExperimentalOwnerServiceMismatch
	}
	experimentalOwnerState.Lock()
	defer experimentalOwnerState.Unlock()
	if experimentalOwnerState.shutdownInProgress {
		return errExperimentalOwnerShutdown
	}
	if experimentalOwnerState.owners == nil {
		experimentalOwnerState.owners = make(map[string]*experimentalOwnerEntry)
	}
	entry := experimentalOwnerState.owners[service]
	if entry != nil && entry.shutdownInProgress {
		return errExperimentalOwnerShutdown
	}
	if entry != nil && entry.closed {
		if entry.owner != nil && entry.owner.hasResources() {
			return errBaseFileOwnerPending
		}
		// Never mutate a retired entry into a new generation: an already queued
		// close/dispatcher still holds that entry pointer.
		entry = nil
	}
	if entry == nil {
		entry = &experimentalOwnerEntry{}
		experimentalOwnerState.owners[service] = entry
	}
	if entry.owner == nil {
		entry.owner = newBaseFileOwner(experimentalBaseFileReuseMaxBytes, experimentalBaseFileReuseMaxFiles)
		entry.generation++
		entry.token = &BaseFileReuseOwnerToken{service: service, generation: entry.generation, owner: entry.owner}
	}
	if entry.token == nil {
		entry.generation++
		entry.token = &BaseFileReuseOwnerToken{service: service, generation: entry.generation, owner: entry.owner}
	}
	entry.shutdownInProgress = false
	// An explicit service-start boundary may open a new generation after the
	// process-wide vector-cache dispatcher observed shutdown. Pending owners
	// remain reachable until their final resources have been released.
	experimentalOwnerState.globalClosed = false
	return nil
}

// InitializeBaseFileReuseOwner is the explicit service-start boundary used by
// the tagged CN integration.  Keeping this boundary separate from the SQL
// factory prevents a query from resurrecting a closed service owner.
func InitializeBaseFileReuseOwner(service string) (*BaseFileReuseOwnerToken, error) {
	if err := initializeExperimentalOwnerService(service); err != nil {
		return nil, err
	}
	experimentalOwnerState.Lock()
	defer experimentalOwnerState.Unlock()
	entry := experimentalOwnerState.owners[service]
	if entry == nil || entry.token == nil {
		return nil, errExperimentalOwnerServiceMismatch
	}
	return entry.token, nil
}

// CloseBaseFileReuseOwner closes the owner named by one service-generation
// token. It is deliberately idempotent and may return errBaseFileOwnerPending
// while a pinned mapping or deferred munmap still has a live owner. The owner
// remains reachable so a later service-close retry can finish the cleanup.
func CloseBaseFileReuseOwner(token *BaseFileReuseOwnerToken) error {
	if token == nil || token.service == "" {
		return nil
	}
	experimentalOwnerState.Lock()
	entry := experimentalOwnerState.owners[token.service]
	// A stale service object is allowed to close idempotently, but it must never
	// operate on the current generation that reused the same UUID.
	if entry == nil || entry.token != token || entry.generation != token.generation {
		experimentalOwnerState.Unlock()
		return nil
	}
	entry.closed = true
	entry.shutdownInProgress = true
	experimentalOwnerState.Unlock()

	err := closeExperimentalOwnerEntry(token.service, entry)
	experimentalOwnerState.Lock()
	if current := experimentalOwnerState.owners[token.service]; current == entry && current.token == token {
		current.closed = true
		if err == nil {
			delete(experimentalOwnerState.owners, token.service)
		}
	}
	entry.shutdownInProgress = false
	experimentalOwnerState.Unlock()
	return err
}

func closeExperimentalOwnerEntry(service string, entry *experimentalOwnerEntry) error {
	if entry == nil || entry.owner == nil {
		return nil
	}
	entry.closeMu.Lock()
	defer entry.closeMu.Unlock()
	experimentalOwnerState.Lock()
	current := experimentalOwnerState.owners[service] == entry
	experimentalOwnerState.Unlock()
	if !current {
		// A late close that was queued for a removed generation must not drain
		// the cache of the new service instance that reused the same UUID.
		return nil
	}
	entry.owner.beginClose()
	// Drain the actual VectorIndexCache entries before closing the owner.  The
	// cache owns the Search wrappers; the owner only owns files and must not
	// wait forever for an idle wrapper that remains resident there.
	veccache.Cache.DestroyByService(service)
	return entry.owner.close()
}

func experimentalOwnerForSQL(service string) experimentalOwnerSelection {
	if service == "" {
		return experimentalOwnerSelection{}
	}
	experimentalOwnerState.Lock()
	entry := experimentalOwnerState.owners[service]
	if entry != nil && entry.closed {
		selection := experimentalOwnerSelection{
			owner:  entry.owner,
			closed: true,
		}
		experimentalOwnerState.Unlock()
		return selection
	}
	// Only CN Start creates an owner. In particular an empty registry after
	// successful teardown must not let a late SQL factory resurrect a service.
	// This allows fully cleaned entries to be removed instead of retaining an
	// unbounded tombstone for every historical service UUID.
	if entry == nil {
		experimentalOwnerState.Unlock()
		return experimentalOwnerSelection{closed: true}
	}
	owner := entry.owner
	if owner != nil {
		experimentalOwnerState.Unlock()
		if _, err := owner.poolForSearch(); err == nil {
			return experimentalOwnerSelection{owner: owner}
		}
		// A closed owner is deliberately returned to the factory. The caller
		// retains it as a closed Search handle, so Load cannot fall back to the
		// ordinary path after the service has shut down.
		experimentalOwnerState.Lock()
		if current := experimentalOwnerState.owners[service]; current == entry {
			current.closed = true
		}
		experimentalOwnerState.Unlock()
		return experimentalOwnerSelection{owner: owner, closed: true}
	}
	// This branch is defensive: a well-formed entry always carries an owner.
	experimentalOwnerState.Unlock()
	return experimentalOwnerSelection{closed: true}
}

// newExperimentalFulltext2Search is the only build-tagged activation point for
// the local Base-file reuse prototype. It is intentionally absent from normal
// SQL construction; callers must opt into the tag and supply an explicit
// budget.
func newExperimentalFulltext2Search(cfg TableConfig, maxBytes int64, maxFiles int) *Fulltext2Search {
	return newFulltext2SearchWithBaseOwner(cfg, newBaseFileOwner(maxBytes, maxFiles))
}

// NewFulltext2SearchForExecution is the sole SQL-construction switch for the
// local experiment. The regular build returns the ordinary search object; the
// tagged build borrows the CN-process owner above.
func NewFulltext2SearchForExecution(cfg TableConfig, service string) *Fulltext2Search {
	selection := experimentalOwnerForSQL(service)
	if selection.closed && selection.owner == nil {
		return &Fulltext2Search{cfg: cfg, baseOwnerClosed: true}
	}
	return newFulltext2SearchWithBaseOwnerForService(cfg, selection.owner, service)
}
