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
	"context"
	"slices"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

const idleRemoteRouteEvictionBatchSize = 64

// Snapshots are immutable. A cold publisher may confirm a missing owner using
// newer discovery data, but that confirmation cannot outlive this snapshot.
type remoteRouteOwnerSnapshot struct {
	owners map[string]struct{}
}

// keepIdleRemoteRoutes runs only in the existing remote keeper. A membership
// change is a reason to discard idle routing hints, never proof that an active
// owner's physical locks were released. TN remains the authority for GetBind.
func (k *lockTableKeeper) keepIdleRemoteRoutes(ctx context.Context) {
	reader, ok := k.client.(interface {
		cachedLockServiceOwners(context.Context, []string) ([]string, error)
	})
	if !ok || ctx.Err() != nil {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	owners, err := reader.cachedLockServiceOwners(readCtx, k.routeOwnerScratch)
	cancel()
	if err != nil || ctx.Err() != nil {
		return
	}
	if k.service.idleRouteScanRequested.Swap(false) {
		k.idleRouteScanPending = true
	}
	slices.Sort(owners)
	if !k.routeOwnersKnown || !slices.Equal(owners, k.routeOwners) {
		removed := !k.routeOwnersKnown
		for _, previous := range k.routeOwners {
			if _, present := slices.BinarySearch(owners, previous); !present {
				removed = true
				break
			}
		}
		current := make(map[string]struct{}, len(owners))
		for _, owner := range owners {
			current[owner] = struct{}{}
		}
		k.service.bindChangeMu.Lock()
		k.service.remoteRouteOwners = &remoteRouteOwnerSnapshot{owners: current}
		k.service.bindChangeMu.Unlock()
		k.routeOwners, k.routeOwnerScratch = owners, k.routeOwners[:0]
		k.routeOwnersKnown = true
		k.idleRouteScanPending = k.idleRouteScanPending || removed
	} else {
		k.routeOwnerScratch = owners
	}

	if k.idleRouteScanPending {
		clear(k.idleRemoteRoutes)
		k.idleRemoteRoutes = k.idleRemoteRoutes[:0]
		k.idleRemoteRouteOffset = 0
		// No service/transaction mutex is held during the cache scan. The
		// per-bind eviction below rechecks both membership and live references.
		k.groupTables.iter(func(_ uint64, table lockTable) bool {
			if ctx.Err() != nil {
				return false
			}
			bind := table.getBind()
			if bind.ServiceID != k.serviceID {
				if _, present := slices.BinarySearch(k.routeOwners, getUUIDFromServiceIdentifier(bind.ServiceID)); !present {
					k.idleRemoteRoutes = append(k.idleRemoteRoutes, bind)
				}
			}
			return true
		})
		if ctx.Err() != nil {
			return // retry the complete scan on the next keeper round
		}
		k.idleRouteScanPending = false
	}

	// Active routes stay in this finite worklist until cleanup releases them
	// or the existing remote keeper replaces/removes them. Rotate fairly, and
	// bound exclusive-lock acquisitions and closes in every keeper round.
	budget := min(idleRemoteRouteEvictionBatchSize, len(k.idleRemoteRoutes))
	for ; budget > 0 && ctx.Err() == nil; budget-- {
		index := k.idleRemoteRouteOffset % len(k.idleRemoteRoutes)
		if k.service.evictIdleRemoteRoute(k.idleRemoteRoutes[index]) {
			last := len(k.idleRemoteRoutes) - 1
			k.idleRemoteRoutes[index] = k.idleRemoteRoutes[last]
			k.idleRemoteRoutes[last] = pb.LockTable{}
			k.idleRemoteRoutes = k.idleRemoteRoutes[:last]
			k.idleRemoteRouteOffset = index
		} else {
			k.idleRemoteRouteOffset = index + 1
		}
	}
	if len(k.idleRemoteRoutes) == 0 {
		k.idleRemoteRoutes = nil
		k.idleRemoteRouteOffset = 0
	}
}

// prepareRemoteRoutePublication confirms an owner missing from the keeper's
// last snapshot. Discovery may already contain a newly joined owner; on an
// address miss, preserve the RPC client's existing on-demand refresh. No
// service mutex is held across discovery or refresh. Cached Lock/Unlock never
// call this helper.
func (s *service) prepareRemoteRoutePublication(
	ctx context.Context,
	bind pb.LockTable,
) (*remoteRouteOwnerSnapshot, error) {
	s.bindChangeMu.RLock()
	snapshot := s.remoteRouteOwners
	allowed := s.canPublishRemoteRouteLocked(bind, nil)
	s.bindChangeMu.RUnlock()
	if allowed {
		return nil, nil
	}
	reader, ok := s.remote.client.(interface {
		confirmLockServiceOwner(context.Context, string) (bool, error)
	})
	if !ok {
		return nil, ErrLockTableBindChanged
	}
	present, err := reader.confirmLockServiceOwner(ctx, bind.ServiceID)
	if err != nil {
		return nil, lockWaitContextError(ctx, err)
	}
	if !present {
		return nil, ErrLockTableBindChanged
	}
	return snapshot, nil
}

// canPublishRemoteRouteLocked rejects delayed replies for departed owners.
// A confirmation obtained outside the lock is valid only while the captured
// snapshot is still current. Rejection precedes transaction admission. The
// caller holds bindChangeMu; this check performs no discovery or RPC.
func (s *service) canPublishRemoteRouteLocked(bind pb.LockTable, confirmed *remoteRouteOwnerSnapshot) bool {
	if bind.ServiceID == s.serviceID || s.remoteRouteOwners == nil {
		return true
	}
	_, present := s.remoteRouteOwners.owners[getUUIDFromServiceIdentifier(bind.ServiceID)]
	return present || confirmed == s.remoteRouteOwners
}

// Request a scan after publication when the keeper has not yet observed this
// owner. It may join and leave between keeper rounds, producing no snapshot
// difference; the newly cached route must still get an absence check. Call
// after the cache write, before releasing bindChangeMu.
func (s *service) remoteRoutePublished(confirmed *remoteRouteOwnerSnapshot) {
	if confirmed != nil {
		s.idleRouteScanRequested.Store(true)
	}
}

// evictIdleRemoteRoute returns false only while this exact cached generation
// still has a transaction reference. Admission, publication and eviction share
// bindChangeMu; the reference check and exact detachment are one transition.
func (s *service) evictIdleRemoteRoute(bind pb.LockTable) bool {
	s.bindChangeMu.Lock()
	if s.canPublishRemoteRouteLocked(bind, nil) {
		s.bindChangeMu.Unlock()
		return true
	}
	current := s.tableGroups.get(bind.Group, bind.Table)
	if current == nil || makeRemoteBindKey(current.getBind()) != makeRemoteBindKey(bind) {
		s.bindChangeMu.Unlock()
		return true
	}
	s.mu.RLock()
	if ref := s.mu.remoteBindRefs[makeRemoteBindKey(bind)]; ref.refs > 0 {
		s.mu.RUnlock()
		s.bindChangeMu.Unlock()
		return false
	}
	removed := s.tableGroups.detachExactBind(bind)
	s.mu.RUnlock()
	s.bindChangeMu.Unlock()
	if removed != nil {
		removed.close(closeReasonBindChanged)
	}
	return true
}
