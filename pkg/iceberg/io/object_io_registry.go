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

package icebergio

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const defaultObjectIORefTTL = 30 * time.Minute

type objectIORegistryEntry struct {
	provider         ObjectIOProvider
	scopeForLocation ObjectScopeForLocation
	expiresAt        time.Time
	refCount         int
}

var objectIORegistry = struct {
	sync.Mutex
	entries map[string]objectIORegistryEntry
}{
	entries: make(map[string]objectIORegistryEntry),
}

func RegisterObjectIOProvider(
	ctx context.Context,
	provider ObjectIOProvider,
	scopeForLocation ObjectScopeForLocation,
	ttl time.Duration,
) (string, error) {
	if provider == nil {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg object IO registry requires provider", nil))
	}
	if ttl == 0 {
		ttl = defaultObjectIORefTTL
	}
	ref, err := newObjectIORef(ctx)
	if err != nil {
		return "", err
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	objectIORegistry.Lock()
	sweepExpiredObjectIORefsLocked(time.Now())
	objectIORegistry.entries[ref] = objectIORegistryEntry{
		provider:         provider,
		scopeForLocation: scopeForLocation,
		expiresAt:        expiresAt,
	}
	objectIORegistry.Unlock()
	return ref, nil
}

func ResolveObjectIORef(
	ctx context.Context,
	ref string,
	location string,
) (fileservice.ETLFileService, string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg object IO ref is required", nil))
	}
	objectIORegistry.Lock()
	sweepExpiredObjectIORefsLocked(time.Now())
	entry, ok := objectIORegistry.entries[ref]
	objectIORegistry.Unlock()
	if !ok {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object IO ref is not registered or expired", map[string]string{
			"object_io_ref": api.PathHash(ref),
			"location":      RedactObjectPath(location),
		}))
	}
	scope := ObjectScope{StorageLocation: strings.TrimSpace(location)}
	if entry.scopeForLocation != nil {
		scope = entry.scopeForLocation(location)
		if strings.TrimSpace(scope.StorageLocation) == "" {
			scope.StorageLocation = strings.TrimSpace(location)
		}
	}
	return entry.provider.Resolve(ctx, scope)
}

func RetainObjectIORef(ref string) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	objectIORegistry.Lock()
	defer objectIORegistry.Unlock()
	sweepExpiredObjectIORefsLocked(time.Now())
	entry, ok := objectIORegistry.entries[ref]
	if !ok {
		return false
	}
	entry.refCount++
	objectIORegistry.entries[ref] = entry
	return true
}

func ReleaseObjectIORef(ref string) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return
	}
	objectIORegistry.Lock()
	if entry, ok := objectIORegistry.entries[ref]; ok && entry.refCount > 1 {
		entry.refCount--
		objectIORegistry.entries[ref] = entry
	} else {
		delete(objectIORegistry.entries, ref)
	}
	objectIORegistry.Unlock()
}

func SweepExpiredObjectIORefs(now time.Time) int {
	objectIORegistry.Lock()
	defer objectIORegistry.Unlock()
	return sweepExpiredObjectIORefsLocked(now)
}

func sweepExpiredObjectIORefsLocked(now time.Time) int {
	if now.IsZero() {
		now = time.Now()
	}
	removed := 0
	for ref, entry := range objectIORegistry.entries {
		if entry.refCount == 0 && !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
			delete(objectIORegistry.entries, ref)
			removed++
		}
	}
	return removed
}

func newObjectIORef(ctx context.Context) (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg object IO ref generation failed", nil, err))
	}
	return "iceberg-object-io://" + hex.EncodeToString(data[:]), nil
}
