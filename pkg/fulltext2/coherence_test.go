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

package fulltext2

import (
	"encoding/json"
	"testing"
	"time"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/stretchr/testify/require"
)

func TestCacheIdentityIsTenantAndDelimiterSafe(t *testing.T) {
	a := CacheIdentity{AccountID: 7, Database: "a.b", StorageTable: "c", MetadataTable: "m"}
	b := CacheIdentity{AccountID: 7, Database: "a", StorageTable: "b.c", MetadataTable: "m"}
	c := CacheIdentity{AccountID: 8, Database: "a.b", StorageTable: "c", MetadataTable: "m"}
	d := CacheIdentity{AccountID: 7, Database: "a.b", StorageTable: "c", MetadataTable: "m2"}

	require.NotEqual(t, a.Key(), b.Key())
	require.NotEqual(t, a.Key(), c.Key())
	require.NotEqual(t, a.Key(), d.Key())
	require.Equal(t, a, a.TableConfig().CacheIdentity(a.AccountID))
	require.Contains(t, a.Key(), "fulltext2:")
}

func TestTableConfigRejectsTenantFromTVFJSON(t *testing.T) {
	var cfg TableConfig
	require.NoError(t, json.Unmarshal([]byte(`{"AccountID":999,"db":"db","index":"s","metadata":"m"}`), &cfg))
	require.Zero(t, cfg.AccountID)
}

func TestGenerationLexicographicOrderCoversTailReset(t *testing.T) {
	require.True(t, (Generation{}).IsZero())
	require.False(t, (Generation{TailChunk: 1}).IsZero())
	require.True(t, (Generation{BaseTimestamp: 9, TailChunk: -1}).AtLeast(
		Generation{BaseTimestamp: 8, TailChunk: 1000}))
	require.True(t, (Generation{BaseTimestamp: 9, TailChunk: 3}).AtLeast(
		Generation{BaseTimestamp: 9, TailChunk: 2}))
	require.False(t, (Generation{BaseTimestamp: 8, TailChunk: 1000}).AtLeast(
		Generation{BaseTimestamp: 9, TailChunk: -1}))
}

func TestFenceRegistryUsesDefaultCapacity(t *testing.T) {
	r := newFenceRegistry(0)
	require.Equal(t, defaultFenceRegistryInitialCapacity, r.initialCapacity)
}

func TestFenceRegistryClaimsExistingUnclaimedFence(t *testing.T) {
	r := newFenceRegistry(8)
	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	generation := Generation{BaseTimestamp: 1, TailChunk: 1}

	r.entries[id.Key()] = &fenceEntry{required: generation, hasRequired: true}
	claim, current, overflow := r.install(id, generation)
	require.True(t, claim)
	require.Equal(t, generation, current)
	require.False(t, overflow)
	require.True(t, r.finishClaim(id, generation))
}

func TestDropCacheIdentityRemovesLocalSafetyState(t *testing.T) {
	oldRegistry := localFences
	oldCache := veccache.Cache
	localFences = newFenceRegistry(8)
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() {
		localFences = oldRegistry
		veccache.Cache = oldCache
	})

	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	generation := Generation{BaseTimestamp: 1, TailChunk: 1}
	claim, _, overflow := localFences.install(id, generation)
	require.True(t, claim)
	require.False(t, overflow)
	require.True(t, localFences.finishClaim(id, generation))

	DropCacheIdentity(id)
	require.Zero(t, localFences.required(id))
	require.False(t, localFences.claimedAtLeast(id, generation))
}

func TestFenceRegistryPrunesOnlyInactiveIdentities(t *testing.T) {
	r := newFenceRegistry(1)
	active := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "active", MetadataTable: "ma"}
	inactive := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "inactive", MetadataTable: "mi"}
	claiming := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "claiming", MetadataTable: "mc"}
	recovering := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "recovering", MetadataTable: "mr"}
	generation := Generation{BaseTimestamp: 9, TailChunk: 9}
	for _, id := range []CacheIdentity{active, inactive} {
		claim, _, overflow := r.install(id, generation)
		require.True(t, claim)
		require.False(t, overflow)
		require.True(t, r.finishClaim(id, generation))
	}
	claim, _, overflow := r.install(claiming, generation)
	require.True(t, claim)
	require.False(t, overflow)
	r.markUncertain(recovering)
	_, recover := r.beginRecovery(recovering, time.Now())
	require.True(t, recover)

	r.pruneInactive(func(key string) bool { return key == active.Key() })
	require.Equal(t, generation, r.required(active))
	require.Zero(t, r.required(inactive))
	require.Equal(t, generation, r.required(claiming), "an in-flight claim must not be pruned")
	require.True(t, r.uncertain(recovering), "an in-flight recovery token must not be recycled")
}

func TestFenceRegistryMonotonicClaimAndOutOfOrder(t *testing.T) {
	r := newFenceRegistry(8)
	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	g1 := Generation{BaseTimestamp: 3, TailChunk: 5}
	g2 := Generation{BaseTimestamp: 3, TailChunk: 7}

	claim, current, overflow := r.install(id, g1)
	require.True(t, claim)
	require.False(t, overflow)
	require.Equal(t, g1, current)
	require.False(t, r.finishClaim(id, g2))
	require.True(t, r.finishClaim(id, g1))

	claim, current, overflow = r.install(id, g1)
	require.False(t, claim)
	require.False(t, overflow)
	require.Equal(t, g1, current)

	claim, current, overflow = r.install(id, g2)
	require.True(t, claim)
	require.False(t, overflow)
	require.Equal(t, g2, current)

	// An older duplicate cannot finish the newer pending generation.
	require.False(t, r.finishClaim(id, g1))
	require.True(t, r.finishClaim(id, g2))
	require.True(t, r.required(id).AtLeast(g2))
}

func TestFenceRegistryRetainsClaimsBeyondInitialCapacity(t *testing.T) {
	r := newFenceRegistry(1)
	a := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "a", MetadataTable: "ma"}
	b := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "b", MetadataTable: "mb"}
	g := Generation{BaseTimestamp: 1, TailChunk: 1}

	claim, _, overflow := r.install(a, g)
	require.True(t, claim)
	require.False(t, overflow)
	claim, _, overflow = r.install(b, g)
	require.True(t, claim)
	require.False(t, overflow)
	require.Equal(t, g, r.required(a))
	require.Equal(t, g, r.required(b))
	require.False(t, r.uncertain(a))
	require.True(t, r.finishClaim(a, g))
	require.True(t, r.finishClaim(b, g))
}

func TestFenceRegistryCapPlusOneRotationConverges(t *testing.T) {
	r := newFenceRegistry(2)
	ids := []CacheIdentity{
		{AccountID: 1, Database: "db", StorageTable: "a", MetadataTable: "ma"},
		{AccountID: 1, Database: "db", StorageTable: "b", MetadataTable: "mb"},
		{AccountID: 1, Database: "db", StorageTable: "c", MetadataTable: "mc"},
	}
	generation := Generation{BaseTimestamp: 1, TailChunk: 1}

	for round := 0; round < 3; round++ {
		for _, id := range ids {
			claim, current, overflow := r.install(id, generation)
			require.False(t, overflow)
			require.Equal(t, generation, current)
			if claim {
				require.True(t, r.finishClaim(id, generation))
			}
		}
		require.Len(t, r.entries, len(ids))
		r.pruneInactive(func(string) bool { return false })
		require.Empty(t, r.entries, "inactive rotation must recover exact registry memory")
	}
}

func TestInstallGenerationFenceGrowsPastInitialCapacity(t *testing.T) {
	oldRegistry := localFences
	oldCache := veccache.Cache
	localFences = newFenceRegistry(1)
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() {
		localFences = oldRegistry
		veccache.Cache = oldCache
	})

	a := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "a", MetadataTable: "ma"}
	b := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "b", MetadataTable: "mb"}
	g := Generation{BaseTimestamp: 1, TailChunk: 1}
	claim, _, overflow := localFences.install(a, g)
	require.True(t, claim)
	require.False(t, overflow)

	current, claimed, overflow := InstallGenerationFence(b, g)
	require.Equal(t, g, current)
	require.True(t, claimed)
	require.False(t, overflow)
	require.Equal(t, g, localFences.required(a))
	require.False(t, requiresTransientLoad(a))
	require.True(t, localFences.finishClaim(a, g))

	search := NewFulltext2SearchForAccount(a.TableConfig(), a.AccountID)
	transient, err := search.UseTransientLoad(nil)
	require.NoError(t, err)
	require.False(t, transient)
}

func TestFreshnessUncertaintyIsTransientUntilConfirmed(t *testing.T) {
	oldRegistry := localFences
	localFences = newFenceRegistry(1)
	t.Cleanup(func() { localFences = oldRegistry })

	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	s := NewFulltext2SearchForAccount(id.TableConfig(), id.AccountID)
	s.OnFreshnessUncertain()
	transient, err := s.UseTransientLoad(nil)
	require.NoError(t, err)
	require.True(t, transient)

	s.OnFreshnessConfirmed()
	transient, err = NewFulltext2SearchForAccount(id.TableConfig(), id.AccountID).UseTransientLoad(nil)
	require.NoError(t, err)
	require.False(t, transient)
}

func TestFenceRegistryUncertaintySurvivesPushAndBacksOffRecovery(t *testing.T) {
	r := newFenceRegistry(1)
	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	generation := Generation{BaseTimestamp: 2, TailChunk: 3}
	now := time.Unix(100, 0)

	r.markUncertain(id)
	claim, _, overflow := r.install(id, generation)
	require.True(t, claim)
	require.False(t, overflow)
	require.True(t, r.finishClaim(id, generation))
	require.True(t, r.uncertain(id), "push delivery is not a current-generation read")

	version, ok := r.beginRecovery(id, now)
	require.True(t, ok)
	_, ok = r.beginRecovery(id, now)
	require.False(t, ok, "recovery must be single-flight")
	r.finishRecovery(id, version, false, now)
	require.True(t, r.uncertain(id))
	_, ok = r.beginRecovery(id, now.Add(freshnessRecoveryInitialBackoff-time.Nanosecond))
	require.False(t, ok)
	version, ok = r.beginRecovery(id, now.Add(freshnessRecoveryInitialBackoff))
	require.True(t, ok)
	r.finishRecovery(id, version, true, now.Add(freshnessRecoveryInitialBackoff))
	require.False(t, r.uncertain(id))
}

func TestFenceRegistryRecoveryCannotClearNewerUncertainty(t *testing.T) {
	r := newFenceRegistry(1)
	id := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	now := time.Unix(200, 0)
	r.markUncertain(id)
	version, ok := r.beginRecovery(id, now)
	require.True(t, ok)

	r.markUncertain(id)
	r.finishRecovery(id, version, true, now)
	require.True(t, r.uncertain(id), "a later failed sweep must dominate an older successful probe")
}

func TestFenceRegistryDropRecreateIsolation(t *testing.T) {
	r := newFenceRegistry(8)
	oldID := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "hidden-old", MetadataTable: "meta-old"}
	newID := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "hidden-new", MetadataTable: "meta-new"}
	g := Generation{BaseTimestamp: 4, TailChunk: 9}

	claim, _, _ := r.install(oldID, g)
	require.True(t, claim)
	require.True(t, r.finishClaim(oldID, g))
	r.drop(oldID)
	require.Zero(t, r.required(oldID))
	require.Zero(t, r.required(newID))
}

func TestInstallGenerationFenceNoCacheDuplicateAndOldAreIdempotent(t *testing.T) {
	oldRegistry := localFences
	oldCache := veccache.Cache
	localFences = newFenceRegistry(8)
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() {
		localFences = oldRegistry
		veccache.Cache = oldCache
	})

	id := CacheIdentity{AccountID: 9, Database: "db", StorageTable: "s", MetadataTable: "m"}
	g1 := Generation{BaseTimestamp: 2, TailChunk: 3}
	g2 := Generation{BaseTimestamp: 2, TailChunk: 4}

	current, claimed, overflow := InstallGenerationFence(id, g1)
	require.Equal(t, g1, current)
	require.True(t, claimed) // no cache is still a completed eviction claim
	require.False(t, overflow)

	_, claimed, _ = InstallGenerationFence(id, g1)
	require.True(t, claimed)
	_, claimed, _ = InstallGenerationFence(id, Generation{BaseTimestamp: 1, TailChunk: 99})
	require.True(t, claimed)
	current, claimed, _ = InstallGenerationFence(id, g2)
	require.Equal(t, g2, current)
	require.True(t, claimed)
}
