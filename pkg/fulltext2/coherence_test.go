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

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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

func TestDropCacheIdentityClearsFenceState(t *testing.T) {
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

func TestFenceRegistryReclaimsClaimedFenceOnlyIntoTransientIdentity(t *testing.T) {
	r := newFenceRegistry(1)
	a := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "a", MetadataTable: "ma"}
	b := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "b", MetadataTable: "mb"}
	g := Generation{BaseTimestamp: 1, TailChunk: 1}

	claim, _, overflow := r.install(a, g)
	require.True(t, claim)
	require.False(t, overflow)
	claim, _, overflow = r.install(b, g)
	require.False(t, claim)
	require.True(t, overflow)
	require.Equal(t, g, r.required(a))
	require.Zero(t, r.required(b))

	require.True(t, r.finishClaim(a, g))
	claim, _, overflow = r.install(b, g)
	require.True(t, claim)
	require.False(t, overflow)
	require.Zero(t, r.required(a))
	require.Equal(t, g, r.required(b))
	require.True(t, r.retiredIdentity(a))
}

func TestInstallGenerationFenceOverflowRecoversWithoutGlobalPublication(t *testing.T) {
	oldRegistry := localFences
	oldCache := veccache.Cache
	oldEpoch := coherenceEpoch.Load()
	localFences = newFenceRegistry(1)
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() {
		localFences = oldRegistry
		veccache.Cache = oldCache
		coherenceEpoch.Store(oldEpoch)
	})

	a := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "a", MetadataTable: "ma"}
	b := CacheIdentity{AccountID: 1, Database: "db", StorageTable: "b", MetadataTable: "mb"}
	g := Generation{BaseTimestamp: 1, TailChunk: 1}
	claim, _, overflow := localFences.install(a, g)
	require.True(t, claim)
	require.False(t, overflow)

	before := coherenceEpoch.Load()
	_, claimed, overflow := InstallGenerationFence(b, g)
	require.False(t, claimed)
	require.True(t, overflow)
	require.Equal(t, before+1, coherenceEpoch.Load())

	require.True(t, localFences.finishClaim(a, g))
	current, claimed, overflow := InstallGenerationFence(b, g)
	require.Equal(t, g, current)
	require.True(t, claimed)
	require.False(t, overflow)
	require.Zero(t, localFences.required(a))
	require.True(t, requiresTransientLoad(a))

	search := NewFulltext2SearchForAccount(a.TableConfig(), a.AccountID)
	transient, err := search.UseTransientLoad(nil)
	require.NoError(t, err)
	require.True(t, transient)
}

func TestWarmSearchRejectsOlderCoherenceEpoch(t *testing.T) {
	oldCache := veccache.Cache
	oldEpoch := coherenceEpoch.Load()
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() {
		veccache.Cache = oldCache
		coherenceEpoch.Store(oldEpoch)
	})

	s := &Fulltext2Search{
		identity:    CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"},
		identitySet: true,
		loadedEpoch: oldEpoch,
	}
	coherenceEpoch.Add(1)
	_, _, _, _, _, err := s.prepare(nil, nil, vectorindex.RuntimeConfig{})
	require.ErrorIs(t, err, errLoadGenerationSuperseded)
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
