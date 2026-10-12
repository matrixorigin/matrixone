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

package substrait

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestReadConsumerMarkersAreDisjoint(t *testing.T) {
	nonzero := bytes.Repeat([]byte{7}, sha256.Size)
	marker, err := consumerMarker(ReadConsumerFlight, nonzero)
	require.NoError(t, err)
	require.Equal(t, nonzero, marker)
	require.ErrorContains(t, mustConsumerMarkerError(ReadConsumerFlight, make([]byte, sha256.Size)), "nonzero")

	marker, err = consumerMarker(ReadConsumerEmbeddedTAE, nil)
	require.NoError(t, err)
	require.Equal(t, make([]byte, sha256.Size), marker)
	require.ErrorContains(t, mustConsumerMarkerError(ReadConsumerEmbeddedTAE, nonzero), "reserved zero")

	consumer, err := inferReadConsumer(marker)
	require.NoError(t, err)
	require.Equal(t, ReadConsumerEmbeddedTAE, consumer)
	consumer, err = inferReadConsumer(nonzero)
	require.NoError(t, err)
	require.Equal(t, ReadConsumerFlight, consumer)
}

func mustConsumerMarkerError(consumer ReadConsumer, marker []byte) error {
	_, err := consumerMarker(consumer, marker)
	return err
}

func TestFlightJournalSchemaAndAuthorityRemainConsumerNeutral(t *testing.T) {
	lease := testDurableLease(t, 31, uint64(time.Now().Add(time.Minute).UnixMilli()))
	require.Equal(t, ReadConsumerFlight, lease.Consumer)
	record := journalRecord{
		Wire: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema,
		AuthorizedClientSPKIHash: lease.AuthorizedClientSPKIHash, ObjectNames: lease.ObjectNames,
	}
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	type legacyJournalRecord struct {
		Wire                     []byte   `json:"wire"`
		Manifest                 []byte   `json:"manifest"`
		CanonicalSchema          []byte   `json:"canonical_schema"`
		AuthorizedClientSPKIHash []byte   `json:"authorized_client_spki_hash"`
		ObjectNames              []string `json:"object_names,omitempty"`
	}
	legacyEncoded, err := json.Marshal(legacyJournalRecord{
		Wire: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema,
		AuthorizedClientSPKIHash: lease.AuthorizedClientSPKIHash, ObjectNames: lease.ObjectNames,
	})
	require.NoError(t, err)
	require.Equal(t, legacyEncoded, encoded)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	require.NotContains(t, fields, "consumer")
	require.ElementsMatch(t,
		[]string{"wire", "manifest", "canonical_schema", "authorized_client_spki_hash"},
		mapKeys(fields),
	)

	legacy := sha256.New()
	_, _ = legacy.Write([]byte("matrixone/substrait/read-lease-authority/v1\x00"))
	_, _ = legacy.Write(lease.Wire)
	_, _ = legacy.Write(lease.AuthorizedClientSPKIHash)
	authority := leaseAuthorityDigest(lease)
	require.Equal(t, legacy.Sum(nil), authority[:])
}

func mapKeys(values map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func TestJournalReplayInfersConsumerWithoutSchemaChange(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("consumer-replay", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := newFileServiceLeaseJournal(fs, "sirius/read-leases", newTestJournalAdmission())
	require.NoError(t, err)

	flight := testDurableLease(t, 32, uint64(time.Now().Add(time.Minute).UnixMilli()))
	embedded := testDurableLease(t, 33, uint64(time.Now().Add(time.Minute).UnixMilli()))
	embedded.Consumer = ReadConsumerEmbeddedTAE
	embedded.AuthorizedClientSPKIHash = make([]byte, sha256.Size)
	stored, err := journal.StoreIfCapacity(ctx, []*Lease{flight, embedded}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, stored)

	loaded, err := collectJournalLeases(ctx, journal)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	byRef := make(map[string]ReadConsumer, len(loaded))
	for _, lease := range loaded {
		byRef[string(lease.Read.ReadRef)] = lease.Consumer
	}
	require.Equal(t, ReadConsumerFlight, byRef[string(flight.Read.ReadRef)])
	require.Equal(t, ReadConsumerEmbeddedTAE, byRef[string(embedded.Read.ReadRef)])
}

func TestResolverRejectsEmbeddedConsumerBeforeCertificateHandling(t *testing.T) {
	now := time.Now()
	lease := testDurableLease(t, 34, uint64(now.Add(time.Minute).UnixMilli()))
	lease.Consumer = ReadConsumerEmbeddedTAE
	lease.AuthorizedClientSPKIHash = make([]byte, sha256.Size)
	manager := NewLeaseManager(1, new(fakeProtector))
	require.NoError(t, manager.Acquire(context.Background(), []*Lease{lease}))
	unknown := cloneTaeRead(lease.Read)
	unknown.ReadRef = bytes.Repeat([]byte{99}, sha256.Size)
	unknownWire, err := MarshalTaeRead(unknown)
	require.NoError(t, err)
	unknownBody := appendBytes(nil, 1, unknownWire)
	unknownBody = appendBytes(unknownBody, 2, lease.CanonicalSchema)
	unknownRequest := httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(unknownBody))
	unknownRequest.Header.Set("Content-Type", "application/x-protobuf")
	unknownResponse := httptest.NewRecorder()
	ResolveHandler(manager, func() time.Time { return now }, new(fakeResolveAuditor)).ServeHTTP(unknownResponse, unknownRequest)
	require.Equal(t, http.StatusUnauthorized, unknownResponse.Code,
		"an unknown reference must not bypass Flight certificate validation")

	body := appendBytes(nil, 1, lease.Wire)
	body = appendBytes(body, 2, lease.CanonicalSchema)

	req := httptest.NewRequest(http.MethodPost, ResolvePath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	auditor := new(fakeResolveAuditor)
	response := httptest.NewRecorder()
	ResolveHandler(manager, func() time.Time { return now }, auditor).ServeHTTP(response, req)
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Empty(t, auditor.events)
}

func TestRestartReconciliationRetainsFailedEmbeddedCleanup(t *testing.T) {
	now := uint64(time.Now().Add(time.Minute).UnixMilli())
	flight := testDurableLease(t, 37, now)
	embedded := testDurableLease(t, 38, now)
	embedded.Consumer = ReadConsumerEmbeddedTAE
	embedded.AuthorizedClientSPKIHash = make([]byte, sha256.Size)
	journal := &fakeLeaseJournal{leases: []*Lease{cloneLease(flight), cloneLease(embedded)}}
	protector := &fakeProtector{failUnregister: true}
	manager := NewPersistentLeaseManager(2, protector, journal)
	require.NoError(t, manager.Replay(context.Background()))

	_, err := manager.ReconcileRestart(context.Background(), ReadConsumerFlight)
	require.ErrorContains(t, err, "release stale embedded TAE reads")
	require.Len(t, manager.leases, 2)
	require.Equal(t, releaseMarked, manager.releases[string(embedded.Read.ReadRef)])
	require.Len(t, manager.PendingExecutions(), 1)

	protector.failUnregister = false
	pending, err := manager.ReconcileRestart(context.Background(), ReadConsumerFlight)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Len(t, manager.leases, 1)
}

func TestRestartReconciliationCleansEmbeddedAndPreservesFlight(t *testing.T) {
	now := uint64(time.Now().Add(time.Minute).UnixMilli())
	flight := testDurableLease(t, 35, now)
	embedded := testDurableLease(t, 36, now)
	embedded.Consumer = ReadConsumerEmbeddedTAE
	embedded.AuthorizedClientSPKIHash = make([]byte, sha256.Size)
	journal := &fakeLeaseJournal{leases: []*Lease{cloneLease(flight), cloneLease(embedded)}}
	protector := new(fakeProtector)
	manager := NewPersistentLeaseManager(2, protector, journal)
	require.NoError(t, manager.Replay(context.Background()))

	pending, err := manager.ReconcileRestart(context.Background(), ReadConsumerEmbeddedTAE)
	require.ErrorContains(t, err, "unreconciled Flight")
	require.Nil(t, pending)
	require.Equal(t, 1, protector.unregistered)
	require.Len(t, manager.leases, 1)
	require.NotNil(t, manager.leases[string(flight.Read.ReadRef)])
	require.Len(t, manager.PendingExecutions(), 1)
	require.Equal(t, ReadConsumerFlight, manager.PendingExecutions()[0].Consumer)

	pending, err = manager.ReconcileRestart(context.Background(), ReadConsumerFlight)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, ReadConsumerFlight, pending[0].Consumer)
}

type metadataFactsProvider struct {
	facts map[int32]SnapshotFacts
	calls int
}

func (p *metadataFactsProvider) PrepareSnapshotRead(_ context.Context, read Read, _ []byte) (SnapshotFacts, error) {
	p.calls++
	return p.facts[read.NodeID], nil
}

type boundedMetadataFactsProvider struct {
	metadataFactsProvider
	limits       []int
	rejectedNode int32
	ignoreLimit  bool
}

func (p *boundedMetadataFactsProvider) PrepareSnapshotReadBounded(_ context.Context, read Read, _ []byte, maximum int) (SnapshotFacts, error) {
	p.limits = append(p.limits, maximum)
	facts := p.facts[read.NodeID]
	need := len(facts.Manifest)
	for _, name := range facts.ObjectNames {
		need += 16 + len(name)
	}
	if need > maximum && !p.ignoreLimit {
		p.rejectedNode = read.NodeID
		return SnapshotFacts{}, notEligiblef(EligibilitySnapshot, "bounded provider rejected table %d during construction", read.TableID)
	}
	return facts, nil
}

func TestAdmissionEnforcesAggregateTAEMetadataBoundary(t *testing.T) {
	reads := []Read{
		{NodeID: 1, DatabaseID: 7, TableID: 41, Columns: []ColumnMapping{{ColumnID: 1}}, Schema: []byte("schema-a")},
		{NodeID: 2, DatabaseID: 7, TableID: 42, Columns: []ColumnMapping{{ColumnID: 2}}, Schema: []byte("schema-b")},
	}
	facts := map[int32]SnapshotFacts{
		1: {Manifest: []byte("manifest-a"), CanonicalSchema: reads[0].Schema, ObjectNames: []string{"object-a"}},
		2: {Manifest: []byte("manifest-b"), CanonicalSchema: reads[1].Schema, ObjectNames: []string{"object-b"}},
	}
	first, err := prechargeQueryTAEMetadata(0, reads[0], 1<<20)
	require.NoError(t, err)
	first, err = chargeReturnedTAEMetadata(first, facts[1], 1<<20)
	require.NoError(t, err)
	secondBase, err := prechargeQueryTAEMetadata(first, reads[1], 1<<20)
	require.NoError(t, err)
	exact, err := chargeReturnedTAEMetadata(secondBase, facts[2], 1<<20)
	require.NoError(t, err)
	request := func(provider SnapshotProvider, consumer ReadConsumer) AdmissionRequest {
		spki := []byte(nil)
		if consumer == ReadConsumerFlight {
			spki = bytes.Repeat([]byte{7}, sha256.Size)
		}
		return AdmissionRequest{
			Candidate: &Candidate{reads: reads}, Provider: provider,
			Leases: NewLeaseManager(2, new(fakeProtector)), AccountID: 1, QueryID: []byte("query"),
			SnapshotTS: make([]byte, 12), Consumer: consumer, AuthorizedClientSPKIHash: spki, TTL: time.Minute,
			ReadOnly: true, Random: bytes.NewReader(append(bytes.Repeat([]byte{9}, 32), bytes.Repeat([]byte{8}, 32)...)), Now: time.Now(),
		}
	}

	provider := &boundedMetadataFactsProvider{metadataFactsProvider: metadataFactsProvider{facts: facts}}
	accepted := request(provider, ReadConsumerEmbeddedTAE)
	admitted, err := admitReadsWithMetadataLimit(context.Background(), accepted, exact)
	require.NoError(t, err)
	require.Len(t, admitted.Wires, 2)
	require.Len(t, admitted.EmbeddedTAEReads, 2)
	for nodeID, metadata := range admitted.EmbeddedTAEReads {
		require.Equal(t, facts[nodeID].Manifest, metadata.Manifest)
		require.Equal(t, facts[nodeID].CanonicalSchema, metadata.CanonicalSchema)
		managed := accepted.Leases.leases[string(metadata.ReadRef)]
		require.NotNil(t, managed)
		require.True(t, &managed.Manifest[0] == &metadata.Manifest[0], "embedded metadata must borrow the protected lease allocation")
		require.True(t, &managed.CanonicalSchema[0] == &metadata.CanonicalSchema[0], "embedded schema must borrow the protected lease allocation")
	}
	require.Equal(t, []int{exact - (256 + 128*len(reads[0].Columns) + len(reads[0].Schema)), exact - secondBase}, provider.limits)

	provider = &boundedMetadataFactsProvider{metadataFactsProvider: metadataFactsProvider{facts: facts}}
	rejected := request(provider, ReadConsumerEmbeddedTAE)
	protector := rejected.Leases.protector.(*fakeProtector)
	_, err = admitReadsWithMetadataLimit(context.Background(), rejected, exact-1)
	require.ErrorContains(t, err, "during construction")
	require.True(t, IsNotEligible(err))
	require.Equal(t, int32(2), provider.rejectedNode)
	require.Len(t, provider.limits, 2)
	require.Less(t, provider.limits[1], provider.limits[0])
	require.Zero(t, protector.registered)

	provider = &boundedMetadataFactsProvider{
		metadataFactsProvider: metadataFactsProvider{facts: facts}, ignoreLimit: true,
	}
	rejected = request(provider, ReadConsumerEmbeddedTAE)
	protector = rejected.Leases.protector.(*fakeProtector)
	_, err = admitReadsWithMetadataLimit(context.Background(), rejected, exact-1)
	require.ErrorContains(t, err, "query snapshot metadata exceeds")
	require.True(t, IsNotEligible(err))
	require.Zero(t, protector.registered, "defensive recomputation must reject before publication")

	unbounded := &metadataFactsProvider{facts: facts}
	rejected = request(unbounded, ReadConsumerEmbeddedTAE)
	protector = rejected.Leases.protector.(*fakeProtector)
	_, err = admitReadsWithMetadataLimit(context.Background(), rejected, exact)
	require.ErrorContains(t, err, "requires a bounded snapshot provider")
	require.Zero(t, unbounded.calls)
	require.Zero(t, protector.begun)

	flight := request(unbounded, ReadConsumerFlight)
	admitted, err = admitReadsWithMetadataLimit(context.Background(), flight, 1)
	require.NoError(t, err)
	require.Len(t, admitted.Wires, 2)
	require.Nil(t, admitted.EmbeddedTAEReads)
	require.Equal(t, 2, unbounded.calls)
}
