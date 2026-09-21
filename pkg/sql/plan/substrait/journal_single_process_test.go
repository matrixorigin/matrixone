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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type boundedJournalReadFS struct {
	fileservice.FileService
	requested int64
}

func (fs *boundedJournalReadFS) Read(_ context.Context, vector *fileservice.IOVector) error {
	fs.requested = vector.Entries[0].Size
	return errors.New("injected read stop")
}

func TestSingleProcessLeaseJournalBoundsRecordReadBeforeAllocation(t *testing.T) {
	fs := new(boundedJournalReadFS)
	journal := &fileServiceLeaseJournal{fs: fs}
	_, err := journal.read(t.Context(), "record", maxJournalRecordSize+1)
	require.ErrorContains(t, err, "invalid lease journal record size")
	require.Zero(t, fs.requested, "oversized metadata must be rejected before filesystem allocation")
	_, err = journal.read(t.Context(), "record", 23)
	require.ErrorContains(t, err, "injected read stop")
	require.EqualValues(t, 23, fs.requested, "read must use the bounded list size, never Size=-1")
}

func TestSingleProcessLeaseJournalSerializesSameNamespaceAndHonorsContext(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("single-process-journal", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	firstJournal, err := NewSingleProcessFileServiceLeaseJournal(fs, "sirius/read-leases/shard-1")
	require.NoError(t, err)
	secondJournal, err := NewSingleProcessFileServiceLeaseJournal(fs, "/sirius/read-leases/shard-1/")
	require.NoError(t, err)
	first := firstJournal.(*fileServiceLeaseJournal)
	second := secondJournal.(*fileServiceLeaseJournal)

	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.admission.RunExclusive(
			context.Background(), first.admissionKey,
			func(context.Context) error {
				close(entered)
				<-release
				return nil
			},
		)
	}()
	<-entered

	secondEntered := make(chan struct{})
	waitCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.admission.RunExclusive(
			waitCtx, second.admissionKey,
			func(context.Context) error {
				close(secondEntered)
				return nil
			},
		)
	}()
	cancel()
	require.ErrorIs(t, <-secondDone, context.Canceled)
	select {
	case <-secondEntered:
		t.Fatal("same-namespace callback entered while the first owner was live")
	default:
	}
	close(release)
	require.NoError(t, <-firstDone)
}

func TestSingleProcessLeaseJournalPanicDoesNotRetainNamespace(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("single-process-journal-panic", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewSingleProcessFileServiceLeaseJournal(fs, "sirius/read-leases/shard-2")
	require.NoError(t, err)
	concrete := journal.(*fileServiceLeaseJournal)

	func() {
		defer func() { require.Equal(t, "injected panic", recover()) }()
		_ = concrete.admission.RunExclusive(
			context.Background(), concrete.admissionKey,
			func(context.Context) error { panic("injected panic") },
		)
	}()

	called := false
	require.NoError(t, concrete.admission.RunExclusive(
		context.Background(), concrete.admissionKey,
		func(context.Context) error {
			called = true
			return nil
		},
	))
	require.True(t, called, "the next same-namespace owner must not deadlock")
}

func TestSingleProcessLeaseJournalReplaysExactDurableLease(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("single-process-replay", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	journal, err := NewSingleProcessFileServiceLeaseJournal(fs, "sirius/read-leases/shard-7")
	require.NoError(t, err)
	candidate, err := Export(scanQuery())
	require.NoError(t, err)
	read := candidate.Reads()[0]
	now := time.Now()
	provider := &fakeProvider{facts: SnapshotFacts{
		Manifest:        []byte("exact-manifest"),
		CanonicalSchema: read.Schema,
		ObjectNames:     []string{"object-a", "object-b"},
	}}
	originalProtector := new(fakeProtector)
	original := NewPersistentLeaseManager(1, originalProtector, journal)
	require.NoError(t, original.Replay(ctx))
	wires, err := Admit(ctx, AdmissionRequest{
		Candidate: candidate, Provider: provider, Leases: original,
		AccountID: 7, QueryID: []byte("query-7"), SnapshotTS: make([]byte, 12),
		AuthorizedClientSPKIHash: testClientSPKIHash(), TTL: time.Minute,
		ReadOnly: true, Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)), Now: now,
	})
	require.NoError(t, err)
	require.Len(t, wires, 1)
	readWire, err := UnmarshalTaeRead(wires[0], uint64(now.UnixMilli()))
	require.NoError(t, err)

	replayProtector := new(fakeProtector)
	replayed := NewPersistentLeaseManager(1, replayProtector, journal)
	replayed.now = func() time.Time { return now }
	require.NoError(t, replayed.Replay(ctx))
	lease, ok := resolveLease(replayed, readWire.ReadRef)
	require.True(t, ok)
	require.Equal(t, wires[0], lease.Wire)
	require.Equal(t, provider.facts.Manifest, lease.Manifest)
	require.Equal(t, provider.facts.CanonicalSchema, lease.CanonicalSchema)
	require.Equal(t, provider.facts.ObjectNames, lease.ObjectNames)
	require.Equal(t, testClientSPKIHash(), lease.AuthorizedClientSPKIHash)
	require.Equal(t, ReadConsumerFlight, lease.Consumer)
	require.Equal(t, 1, replayProtector.registered)
}
