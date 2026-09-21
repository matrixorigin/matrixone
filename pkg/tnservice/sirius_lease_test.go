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

package tnservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func testSiriusLeaseBootstrapContext(
	t *testing.T,
	shard metadata.TNShard,
) options.PreGCBootstrapContext {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("tn-sirius-lease", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	return options.PreGCBootstrapContext{
		SharedFileService: fs,
		Shard:             shard,
		StorageGeneration: testSiriusStorageGeneration,
		Protector: gc.SidecarReadProtector{
			Manager: gc.NewSyncProtectionManager(),
		},
	}
}

func testSiriusStorageGeneration() ([]byte, error) {
	generation := sha256.Sum256([]byte("test-owned-locked-directory"))
	return generation[:], nil
}

func TestSiriusTAELeaseBootstrapPublishesOnlyAfterStorageSuccess(t *testing.T) {
	shard := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 11, LogShardID: 12},
		ReplicaID:     13,
	}
	broker := substrait.NewLeaseManagerBroker()
	bootstrap, err := newSiriusTAELeaseBootstrap(broker, shard, DefaultSiriusReadLeaseCapacity)
	require.NoError(t, err)
	require.NoError(t, bootstrap.bootstrap(
		context.Background(), testSiriusLeaseBootstrapContext(t, shard)))

	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "not published",
		"pre-GC replay must not publish a partially opened storage")
	require.NoError(t, bootstrap.finish(nil))
	manager, identity, err := broker.Acquire()
	require.NoError(t, err)
	require.True(t, manager.DurableReady())
	generation, err := testSiriusStorageGeneration()
	require.NoError(t, err)
	require.Equal(t, "tae-tn-shard/11/replica/13/generation/"+hex.EncodeToString(generation), identity)
}

func TestSiriusTAELeaseBootstrapOpenFailurePublishesNothing(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 21}}
	broker := substrait.NewLeaseManagerBroker()
	bootstrap, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	args := testSiriusLeaseBootstrapContext(t, shard)
	require.NoError(t, bootstrap.bootstrap(context.Background(), args))

	openErr := errors.New("injected storage-open failure")
	require.ErrorIs(t, bootstrap.finish(openErr), openErr)
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "not published")

	// Abort restores only the never-published state, so a real storage retry in
	// the same process can replay and become the one published generation.
	retry, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	require.NoError(t, retry.bootstrap(context.Background(), args))
	require.NoError(t, retry.finish(nil))
	_, _, err = broker.Acquire()
	require.NoError(t, err)
}

func TestSiriusTAELeaseBootstrapRejectsReplacementGeneration(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 31}}
	broker := substrait.NewLeaseManagerBroker()
	args := testSiriusLeaseBootstrapContext(t, shard)
	first, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	require.NoError(t, first.bootstrap(context.Background(), args))
	require.NoError(t, first.finish(nil))

	second, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	require.ErrorContains(t,
		second.bootstrap(context.Background(), args),
		"second TAE lease manager generation",
	)
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "sealed")
}

func TestSiriusTAELeaseBootstrapValidatesShardAndCapacity(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 41}}
	broker := substrait.NewLeaseManagerBroker()
	_, err := newSiriusTAELeaseBootstrap(broker, shard, 0)
	require.Error(t, err)
	_, err = newSiriusTAELeaseBootstrap(broker, shard, MaxSiriusReadLeaseCapacity+1)
	require.Error(t, err)

	bootstrap, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	other := shard
	other.ReplicaID++
	require.NotEqual(t, SiriusTAELeaseStorageIdentity(shard), SiriusTAELeaseStorageIdentity(other))
	require.Equal(t, siriusTAELeaseJournalPrefix(shard), siriusTAELeaseJournalPrefix(other),
		"replay namespace must remain shard-stable while publication identity changes")
	require.ErrorContains(t,
		bootstrap.bootstrap(context.Background(), testSiriusLeaseBootstrapContext(t, other)),
		"invalid co-located TAE pre-GC bootstrap context",
	)
}

func TestSiriusReadLeaseCapacityDefaultAndBounds(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaultValue()
	require.Equal(t, DefaultSiriusReadLeaseCapacity, cfg.Txn.Storage.SiriusReadLeaseCapacity)

	cfg = &Config{UUID: "tn-capacity"}
	cfg.Txn.Storage.SiriusReadLeaseCapacity = -1
	require.ErrorContains(t, cfg.Validate(), "sirius read lease capacity")

	cfg = &Config{UUID: "tn-capacity"}
	cfg.Txn.Storage.SiriusReadLeaseCapacity = MaxSiriusReadLeaseCapacity + 1
	require.ErrorContains(t, cfg.Validate(), "sirius read lease capacity")
}

func TestSiriusTAELeaseBootstrapRequiresExplicitOptIn(t *testing.T) {
	legacy := &store{cfg: &Config{InStandalone: true}}
	legacy.cfg.Txn.Storage.SiriusReadLeaseCapacity = DefaultSiriusReadLeaseCapacity
	WithSiriusLeaseManagerBroker(nil)(legacy)
	for _, shardID := range []uint64{1, 2} {
		bootstrap, err := legacy.newSiriusTAELeaseBootstrap(metadata.TNShard{
			TNShardRecord: metadata.TNShardRecord{ShardID: shardID},
		})
		require.NoError(t, err)
		require.Nil(t, bootstrap, "ordinary standalone storage must not acquire Sirius authority")
	}

	broker := substrait.NewLeaseManagerBroker()
	verified := &store{cfg: &Config{InStandalone: true}}
	verified.cfg.Txn.Storage.SiriusReadLeaseCapacity = DefaultSiriusReadLeaseCapacity
	WithSiriusLeaseManagerBroker(broker)(verified)
	bootstrap, err := verified.newSiriusTAELeaseBootstrap(metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 1},
	})
	require.NoError(t, err)
	require.NotNil(t, bootstrap)

	distributed := &store{cfg: &Config{}}
	distributed.cfg.Txn.Storage.SiriusReadLeaseCapacity = DefaultSiriusReadLeaseCapacity
	WithSiriusLeaseManagerBroker(broker)(distributed)
	bootstrap, err = distributed.newSiriusTAELeaseBootstrap(metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 1},
	})
	require.ErrorContains(t, err, "explicitly owned standalone TN")
	require.Nil(t, bootstrap)
}

func TestSiriusTAELeaseBootstrapRejectsMissingOrFailedGeneration(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 43}}
	broker := substrait.NewLeaseManagerBroker()
	bootstrap, err := newSiriusTAELeaseBootstrap(broker, shard, 1)
	require.NoError(t, err)
	args := testSiriusLeaseBootstrapContext(t, shard)
	args.StorageGeneration = nil
	require.ErrorContains(t, bootstrap.bootstrap(t.Context(), args), "invalid co-located")
	args.StorageGeneration = func() ([]byte, error) { return []byte("short"), nil }
	require.ErrorContains(t, bootstrap.bootstrap(t.Context(), args), "invalid locked")
	generationErr := errors.New("directory generation unavailable")
	args.StorageGeneration = func() ([]byte, error) { return nil, generationErr }
	require.ErrorIs(t, bootstrap.bootstrap(t.Context(), args), generationErr)
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "not published")
}

func storeStaleSiriusLease(
	t *testing.T,
	fs fileservice.FileService,
	shard metadata.TNShard,
	consumer substrait.ReadConsumer,
) []byte {
	t.Helper()
	schema := []byte("stale-schema")
	manifest := []byte("stale-manifest")
	schemaDigest := sha256.Sum256(schema)
	manifestDigest := sha256.Sum256(manifest)
	readRef := bytes.Repeat([]byte{byte(consumer) + 1}, sha256.Size)
	read := &substrait.TaeRead{
		ProtocolVersion: substrait.TaeReadProtocolVersion,
		ReadRef:         readRef,
		QueryID:         []byte("stale-query"),
		AccountID:       1,
		DatabaseID:      2,
		TableID:         3,
		SnapshotTS:      make([]byte, 12),
		SchemaDigest:    schemaDigest[:],
		ManifestSHA256:  manifestDigest[:],
		CapabilityHash:  substrait.CapabilityHash[:],
		ExpiresAtUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
	}
	wire, err := substrait.MarshalTaeRead(read)
	require.NoError(t, err)
	marker := make([]byte, sha256.Size)
	if consumer == substrait.ReadConsumerFlight {
		marker[0] = 1
	}
	journal, err := substrait.NewSingleProcessFileServiceLeaseJournal(
		fs, siriusTAELeaseJournalPrefix(shard))
	require.NoError(t, err)
	stored, err := journal.StoreIfCapacity(context.Background(), []*substrait.Lease{{
		Read: read, Wire: wire, Manifest: manifest, CanonicalSchema: schema,
		AuthorizedClientSPKIHash: marker,
		ObjectNames:              []string{"stale-object"},
		Consumer:                 consumer,
	}}, DefaultSiriusReadLeaseCapacity)
	require.NoError(t, err)
	require.Equal(t, 1, stored)
	return readRef
}

func countSiriusLeaseRecords(
	t *testing.T,
	fs fileservice.FileService,
	shard metadata.TNShard,
) int {
	t.Helper()
	journal, err := substrait.NewSingleProcessFileServiceLeaseJournal(
		fs, siriusTAELeaseJournalPrefix(shard))
	require.NoError(t, err)
	count := 0
	require.NoError(t, journal.Load(context.Background(), func(*substrait.Lease) error {
		count++
		return nil
	}))
	return count
}

func TestSiriusTAELeaseBootstrapWithoutBrokerReleasesStaleEmbeddedLease(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 51}}
	fs, err := fileservice.NewMemoryFS("stale-embedded", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	storeStaleSiriusLease(t, fs, shard, substrait.ReadConsumerEmbeddedTAE)
	manager := gc.NewSyncProtectionManager()
	bootstrap, err := newSiriusTAELeaseBootstrap(nil, shard, DefaultSiriusReadLeaseCapacity)
	require.NoError(t, err)
	require.NoError(t, bootstrap.bootstrap(context.Background(), options.PreGCBootstrapContext{
		SharedFileService: fs,
		Shard:             shard,
		Protector:         gc.SidecarReadProtector{Manager: manager},
		StorageGeneration: testSiriusStorageGeneration,
	}))
	require.NoError(t, bootstrap.finish(nil))
	require.Zero(t, countSiriusLeaseRecords(t, fs, shard))
	require.False(t, manager.IsProtected("stale-object"))
}

func TestSiriusTAELeaseBootstrapWithoutBrokerRejectsStaleFlightLease(t *testing.T) {
	shard := metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 61}}
	fs, err := fileservice.NewMemoryFS("stale-flight", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	storeStaleSiriusLease(t, fs, shard, substrait.ReadConsumerFlight)
	manager := gc.NewSyncProtectionManager()
	bootstrap, err := newSiriusTAELeaseBootstrap(nil, shard, DefaultSiriusReadLeaseCapacity)
	require.NoError(t, err)
	err = bootstrap.bootstrap(context.Background(), options.PreGCBootstrapContext{
		SharedFileService: fs,
		Shard:             shard,
		Protector:         gc.SidecarReadProtector{Manager: manager},
		StorageGeneration: testSiriusStorageGeneration,
	})
	require.ErrorContains(t, err, "unreconciled Flight reads")
	require.Equal(t, 1, countSiriusLeaseRecords(t, fs, shard))
	require.True(t, manager.IsProtected("stale-object"),
		"failed startup must not expose an unreconciled Flight object's GC protection")
}
