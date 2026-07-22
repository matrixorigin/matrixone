// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceBootstrapRestoresHAKeeperAndWAL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir := t.TempDir()
	backupPath := filepath.Join(dir, "hakeeper_backup.data")
	walPath := filepath.Join(dir, "wal_data.bin")
	backup := &pb.BackupData{
		NextID: hakeeper.K8SIDRangeEnd + 123,
		NextIDByKey: map[string]uint64{
			"index_key":          456,
			"____server_conn_id": 789,
		},
	}
	data, err := backup.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(backupPath, data, 0644))
	require.NoError(t, writeTestWALDataFile(walPath, []WALEntry{{
		DSN:        1,
		SafeDSN:    1,
		RaftIndex:  10,
		RaftTerm:   1,
		EntryCount: 1,
		RawData:    testRawLogEntry(1, 1, 1),
	}}))
	// A recovery artifact can outlive a deleted PVC. Its sidecar must never
	// make a fresh destination shard skip replay.
	require.NoError(t, writeWALRecoveryState(context.Background(),
		getWALRecoveryStateFile(walPath), walRecoveryState{
			Version:          walRecoveryStateVersion,
			WALDigest:        "stale-destination",
			EntryCount:       99,
			CompletedEntries: 99,
			Complete:         true,
		}))

	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	cfg.DisableWorkers = false
	cfg.BootstrapConfig.InitHAKeeperMembers = []string{"131072:" + cfg.UUID}
	cfg.BootstrapConfig.Restore.FilePath = backupPath
	cfg.BootstrapConfig.Restore.WALDataPath = walPath
	cfg.BootstrapConfig.Restore.Enabled = true
	cfg.HAKeeperClientConfig.ServiceAddresses = []string{cfg.LogServiceServiceAddr()}
	fileServices := newFS()
	s, err := NewService(
		cfg,
		fileServices,
		nil,
		WithBackendFilter(func(morpc.Message, string) bool { return true }),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.True(t, s.walRecovery.configured)
	require.True(t, s.walRecovery.pending.Load())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, s.BootstrapHAKeeper(ctx, cfg))
	require.False(t, s.walRecovery.pending.Load())

	state, err := s.store.getCheckerState()
	require.NoError(t, err)
	require.GreaterOrEqual(t, state.NextId, backup.NextID)
	require.Equal(t, backup.NextIDByKey["index_key"], state.NextIDByKey["index_key"])
	require.Equal(t, backup.NextIDByKey["____server_conn_id"], state.NextIDByKey["____server_conn_id"])
	require.Greater(t, state.IDWatermarkRestoreGeneration, uint64(0))
	require.False(t, state.LogServiceRecoveryPending)
	require.True(t, state.LogServiceRecoveryCompleted)

	recoveryState, err := readWALRecoveryState(ctx, getWALRecoveryStateFile(walPath))
	require.NoError(t, err)
	require.NotNil(t, recoveryState)
	require.True(t, recoveryState.Complete)
	require.Equal(t, uint64(1), recoveryState.EntryCount)
	require.Equal(t, uint64(1), recoveryState.CompletedEntries)
	require.Greater(t, recoveryState.LastLSN, recoveryState.BaseLSN)
	require.Zero(t, recoveryState.OriginalLeaseHolderID)
	leaseHolder, err := s.store.read(ctx, firstLogShardID, leaseHolderIDQuery{})
	require.NoError(t, err)
	require.Equal(t, recoveryState.OriginalLeaseHolderID, leaseHolder)
	require.NotEqual(t, walRecoveryLeaseHolderID, leaseHolder)

	marker, err := s.getWALRecoveryMarker(ctx, firstLogShardID)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, recoveryState.WALDigest, marker.Digest)
	require.Equal(t, recoveryState.EntryCount, marker.EntryCount)

	// The replicated destination marker, not the artifact-local sidecar, is
	// authoritative. Losing the sidecar must not replay a completed WAL.
	require.NoError(t, os.Remove(getWALRecoveryStateFile(walPath)))
	require.NoError(t, s.RecoverWALData(ctx, cfg))
	leaseHolder, err = s.store.read(ctx, firstLogShardID, leaseHolderIDQuery{})
	require.NoError(t, err)
	require.Equal(t, recoveryState.OriginalLeaseHolderID, leaseHolder)
	require.NoError(t, os.WriteFile(
		getWALRecoveryStateFile(walPath), []byte("{"), 0644))
	require.NoError(t, s.RecoverWALData(ctx, cfg))

	// Normal service use can change the lease after recovery. Keeping the
	// restore configuration for a later restart must not reject that lease or
	// replay the WAL again once the durable sidecar is complete.
	require.NoError(t, s.store.getOrExtendTNLease(ctx, firstLogShardID, 42))
	require.NoError(t, s.RecoverWALData(ctx, cfg))
	leaseHolder, err = s.store.read(ctx, firstLogShardID, leaseHolderIDQuery{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), leaseHolder)

	// A non-coordinator may restart by itself after recovery. It has no WAL
	// artifact and must not wait for the coordinator to restart with it.
	nonCoordinatorCfg := cfg
	nonCoordinatorCfg.BootstrapConfig.Restore.WALDataPath = ""
	s.walRecovery.coordinator = false
	s.setWALRecoveryInProgress(true)
	require.NoError(t, s.BootstrapHAKeeper(ctx, nonCoordinatorCfg))
	require.False(t, s.walRecovery.pending.Load())

	local, err := fileservice.Get[fileservice.FileService](fileServices, defines.LocalFileServiceName)
	require.NoError(t, err)
	_, err = local.StatFile(ctx, restoredTagFile)
	require.NoError(t, err)

	// A restart normally retains the restore configuration. It must verify the
	// replicated destination marker, preserve the current lease and skip replay.
	s.walRecovery.coordinator = true
	s.setWALRecoveryInProgress(true)
	require.NoError(t, s.BootstrapHAKeeper(ctx, cfg))
	require.False(t, s.walRecovery.pending.Load())
	restartedState, err := s.store.getCheckerState()
	require.NoError(t, err)
	require.GreaterOrEqual(t, restartedState.NextId, state.NextId)
	require.Equal(t, state.NextIDByKey, restartedState.NextIDByKey)
	leaseHolder, err = s.store.read(ctx, firstLogShardID, leaseHolderIDQuery{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), leaseHolder)

	canceledCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	_, err = s.waitWALRecoveryCoordinator(canceledCtx, 2)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWALRecoveryRejectsDirtyDestinationAndKeepsFence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal_data.bin")
	require.NoError(t, writeTestWALDataFile(walPath, nil))

	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	cfg.DisableWorkers = false
	cfg.BootstrapConfig.InitHAKeeperMembers = []string{"131072:" + cfg.UUID}
	cfg.HAKeeperClientConfig.ServiceAddresses = []string{cfg.LogServiceServiceAddr()}
	s, err := NewService(
		cfg,
		newFS(),
		nil,
		WithBackendFilter(func(morpc.Message, string) bool { return true }),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.BootstrapHAKeeper(ctx, cfg))
	require.Eventually(t, func() bool {
		info, ok := s.getShardInfo(ctx, firstLogShardID, true, true)
		return ok && info.LeaderID != 0
	}, 15*time.Second, 20*time.Millisecond)
	require.NoError(t, s.store.getOrExtendTNLease(ctx, firstLogShardID, 42))
	_, err = s.store.append(ctx, firstLogShardID, buildUserEntryCmd(42, []byte("existing")))
	require.NoError(t, err)

	restoreCfg := cfg
	restoreCfg.BootstrapConfig.Restore.WALDataPath = walPath
	restoreCfg.BootstrapConfig.Restore.Enabled = true
	err = s.recoverWALData(ctx, restoreCfg, true)
	require.ErrorContains(t, err, "has no completion marker")
	err = s.RecoverWALData(ctx, restoreCfg)
	require.ErrorContains(t, err, "requires a clean destination Log shard")
	err = s.RecoverWALData(ctx, restoreCfg)
	require.ErrorContains(t, err, "requires a clean destination Log shard")

	leaseHolder, err := s.store.read(ctx, firstLogShardID, leaseHolderIDQuery{})
	require.NoError(t, err)
	require.Equal(t, walRecoveryLeaseHolderID, leaseHolder)
	_, err = s.store.append(ctx, firstLogShardID, buildUserEntryCmd(42, []byte("must-fail")))
	require.Error(t, err)
}

func TestGetBackupData(t *testing.T) {
	var nextID uint64 = 900
	nextIDByKey := make(map[string]uint64)
	nextIDByKey["a"] = 1
	nextIDByKey["b"] = 2

	backup := pb.BackupData{
		NextID:      nextID,
		NextIDByKey: nextIDByKey,
	}
	data, err := backup.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	ctx := context.Background()
	dir := t.TempDir()
	name := defines.LocalFileServiceName
	fs, err := fileservice.NewLocalFS(ctx, name, dir, fileservice.DisabledCacheConfig, nil)
	assert.Nil(t, err)
	assert.NotNil(t, fs)

	s := &Service{
		fileService: fs,
	}
	// If the file do not exist, do not return error.
	restore, err := s.getBackupData(ctx)
	assert.NoError(t, err)
	assert.Nil(t, restore)

	const localBackupData = "restore_data"
	ioVec := fileservice.IOVector{
		FilePath: localBackupData,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Size:   int64(len(data)),
		Data:   data,
	}
	err = fs.Write(ctx, ioVec)
	assert.NoError(t, err)

	restore, err = s.getBackupData(ctx)
	assert.NoError(t, err)
	assert.Nil(t, restore)

	s.cfg.BootstrapConfig.Restore.FilePath = "missing_backup_data"
	restore, err = s.getBackupData(ctx)
	assert.Error(t, err)
	assert.Nil(t, restore)

	s.cfg.BootstrapConfig.Restore.FilePath = localBackupData
	restore, err = s.getBackupData(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, restore)
	assert.Equal(t, nextID, restore.NextID)
	assert.Equal(t, nextIDByKey, restore.NextIDByKey)

	rawPath := filepath.Join(dir, "raw_backup_data")
	assert.NoError(t, os.WriteFile(rawPath, data, 0644))
	s.cfg.BootstrapConfig.Restore.FilePath = rawPath
	restore, err = s.getBackupData(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, restore)
	assert.Equal(t, nextID, restore.NextID)
	assert.Equal(t, nextIDByKey, restore.NextIDByKey)
}

func TestGetBackupDataForBootstrap(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fs, err := fileservice.NewLocalFS(
		ctx,
		defines.LocalFileServiceName,
		root,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	s := &Service{fileService: fs}
	cfg := DefaultConfig()
	cfg.BootstrapConfig.Restore.Enabled = true
	cfg.BootstrapConfig.Restore.WALDataPath = ""

	setPath := func(path string) {
		s.cfg.BootstrapConfig.Restore.FilePath = path
		cfg.BootstrapConfig.Restore.FilePath = path
	}
	for _, path := range []string{
		"missing-custom-backup.data",
		filepath.Join(root, "missing-absolute-backup.data"),
	} {
		setPath(path)
		backup, err := s.getBackupDataForBootstrap(ctx, cfg)
		require.NoError(t, err)
		require.Nil(t, backup)
	}

	// The member that owns wal-data-path is the coordinator and must have a
	// valid HAKeeper backup before it can replay WAL.
	cfg.BootstrapConfig.Restore.WALDataPath = filepath.Join(root, "wal_data.bin")
	_, err = s.getBackupDataForBootstrap(ctx, cfg)
	require.Error(t, err)

	// Only absence is optional on a non-coordinator. A present but malformed
	// backup must still fail closed.
	cfg.BootstrapConfig.Restore.WALDataPath = ""
	malformed := filepath.Join(root, "malformed-backup.data")
	require.NoError(t, os.WriteFile(malformed, []byte("not-protobuf"), 0644))
	setPath(malformed)
	_, err = s.getBackupDataForBootstrap(ctx, cfg)
	require.Error(t, err)

	// Outside LogService recovery, missing restore artifacts keep the historical
	// optional behavior so normal startup is unchanged.
	cfg.BootstrapConfig.Restore.Enabled = false
	for _, path := range []string{
		"normal-restore-missing.data",
		filepath.Join(root, "normal-restore-missing-absolute.data"),
	} {
		setPath(path)
		backup, err := s.getBackupDataForBootstrap(ctx, cfg)
		require.NoError(t, err)
		require.Nil(t, backup)
	}
}

func TestParseBackupDataRejectsInvalidWatermarks(t *testing.T) {
	_, err := parseBackupData(nil)
	require.Error(t, err)

	data, err := (&pb.BackupData{NextID: ^uint64(0)}).Marshal()
	require.NoError(t, err)
	_, err = parseBackupData(data)
	require.Error(t, err)

	data, err = (&pb.BackupData{
		NextID:      100,
		NextIDByKey: map[string]uint64{"index_key": ^uint64(0)},
	}).Marshal()
	require.NoError(t, err)
	_, err = parseBackupData(data)
	require.Error(t, err)
}

func TestRestoredHAKeeperStateReached(t *testing.T) {
	backup := &pb.BackupData{
		NextID: hakeeper.K8SIDRangeEnd + 10,
		NextIDByKey: map[string]uint64{
			"index_key":          200,
			"____server_conn_id": 900,
			"_mo_bootstrap":      1,
		},
	}
	assert.False(t, restoredHAKeeperStateReached(backup, nil))
	assert.False(t, restoredHAKeeperStateReached(backup, &pb.CheckerState{
		NextId:      hakeeper.K8SIDRangeEnd + 9,
		NextIDByKey: backup.NextIDByKey,
	}))
	assert.False(t, restoredHAKeeperStateReached(backup, &pb.CheckerState{
		NextId: hakeeper.K8SIDRangeEnd + 10,
		NextIDByKey: map[string]uint64{
			"index_key":          199,
			"____server_conn_id": 900,
			"_mo_bootstrap":      1,
		},
	}))
	assert.True(t, restoredHAKeeperStateReached(backup, &pb.CheckerState{
		NextId: hakeeper.K8SIDRangeEnd + 10,
		NextIDByKey: map[string]uint64{
			"index_key":          200,
			"____server_conn_id": 900,
			"_mo_bootstrap":      1,
		},
	}))

	backup.NextID = 10
	assert.False(t, restoredHAKeeperStateReached(backup, &pb.CheckerState{
		NextId:      hakeeper.K8SIDRangeEnd - 1,
		NextIDByKey: backup.NextIDByKey,
	}))
	assert.True(t, restoredHAKeeperStateReached(backup, &pb.CheckerState{
		NextId:      hakeeper.K8SIDRangeEnd,
		NextIDByKey: backup.NextIDByKey,
	}))
}

func TestBackupOnlyRestoreDoesNotSendRecoveryWatermarkCommand(t *testing.T) {
	backup := &pb.BackupData{NextID: hakeeper.K8SIDRangeEnd + 1}
	require.NoError(t, (&Service{}).restoreHAKeeperIDWatermarks(
		context.Background(), backup, false))
}

func TestServiceBootstrap(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			assert.Greater(t, len(s.cfg.BootstrapConfig.InitHAKeeperMembers), 0)
			member := s.cfg.BootstrapConfig.InitHAKeeperMembers[0]
			parts := strings.Split(member, ":")
			assert.Equal(t, 2, len(parts))
			s.cfg.UUID = parts[1]
			assert.NoError(t, s.BootstrapHAKeeper(ctx, s.cfg))
		}
		runServiceTest(t, false, false, fn)
	})

	t.Run("context cancelled", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			cancel()
			assert.Greater(t, len(s.cfg.BootstrapConfig.InitHAKeeperMembers), 0)
			member := s.cfg.BootstrapConfig.InitHAKeeperMembers[0]
			parts := strings.Split(member, ":")
			assert.Equal(t, 2, len(parts))
			s.cfg.UUID = parts[1]
			s.cfg.BootstrapConfig.Restore.FilePath = ""
			assert.NoError(t, s.BootstrapHAKeeper(ctx, s.cfg))
		}
		runServiceTest(t, false, false, fn)
	})

	t.Run("WAL recovery requires HAKeeper backup", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			member := s.cfg.BootstrapConfig.InitHAKeeperMembers[0]
			parts := strings.Split(member, ":")
			require.Equal(t, 2, len(parts))
			s.cfg.UUID = parts[1]
			s.cfg.BootstrapConfig.Restore.FilePath = ""
			s.cfg.BootstrapConfig.Restore.WALDataPath = filepath.Join(t.TempDir(), "wal_data.bin")
			s.cfg.BootstrapConfig.Restore.Enabled = true
			err := s.BootstrapHAKeeper(ctx, s.cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires a valid HAKeeper backup")
		}
		runServiceTest(t, false, false, fn)
	})

	// Guards the #24300 review follow-up: if startReplicas's zombie
	// self-check has classified the local HAKeeper (shardID=0, replicaID)
	// as removed, BootstrapHAKeeper must honor that decision and refuse
	// to re-start the replica. Otherwise the production
	// NewService -> Start -> BootstrapHAKeeper path would immediately
	// resurrect the zombie that startReplicas just skipped.
	t.Run("skips zombie HAKeeper replica", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			assert.Greater(t, len(s.cfg.BootstrapConfig.InitHAKeeperMembers), 0)
			member := s.cfg.BootstrapConfig.InitHAKeeperMembers[0]
			parts := strings.Split(member, ":")
			require.Equal(t, 2, len(parts))
			s.cfg.UUID = parts[1]
			replicaID, ok := s.cfg.Bootstrapping()
			require.True(t, ok)

			// Seed the skippedZombies set as if startReplicas had already
			// classified this HAKeeper replica as removed.
			s.store.mu.Lock()
			s.store.mu.skippedZombies = map[zombieKey]struct{}{
				{shardID: hakeeper.DefaultHAKeeperShardID, replicaID: replicaID}: {},
			}
			s.store.mu.Unlock()

			require.NoError(t, s.BootstrapHAKeeper(ctx, s.cfg))
			// startHAKeeperReplica was not called, so haKeeperReplicaID is
			// still zero and dragonboat does not know about shard 0.
			assert.Equal(t, uint64(0), atomic.LoadUint64(&s.store.haKeeperReplicaID))
		}
		runServiceTest(t, false, false, fn)
	})
}
