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
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	util "github.com/matrixorigin/matrixone/pkg/util/logservice"
	"go.uber.org/zap"
)

const restoredTagFile = "./RESTORED"

func (s *Service) BootstrapHAKeeper(ctx context.Context, cfg Config) error {
	replicaID, bootstrapping := cfg.Bootstrapping()
	if !bootstrapping {
		return nil
	}
	// Honor the decision that startReplicas's membership self-check already
	// made. If (HAKeeperShardID, replicaID) has been classified as a zombie
	// -- i.e. the authoritative peer view no longer lists this replica in
	// shard 0 membership -- bootstrap must not recreate it. Starting it here
	// would undo the skip and resurrect the exact zombie startReplicas
	// avoided. The existing HAKeeper L/Add -> L/Kill -> L/Start recovery
	// flow remains authoritative; once HAKeeper has a leader it will
	// re-add this store with a fresh replicaID.
	if s.store.isSkippedZombie(hakeeper.DefaultHAKeeperShardID, replicaID) {
		s.runtime.SubLogger(runtime.SystemInit).Warn(
			"BootstrapHAKeeper: skipping zombie replica, deferring to HAKeeper recovery",
			zap.Uint64("shardID", hakeeper.DefaultHAKeeperShardID),
			zap.Uint64("replicaID", replicaID))
		return nil
	}
	members, err := cfg.GetInitHAKeeperMembers()
	if err != nil {
		return err
	}
	if err := s.store.startHAKeeperReplica(replicaID, members, false); err != nil {
		// let's be a little less strict, when HAKeeper replica is already
		// running as a result of store.startReplicas(), we just ignore the
		// dragonboat.ErrShardAlreadyExist error below.
		if err != dragonboat.ErrShardAlreadyExist {
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to start hakeeper replica", zap.Error(err))
			return err
		}
	}
	numOfLogShards := cfg.BootstrapConfig.NumOfLogShards
	numOfTNShards := cfg.BootstrapConfig.NumOfTNShards
	numOfLogReplicas := cfg.BootstrapConfig.NumOfLogShardReplicas
	nonVotingLocality := util.GetLocalityFromStr(cfg.BootstrapConfig.NonVotingLocality)

	// If the standby mode is enabled, the number of log shard should be 2.
	if cfg.BootstrapConfig.StandbyEnabled {
		numOfLogShards++
	}

	fs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.LocalFileServiceName)
	if err != nil {
		s.runtime.SubLogger(runtime.SystemInit).Error("failed to get file service instance", zap.Error(err))
		return err
	}

	var nextID uint64
	var nextIDByKey map[string]uint64
	backupRestoreTagExists := false
	backup, err := s.getBackupDataForBootstrap(ctx, cfg)
	if err != nil {
		s.runtime.SubLogger(runtime.SystemInit).Error("failed to get backup data", zap.Error(err))
		return err
	}
	if backup != nil { // We are trying to restore from a backup.
		_, err := fs.StatFile(ctx, restoredTagFile)
		if err == nil {
			backupRestoreTagExists = true
		} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to stat restore tag file",
				zap.Error(err))
			return err
		}
		s.runtime.SubLogger(runtime.SystemInit).Info("restore hakeeper data",
			zap.Uint64("next ID", backup.NextID),
			zap.Any("next ID by key", backup.NextIDByKey),
			zap.Bool("restore tag exists", backupRestoreTagExists),
			zap.Bool("force", s.cfg.BootstrapConfig.Restore.Force),
		)
		// Always carry backup IDs into the initial-cluster proposal when
		// restore is configured. RESTORED is only an acknowledgement that a
		// previous restore reached HAKeeper state; old versions wrote it too
		// early, so it must not suppress the backup payload by itself.
		nextID = backup.NextID
		nextIDByKey = backup.NextIDByKey
	} else {
		s.runtime.SubLogger(runtime.SystemInit).Info("backup is nil")
	}

	logServiceRecovery := cfg.BootstrapConfig.Restore.Enabled
	walRecoveryCoordinator := cfg.BootstrapConfig.Restore.WALDataPath != ""
	restoreConfigured := backup != nil || logServiceRecovery
	if walRecoveryCoordinator && backup == nil {
		return moerr.NewInternalError(ctx,
			"WAL recovery requires a valid HAKeeper backup file")
	}

	initialClusterProposed := false
	var lastInitialClusterErr error
	for i := 0; i < checkBootstrapCycles; i++ {
		select {
		case <-ctx.Done():
			s.runtime.SubLogger(runtime.SystemInit).Error("context error", zap.Error(ctx.Err()))
			if restoreConfigured {
				return moerr.AttachCause(ctx, ctx.Err())
			}
			return nil
		default:
		}
		s.runtime.SubLogger(runtime.SystemInit).Info("before initial cluster info")
		applied, err := s.store.setInitialClusterInfoWithRecoveryResult(
			numOfLogShards,
			numOfTNShards,
			numOfLogReplicas,
			nextID,
			nextIDByKey,
			nonVotingLocality,
			logServiceRecovery,
		)
		if err != nil {
			lastInitialClusterErr = err
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to set initial cluster info", zap.Error(err))
			if errors.Is(err, dragonboat.ErrShardNotFound) {
				if restoreConfigured {
					return err
				}
				return nil
			}
			time.Sleep(time.Second)
			continue
		}
		initialClusterProposed = true
		s.runtime.SubLogger(runtime.SystemInit).Info("initial cluster info set",
			zap.Bool("applied", applied))
		break
	}
	if backup != nil {
		if !initialClusterProposed {
			if lastInitialClusterErr != nil {
				return lastInitialClusterErr
			}
			return moerr.NewInternalError(ctx, "restore backup configured but initial cluster info was not proposed")
		}
		// A different LogService may have won the initial-cluster proposal with
		// default IDs. Apply the restore watermarks through an explicit,
		// idempotent HAKeeper command and verify the replicated state before
		// acknowledging the restore.
		if err := s.restoreHAKeeperIDWatermarks(ctx, backup, logServiceRecovery); err != nil {
			return err
		}
		if !backupRestoreTagExists {
			if err := writeRestoredTag(ctx, fs); err != nil {
				s.runtime.SubLogger(runtime.SystemInit).Error("failed to write restore tag file",
					zap.Error(err))
				return err
			}
		}
	}

	// Recover WAL data if configured
	// This must be done after the cluster is initialized and Log shard is running
	if logServiceRecovery {
		state, err := s.store.getCheckerState()
		if err != nil {
			return err
		}
		recoveryAlreadyCompleted := state.LogServiceRecoveryCompleted
		if recoveryAlreadyCompleted && !walRecoveryCoordinator {
			// A non-coordinator can restart after recovery without waiting for the
			// artifact-owning store to restart at the same time.
			s.setWALRecoveryInProgress(false)
			return nil
		}

		if !recoveryAlreadyCompleted {
			coordinator, err := s.waitWALRecoveryCoordinator(ctx, numOfLogReplicas)
			if err != nil {
				return err
			}
			if !coordinator {
				// Only the elected artifact-owning store is allowed to append the WAL.
				s.setWALRecoveryInProgress(false)
				return nil
			}
		}
		if !walRecoveryCoordinator {
			return moerr.NewInternalError(ctx,
				"selected WAL recovery coordinator has no WAL data path")
		}
		s.runtime.SubLogger(runtime.SystemInit).Info("WAL recovery: starting WAL data recovery",
			zap.String("wal_data_path", cfg.BootstrapConfig.Restore.WALDataPath))
		if err := s.recoverWALData(ctx, cfg, recoveryAlreadyCompleted); err != nil {
			s.runtime.SubLogger(runtime.SystemInit).Error("WAL recovery: failed to recover WAL data", zap.Error(err))
			return err
		}
		if err := s.store.completeLogServiceRecovery(ctx); err != nil {
			s.runtime.SubLogger(runtime.SystemInit).Error(
				"WAL recovery: failed to publish replicated completion", zap.Error(err))
			return err
		}
		// Publish completion only after replay and its durable completion record
		// have succeeded. On any failure the replicated heartbeat status remains
		// pending and HAKeeper cannot expose the cluster as Running.
		s.setWALRecoveryInProgress(false)
		s.runtime.SubLogger(runtime.SystemInit).Info("WAL recovery: completed, HAKeeper can now report running state")
	}

	return nil
}

func (s *Service) restoreHAKeeperIDWatermarks(
	ctx context.Context,
	backup *pb.BackupData,
	logServiceRecovery bool,
) error {
	// The explicit watermark command is part of the fenced WAL-recovery
	// protocol. Backup-only restore keeps its existing initial-cluster behavior;
	// it has no replicated recovery intent and must not send this command.
	if !logServiceRecovery {
		return nil
	}
	var lastErr error
	for i := 0; i < checkBootstrapCycles; i++ {
		if err := ctx.Err(); err != nil {
			return moerr.AttachCause(ctx, err)
		}

		state, stateErr := s.store.getCheckerState()
		if stateErr == nil && restoredHAKeeperStateReached(backup, state) &&
			(!logServiceRecovery ||
				(state.LogServiceRecoveryPending && state.LogServiceRecoveryPrepared) ||
				state.LogServiceRecoveryCompleted) {
			return nil
		}

		if err := s.store.restoreIDWatermarks(
			ctx,
			backup.NextID,
			backup.NextIDByKey,
			logServiceRecovery,
		); err != nil {
			lastErr = err
		} else {
			state, err := s.store.getCheckerState()
			if err != nil {
				lastErr = err
			} else if restoredHAKeeperStateReached(backup, state) &&
				(!logServiceRecovery ||
					(state.LogServiceRecoveryPending && state.LogServiceRecoveryPrepared) ||
					state.LogServiceRecoveryCompleted) {
				return nil
			} else {
				lastErr = moerr.NewInternalErrorf(ctx,
					"restored HAKeeper state not confirmed: nextID %d, backup nextID %d, nextIDByKey %v, backup nextIDByKey %v, recovery pending %t, prepared %t",
					state.NextId, backup.NextID, state.NextIDByKey, backup.NextIDByKey,
					state.LogServiceRecoveryPending, state.LogServiceRecoveryPrepared)
			}
		}

		s.runtime.SubLogger(runtime.SystemInit).Warn(
			"HAKeeper ID restore not yet confirmed, retrying",
			zap.Int("attempt", i+1),
			zap.Error(lastErr),
		)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return moerr.AttachCause(ctx, ctx.Err())
		case <-timer.C:
		}
	}
	return moerr.NewInternalErrorf(ctx,
		"failed to restore HAKeeper ID watermarks after %d attempts: %v",
		checkBootstrapCycles, lastErr)
}

func (s *Service) getBackupData(ctx context.Context) (*pb.BackupData, error) {
	filePath := s.cfg.BootstrapConfig.Restore.FilePath
	if filePath == "" {
		return nil, nil
	}

	if filepath.IsAbs(filePath) {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		return parseBackupData(data)
	}

	path, err := fileservice.ParsePath(filePath)
	if err != nil {
		return nil, err
	}

	fsName := defines.LocalFileServiceName
	if path.Service != "" {
		fsName = path.Service
	}

	fs, err := fileservice.Get[fileservice.FileService](s.fileService, fsName)
	if err != nil {
		s.runtime.SubLogger(runtime.SystemInit).Error("failed to get file service instance %s",
			zap.String("fileservice name", fsName),
			zap.Error(err))
		return nil, err
	}

	st, err := fs.StatFile(ctx, filePath)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			if filePath != defaultRestoreFilePath {
				return nil, err
			}
			return nil, nil
		}
		return nil, err
	}

	ioVec := &fileservice.IOVector{
		FilePath: filePath,
		Entries:  make([]fileservice.IOEntry, 1),
	}

	// Read the whole file to one entry.
	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Size:   st.Size,
	}
	if err := fs.Read(ctx, ioVec); err != nil {
		return nil, err
	}
	defer ioVec.Release()

	return parseBackupData(ioVec.Entries[0].Data)
}

func (s *Service) getBackupDataForBootstrap(
	ctx context.Context,
	cfg Config,
) (*pb.BackupData, error) {
	backup, err := s.getBackupData(ctx)
	if err == nil {
		return backup, nil
	}
	if cfg.BootstrapConfig.Restore.WALDataPath == "" &&
		(errors.Is(err, os.ErrNotExist) || moerr.IsMoErrCode(err, moerr.ErrFileNotFound)) {
		// Preserve the historical optional-backup behavior during normal startup.
		// During WAL recovery, only the coordinator owns the recovery artifacts;
		// other initial members join the replicated recovery barrier without them.
		return nil, nil
	}
	return nil, err
}

func parseBackupData(data []byte) (*pb.BackupData, error) {
	var backup pb.BackupData
	if err := backup.Unmarshal(data); err != nil {
		return nil, err
	}
	if backup.NextID == 0 && len(backup.NextIDByKey) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("HAKeeper backup contains no ID watermarks")
	}
	if backup.NextID == math.MaxUint64 {
		return nil, moerr.NewInvalidInputNoCtx("HAKeeper backup NextID is exhausted")
	}
	for key, nextID := range backup.NextIDByKey {
		if nextID == math.MaxUint64 {
			return nil, moerr.NewInvalidInputNoCtxf(
				"HAKeeper backup NextIDByKey is exhausted for key %q", key)
		}
	}
	return &backup, nil
}

func restoredHAKeeperStateReached(backup *pb.BackupData, state *pb.CheckerState) bool {
	if backup == nil {
		return true
	}
	if state == nil {
		return false
	}
	expectedNextID := backup.NextID
	if expectedNextID < hakeeper.K8SIDRangeEnd {
		expectedNextID = hakeeper.K8SIDRangeEnd
	}
	if state.NextId < expectedNextID {
		return false
	}
	for key, backupNextID := range backup.NextIDByKey {
		if state.NextIDByKey[key] < backupNextID {
			return false
		}
	}
	return true
}

func writeRestoredTag(ctx context.Context, fs fileservice.FileService) error {
	return fs.Write(ctx, fileservice.IOVector{
		FilePath: restoredTagFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   1,
				Data:   []byte{1},
			},
		},
	})
}
