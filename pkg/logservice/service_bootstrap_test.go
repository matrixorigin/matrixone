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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
