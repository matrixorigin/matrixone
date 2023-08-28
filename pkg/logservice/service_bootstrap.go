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
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

func (s *Service) BootstrapHAKeeper(ctx context.Context, cfg Config) error {
	replicaID, bootstrapping := cfg.Bootstrapping()
	if !bootstrapping {
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

	var nextID uint64
	var nextIDByKey map[string]uint64
	backup, err := s.getBackupData(ctx)
	if err != nil {
		return err
	}
	if backup != nil {
		nextID = backup.NextID
		nextIDByKey = backup.NextIDByKey
	}
	for i := 0; i < checkBootstrapCycles; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := s.store.setInitialClusterInfo(numOfLogShards,
			numOfTNShards, numOfLogReplicas, nextID, nextIDByKey); err != nil {
			s.runtime.SubLogger(runtime.SystemInit).Error("failed to set initial cluster info", zap.Error(err))
			if err == dragonboat.ErrShardNotFound {
				return nil
			}
			time.Sleep(time.Second)
			continue
		}
		s.runtime.SubLogger(runtime.SystemInit).Info("initial cluster info set")
		break
	}
	return nil
}

func (s *Service) getBackupData(ctx context.Context) (*pb.BackupData, error) {
	fs := s.fileService
	filePath := s.cfg.BootstrapConfig.Restore.FilePath
	if filePath == "" {
		return nil, nil
	}

	st, err := fs.StatFile(ctx, filePath)
	if err != nil {
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

	var data pb.BackupData
	if err := data.Unmarshal(ioVec.Entries[0].Data); err != nil {
		return nil, err
	}
	return &data, nil
}
