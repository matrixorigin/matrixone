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

package dnservice

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
)

const (
	memStorageBackend = "MEM"
	taeStorageBackend = "TAE"

	localClockBackend = "LOCAL"
	hlcClockBackend   = "HLC"

	memFileServiceBackend   = "MEM"
	diskFileServiceBackend  = "DISK"
	s3FileServiceBackend    = "S3"
	minioFileServiceBackend = "MINIO"
)

var (
	supportTxnStorageBackends = map[string]struct{}{
		memStorageBackend: {},
		taeStorageBackend: {},
	}

	supportTxnClockBackends = map[string]struct{}{
		localClockBackend: {},
		hlcClockBackend:   {},
	}

	supportFileServiceBackends = map[string]struct{}{
		memFileServiceBackend:   {},
		diskFileServiceBackend:  {},
		s3FileServiceBackend:    {},
		minioFileServiceBackend: {},
	}
)

func (s *store) createClock() (clock.Clock, error) {
	switch s.cfg.Txn.Clock.Backend {
	case localClockBackend:
		return s.newLocalClock(), nil
	default:
		return nil, fmt.Errorf("not implment for %s", s.cfg.Txn.Clock.Backend)
	}
}

func (s *store) createTxnStorage(shard metadata.DNShard) (storage.TxnStorage, error) {
	logClient, err := s.createLogServiceClient(shard)
	if err != nil {
		return nil, err
	}

	switch s.cfg.Txn.Storage.Backend {
	case memStorageBackend:
		return s.newMemTxnStorage(shard, logClient)
	default:
		return nil, fmt.Errorf("not implment for %s", s.cfg.Txn.Storage.Backend)
	}
}

func (s *store) createLogServiceClient(shard metadata.DNShard) (logservice.Client, error) {
	if s.options.logServiceClientFactory != nil {
		return s.options.logServiceClientFactory(shard)
	}
	return s.newLogServiceClient(shard)
}

func (s *store) createFileService() (fileservice.FileService, error) {
	switch s.cfg.FileService.Backend {
	case memFileServiceBackend:
		return s.newMemFileService()
	case diskFileServiceBackend:
		return s.newDiskFileService()
	case minioFileServiceBackend:
		return s.newMinioFileService()
	case s3FileServiceBackend:
		return s.newS3FileService()
	default:
		return nil, fmt.Errorf("not implment for %s", s.cfg.FileService.Backend)
	}
}

func (s *store) newLogServiceClient(shard metadata.DNShard) (logservice.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.LogService.ConnectTimeout.Duration)
	defer cancel()
	return logservice.NewClient(ctx, logservice.ClientConfig{
		ReadOnly:    false,
		LogShardID:  shard.LogShardID,
		DNReplicaID: shard.ReplicaID,
	})
}

func (s *store) newLocalClock() clock.Clock {
	return clock.NewUnixNanoHLCClockWithStopper(s.stopper, s.cfg.Txn.Clock.MaxClockOffset.Duration)
}

func (s *store) newMemTxnStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return mem.NewKVTxnStorage(0, logClient, s.clock), nil
}

func (s *store) newMemFileService() (fileservice.FileService, error) {
	return fileservice.NewMemoryFS()
}

func (s *store) newDiskFileService() (fileservice.FileService, error) {
	return fileservice.NewLocalFS(s.cfg.getDataDir())
}

func (s *store) newMinioFileService() (fileservice.FileService, error) {
	return fileservice.NewS3FSMinio(s.cfg.FileService.S3)
}

func (s *store) newS3FileService() (fileservice.FileService, error) {
	return fileservice.NewS3FS(s.cfg.FileService.S3.Endpoint,
		s.cfg.FileService.S3.Bucket,
		s.cfg.FileService.S3.KeyPrefix)
}
