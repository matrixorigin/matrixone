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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"go.uber.org/zap"
)

const (
	memStorageBackend   = "MEM"
	memKVStorageBackend = "MEMKV"
	taeStorageBackend   = "TAE"

	s3FileServiceName    = "S3"
	localFileServiceName = "LOCAL"
	etlFileServiceName   = "ETL"
)

var (
	supportTxnStorageBackends = map[string]struct{}{
		memStorageBackend: {},
		taeStorageBackend: {},
	}
)

func (s *store) createTxnStorage(shard metadata.DNShard) (storage.TxnStorage, error) {
	factory := s.createLogServiceClientFactroy(shard)
	closeLogClientFn := func(logClient logservice.Client) {
		if err := logClient.Close(); err != nil {
			s.logger.Error("close log client failed",
				zap.Error(err))
		}
	}

	switch s.cfg.Txn.Storage.Backend {
	case memStorageBackend:
		logClient, err := factory()
		if err != nil {
			return nil, err
		}
		ts, err := s.newMemTxnStorage(shard, logClient, s.hakeeperClient)
		if err != nil {
			closeLogClientFn(logClient)
			return nil, err
		}
		return ts, nil

	case memKVStorageBackend:
		logClient, err := factory()
		if err != nil {
			return nil, err
		}
		return s.newMemKVStorage(shard, logClient)

	case taeStorageBackend:
		ts, err := s.newTAEStorage(shard, factory)
		if err != nil {
			return nil, err
		}
		return ts, nil
	default:
		return nil, moerr.NewInternalError("not implment for %s", s.cfg.Txn.Storage.Backend)
	}
}

func (s *store) createLogServiceClient(shard metadata.DNShard) (logservice.Client, error) {
	if s.options.logServiceClientFactory != nil {
		return s.options.logServiceClientFactory(shard)
	}
	return s.newLogServiceClient(shard)
}

func (s *store) createLogServiceClientFactroy(shard metadata.DNShard) logservice.ClientFactory {
	return func() (logservice.Client, error) {
		return s.createLogServiceClient(shard)
	}
}

func (s *store) newLogServiceClient(shard metadata.DNShard) (logservice.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.LogService.ConnectTimeout.Duration)
	defer cancel()
	return logservice.NewClient(ctx, logservice.ClientConfig{
		ReadOnly:         false,
		LogShardID:       shard.LogShardID,
		DNReplicaID:      shard.ReplicaID,
		ServiceAddresses: s.cfg.HAKeeper.ClientConfig.ServiceAddresses,
	})
}

func (s *store) newMemTxnStorage(
	shard metadata.DNShard,
	logClient logservice.Client,
	hakeeper logservice.DNHAKeeperClient,
) (storage.TxnStorage, error) {
	// should it be no fixed or a certain size?
	mp, err := mpool.NewMPool("mem_txn_storge", 0, mpool.NoFixed)
	if err != nil {
		return nil, err
	}
	return memorystorage.NewMemoryStorage(
		mp,
		memorystorage.SnapshotIsolation,
		s.clock,
		memoryengine.NewHakeeperIDGenerator(hakeeper),
	)
}

func (s *store) newMemKVStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return mem.NewKVTxnStorage(0, logClient, s.clock), nil
}

func (s *store) newTAEStorage(shard metadata.DNShard, factory logservice.ClientFactory) (storage.TxnStorage, error) {
	return taestorage.NewTAEStorage(shard, factory, s.fileService, s.clock)
}
