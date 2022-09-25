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
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	txnstorage "github.com/matrixorigin/matrixone/pkg/txn/storage/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
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
	logClient, err := s.createLogServiceClient(shard)
	if err != nil {
		return nil, err
	}
	closeLogClient := func() {
		if err := logClient.Close(); err != nil {
			s.logger.Error("close log client failed",
				zap.Error(err))
		}
	}

	switch s.cfg.Txn.Storage.Backend {
	case memStorageBackend:
		ts, err := s.newMemTxnStorage(shard, logClient)
		if err != nil {
			closeLogClient()
			return nil, err
		}
		return ts, nil

	case memKVStorageBackend:
		return s.newMemKVStorage(shard, logClient)

	case taeStorageBackend:
		ts, err := s.newTAEStorage(shard, logClient)
		if err != nil {
			closeLogClient()
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

func (s *store) newMemTxnStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	return txnstorage.NewMemoryStorage(mheap.New(gm), txnstorage.SnapshotIsolation, s.clock)
}

func (s *store) newMemKVStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return mem.NewKVTxnStorage(0, logClient, s.clock), nil
}

func (s *store) newTAEStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return taestorage.New(shard, logClient, s.fileService, s.clock)
}
