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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

var (
	supportTxnStorageBackends = map[StorageType]struct{}{
		StorageMEMKV: {},
		StorageMEM:   {},
		StorageTAE:   {},
	}
)

func (s *store) createTxnStorage(ctx context.Context, shard metadata.DNShard) (storage.TxnStorage, error) {
	factory := s.createLogServiceClientFactroy(shard)
	closeLogClientFn := func(logClient logservice.Client) {
		if err := logClient.Close(); err != nil {
			s.rt.Logger().Error("close log client failed",
				zap.Error(err))
		}
	}

	switch s.cfg.Txn.Storage.Backend {
	case StorageMEM:
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

	case StorageMEMKV:
		logClient, err := factory()
		if err != nil {
			return nil, err
		}
		return s.newMemKVStorage(shard, logClient)

	case StorageTAE:
		ts, err := s.newTAEStorage(ctx, shard, factory)
		if err != nil {
			return nil, err
		}
		return ts, nil
	default:
		return nil, moerr.NewInternalError(ctx, "not implment for %s", s.cfg.Txn.Storage.Backend)
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
		MaxMessageSize:   int(s.cfg.RPC.MaxMessageSize),
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
		s.rt.Clock(),
		memoryengine.NewHakeeperIDGenerator(hakeeper),
	)
}

func (s *store) newMemKVStorage(shard metadata.DNShard, logClient logservice.Client) (storage.TxnStorage, error) {
	return mem.NewKVTxnStorage(0, logClient, s.rt.Clock()), nil
}

func (s *store) newTAEStorage(ctx context.Context, shard metadata.DNShard, factory logservice.ClientFactory) (storage.TxnStorage, error) {
	ckpcfg := &options.CheckpointCfg{
		MinCount:            s.cfg.Ckp.MinCount,
		ScanInterval:        s.cfg.Ckp.ScanInterval.Duration,
		FlushInterval:       s.cfg.Ckp.FlushInterval.Duration,
		IncrementalInterval: s.cfg.Ckp.IncrementalInterval.Duration,
		GlobalMinCount:      s.cfg.Ckp.GlobalMinCount,
	}
	logtailServerAddr := s.logtailServiceListenAddr()
	logtailServerCfg := &options.LogtailServerCfg{
		RpcMaxMessageSize:      int64(s.cfg.LogtailServer.RpcMaxMessageSize),
		RpcEnableChecksum:      s.cfg.LogtailServer.RpcEnableChecksum,
		LogtailCollectInterval: s.cfg.LogtailServer.LogtailCollectInterval.Duration,
		ResponseSendTimeout:    s.cfg.LogtailServer.LogtailResponseSendTimeout.Duration,
	}

	// use s3 as main fs
	fs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}

	return taestorage.NewTAEStorage(
		ctx,
		s.cfg.Txn.Storage.dataDir,
		shard,
		factory,
		fs,
		s.rt,
		ckpcfg,
		logtailServerAddr,
		logtailServerCfg,
		s.cfg.Txn.IncrementalDedup == "true",
		uint64(s.cfg.RPC.MaxMessageSize),
	)
}
