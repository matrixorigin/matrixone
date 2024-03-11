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

package taestorage

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
)

type taeStorage struct {
	shard         metadata.TNShard
	taeHandler    rpchandle.Handler
	logtailServer *service.LogtailServer
}

var _ storage.TxnStorage = (*taeStorage)(nil)

func NewTAEStorage(
	ctx context.Context,
	dataDir string,
	shard metadata.TNShard,
	factory logservice.ClientFactory,
	fs fileservice.FileService,
	rt runtime.Runtime,
	ckpCfg *options.CheckpointCfg,
	gcCfg *options.GCCfg,
	logtailServerAddr string,
	logtailServerCfg *options.LogtailServerCfg,
	incrementalDedup bool,
	maxMessageSize uint64,
	taskServiceGetter taskservice.Getter,
) (storage.TxnStorage, error) {
	opt := &options.Options{
		Clock:             rt.Clock(),
		Fs:                fs,
		Lc:                logservicedriver.LogServiceClientFactory(factory),
		Shard:             shard,
		CheckpointCfg:     ckpCfg,
		GCCfg:             gcCfg,
		LogStoreT:         options.LogstoreLogservice,
		IncrementalDedup:  incrementalDedup,
		Ctx:               ctx,
		MaxMessageSize:    maxMessageSize,
		TaskServiceGetter: taskServiceGetter,
	}

	taeHandler := rpc.NewTAEHandle(ctx, dataDir, opt)
	tae := taeHandler.GetDB()
	logtailer := logtail.NewLogtailer(ctx, tae.BGCheckpointRunner, tae.LogtailMgr, tae.Catalog)
	server, err := service.NewLogtailServer(logtailServerAddr, logtailServerCfg, logtailer, rt)
	if err != nil {
		return nil, err
	}

	ss, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.StatusServer)
	if ok {
		ss.(*status.Server).SetLogtailServer(server)
	}

	return &taeStorage{
		shard:         shard,
		taeHandler:    taeHandler,
		logtailServer: server,
	}, nil
}

// Start starts logtail push service.
func (s *taeStorage) Start() error {
	return s.logtailServer.Start()
}

// Close implements storage.TxnTAEStorage
func (s *taeStorage) Close(ctx context.Context) error {
	return errors.Join(s.logtailServer.Close(), s.taeHandler.HandleClose(ctx))
}

// Commit implements storage.TxnTAEStorage
func (s *taeStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	return s.taeHandler.HandleCommit(ctx, txnMeta)
}

// Committing implements storage.TxnTAEStorage
func (s *taeStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.taeHandler.HandleCommitting(ctx, txnMeta)
}

// Destroy implements storage.TxnTAEStorage
func (s *taeStorage) Destroy(ctx context.Context) error {
	return s.taeHandler.HandleDestroy(ctx)
}

// Prepare implements storage.TxnTAEStorage
func (s *taeStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	return s.taeHandler.HandlePrepare(ctx, txnMeta)
}

// Rollback implements storage.TxnTAEStorage
func (s *taeStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.taeHandler.HandleRollback(ctx, txnMeta)
}

// StartRecovery implements storage.TxnTAEStorage
func (s *taeStorage) StartRecovery(ctx context.Context, ch chan txn.TxnMeta) {
	s.taeHandler.HandleStartRecovery(ctx, ch)
}
