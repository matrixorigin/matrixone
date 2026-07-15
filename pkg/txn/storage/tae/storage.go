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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	rpc2 "github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
)

type logtailServer interface {
	Start() error
	Close() error
}

type taeHandle interface {
	rpchandle.Handler
	GetDB() *db.DB
}

type taeStorageDependencies struct {
	newTAEHandle func(
		context.Context,
		string,
		client.QueryClient,
		*options.Options,
	) (taeHandle, error)
	newLogtailServer func(
		context.Context,
		*db.DB,
		string,
		*options.LogtailServerCfg,
		runtime.Runtime,
	) (*service.LogtailServer, error)
}

type taeStorage struct {
	shard         metadata.TNShard
	taeHandler    rpchandle.Handler
	logtailServer logtailServer
}

var _ storage.TxnStorage = (*taeStorage)(nil)

func NewTAEStorage(
	ctx context.Context,
	dataDir string,
	opt *options.Options,
	shard metadata.TNShard,
	rt runtime.Runtime,
	logtailServerAddr string,
	logtailServerCfg *options.LogtailServerCfg,
	txnServer rpc2.TxnServer,
	client client.QueryClient,
) (storage.TxnStorage, error) {
	return newTAEStorage(
		ctx,
		dataDir,
		opt,
		shard,
		rt,
		logtailServerAddr,
		logtailServerCfg,
		txnServer,
		client,
		defaultTAEStorageDependencies,
	)
}

func newTAEStorage(
	ctx context.Context,
	dataDir string,
	opt *options.Options,
	shard metadata.TNShard,
	rt runtime.Runtime,
	logtailServerAddr string,
	logtailServerCfg *options.LogtailServerCfg,
	txnServer rpc2.TxnServer,
	client client.QueryClient,
	deps taeStorageDependencies,
) (storage.TxnStorage, error) {
	if rt.ServiceUUID() != opt.SID {
		panic(fmt.Sprintf("service uuid mismatch, %s != %s", rt.ServiceUUID(), opt.SID))
	}
	taeHandler, err := deps.newTAEHandle(ctx, dataDir, client, opt)
	if err != nil {
		return nil, err
	}
	tae := taeHandler.GetDB()
	tae.TxnServer = txnServer
	server, err := deps.newLogtailServer(ctx, tae, logtailServerAddr, logtailServerCfg, rt)
	if err != nil {
		return nil, errors.Join(err, taeHandler.HandleClose(ctx))
	}

	ss, ok := rt.GetGlobalVariables(runtime.StatusServer)
	if ok {
		ss.(*status.Server).SetLogtailServer(server)
	}

	return &taeStorage{
		shard:         shard,
		taeHandler:    taeHandler,
		logtailServer: server,
	}, nil
}

var defaultTAEStorageDependencies = taeStorageDependencies{
	newTAEHandle:     openTAEHandle,
	newLogtailServer: newTAELogtailServer,
}

func openTAEHandle(
	ctx context.Context,
	dataDir string,
	client client.QueryClient,
	opt *options.Options,
) (taeHandle, error) {
	return rpc.NewTAEHandleWithError(ctx, dataDir, client, opt)
}

func newTAELogtailServer(
	ctx context.Context,
	tae *db.DB,
	address string,
	cfg *options.LogtailServerCfg,
	rt runtime.Runtime,
) (*service.LogtailServer, error) {
	logtailer := logtail.NewLogtailer(ctx, tae, tae.LogtailMgr, tae.Catalog)
	return service.NewLogtailServer(address, cfg, logtailer, rt, nil)
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
func (s *taeStorage) Commit(
	ctx context.Context,
	txnMeta txn.TxnMeta,
	response *txn.TxnResponse,
	commitRequests *txn.TxnCommitRequest,
) (timestamp.Timestamp, error) {
	return s.taeHandler.HandleCommit(ctx, txnMeta, response, commitRequests)
}

// Committing implements storage.TxnTAEStorage
func (s *taeStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.taeHandler.HandleCommitting(ctx, txnMeta)
}

// Destroy implements storage.TxnTAEStorage
func (s *taeStorage) Destroy(ctx context.Context) error {
	return errors.Join(s.Close(ctx), s.taeHandler.HandleDestroy(ctx))
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
