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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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

type taeMetricTask struct {
	shard         metadata.TNShard
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
	logtailServerAddr string,
	logtailServerCfg *options.LogtailServerCfg,
	incrementalDedup bool,
	maxMessageSize uint64,
) (storage.TxnStorage, error) {
	opt := &options.Options{
		Clock:            rt.Clock(),
		Fs:               fs,
		Lc:               logservicedriver.LogServiceClientFactory(factory),
		Shard:            shard,
		CheckpointCfg:    ckpCfg,
		LogStoreT:        options.LogstoreLogservice,
		IncrementalDedup: incrementalDedup,
		Ctx:              ctx,
		MaxMessageSize:   maxMessageSize,
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

	s := &taeStorage{
		shard:         shard,
		taeHandler:    taeHandler,
		logtailServer: server,
	}

	newTaeMetricsTask(ctx, s)
	return s, nil
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

func newTaeMetricsTask(ctx context.Context, s *taeStorage) {
	t := new(taeMetricTask)
	t.shard = s.shard
	t.logtailServer = s.logtailServer
	t.launchTaeMetricsTask(ctx)
}

func (t *taeMetricTask) launchTaeMetricsTask(ctx context.Context) {
	go func() {
		logutil.Info("tae metrics task started")
		defer logutil.Info("tae metrics task exit")

		timer := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				t.mpoolAllocatorSubTask()
				t.queueSizeUpdateSubTask()
			}
		}
	}()
}

func (t *taeMetricTask) queueSizeUpdateSubTask() {
	// collect safe queue size
	for idx := 0; idx < int(sm.QueueNameMax); idx++ {
		if sm.SafeQueueRegister[idx].Queue != nil {
			v2.TxnTNSideQueueSizeGauges[sm.QueueNameType(idx)].
				Set(float64(sm.SafeQueueRegister[idx].Queue.Len()))
		}
	}

	// collect logtail notifier queue depth
	v2.LogTailNotifierQueueSizeGauge.Set(float64(t.logtailServer.NotifierDepth()))
}

func (t *taeMetricTask) mpoolAllocatorSubTask() {
	v2.MemTAEDefaultAllocatorGauge.Set(float64(common.DefaultAllocator.CurrNB()))
	v2.MemTAEDefaultHighWaterMarkGauge.Set(float64(common.DefaultAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMutableAllocatorGauge.Set(float64(common.MutMemAllocator.CurrNB()))
	v2.MemTAEMutableHighWaterMarkGauge.Set(float64(common.MutMemAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAESmallAllocatorGauge.Set(float64(common.SmallAllocator.CurrNB()))
	v2.MemTAESmallHighWaterMarkGauge.Set(float64(common.SmallAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEVectorPoolDefaultAllocatorGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().CurrNB()))
	v2.MemTAEVectorPoolDefaultHighWaterMarkGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().Stats().HighWaterMark.Load()))

	v2.MemTAELogtailAllocatorGauge.Set(float64(common.LogtailAllocator.CurrNB()))
	v2.MemTAELogtailHighWaterMarkGauge.Set(float64(common.LogtailAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAECheckpointAllocatorGauge.Set(float64(common.CheckpointAllocator.CurrNB()))
	v2.MemTAECheckpointHighWaterMarkGauge.Set(float64(common.CheckpointAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMergeAllocatorGauge.Set(float64(common.MergeAllocator.CurrNB()))
	v2.MemTAEMergeHighWaterMarkGauge.Set(float64(common.MergeAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEWorkSpaceAllocatorGauge.Set(float64(common.WorkspaceAllocator.CurrNB()))
	v2.MemTAEWorkSpaceHighWaterMarkGauge.Set(float64(common.WorkspaceAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEDebugAllocatorGauge.Set(float64(common.DebugAllocator.CurrNB()))
	v2.MemTAEDebugHighWaterMarkGauge.Set(float64(common.DebugAllocator.Stats().HighWaterMark.Load()))

}
