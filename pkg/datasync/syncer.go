// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"go.uber.org/zap"
)

const (
	defaultDataSize    = 1024 * 2
	truncateInterval   = time.Second * 10
	logShardID         = 3
	upstreamLogShardID = 1
	producerQueueSize  = 10240
)

var (
	binaryEnc = binary.BigEndian
)

type syncer struct {
	common
	ctx context.Context

	lifecycleMu   sync.Mutex
	cancel        context.CancelFunc
	startDone     chan struct{}
	startDoneOnce sync.Once
	admitted      bool
	closed        atomic.Bool
	closeOnce     sync.Once
	appendWG      sync.WaitGroup

	// producer worker dequeues the items from the data queue and
	// write the entries which is storage related to the datasync
	// shard.
	producer Producer

	// consumer worker reads entries from datasync shard and consume
	// the entries to sync data to the other storage.
	consumer Worker

	// truncation worker truncates the entries according to the synced
	// LSN in the datasync shard.
	truncation Worker

	// the writeLsn is the latest written LSN of the standby shard.
	writeLsn atomic.Uint64
	// the syncedLsn is the LSN of log record which has been synced to
	// standby storage.
	syncedLsn atomic.Uint64
}

// NewDataSync creates a new syncer instance.
func NewDataSync(
	sid string,
	stopper *stopper.Stopper,
	rt moruntime.Runtime,
	cfg logservice.HAKeeperClientConfig,
	fs fileservice.FileService,
) (logservice.DataSync, error) {
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	ss := &syncer{
		ctx:       ownerCtx,
		cancel:    ownerCancel,
		startDone: make(chan struct{}),
		common: common{
			stopper:        stopper,
			log:            rt.Logger().With(zap.String("module", "datasync")),
			sid:            sid,
			haKeeperConfig: cfg,
			pool:           newDataPool(defaultDataSize),
			rt:             rt,
			rpcConfig:      rpc.Config{},
			shardReplicaID: &sync.Map{},
		},
	}
	ss.producer = newProducer(
		ss.common,
		producerQueueSize,
		&ss.writeLsn,
		withRetryTimes(200),
	)
	ss.consumer = newConsumer(ss.common, fs, &ss.writeLsn, &ss.syncedLsn)
	if ss.consumer == nil {
		ss.finishStart()
		_ = ss.Close()
		panic("cannot create consumer, please set standby file-service")
	}
	ss.truncation = newTruncation(
		ss.common,
		&ss.syncedLsn,
		withTruncateInterval(truncateInterval),
	)
	ss.lifecycleMu.Lock()
	ss.admitted = true
	ss.lifecycleMu.Unlock()
	if err := ss.stopper.RunNamedTask("data-syncer", ss.start); err != nil {
		ss.finishStart()
		return nil, err
	}
	return ss, nil
}

// Append implements the logservice.DataSync interface.
func (s *syncer) Append(ctx context.Context, lsn uint64, data []byte) {
	s.lifecycleMu.Lock()
	if s.closed.Load() {
		s.lifecycleMu.Unlock()
		return
	}
	s.appendWG.Add(1)
	ownerCtx := s.ctx
	s.lifecycleMu.Unlock()
	defer s.appendWG.Done()

	// Acquire data intance from pool.
	w := s.pool.acquire(len(data))

	// copy the data.
	_ = copy(w.data, data)

	// update the upstream Lsn in the wrapped data.
	w.upstreamLsn = lsn

	// send data to the queue.
	queueCtx, cancel := context.WithCancel(ctx)
	stopOwner := context.AfterFunc(ownerCtx, cancel)
	s.producer.Enqueue(queueCtx, w)
	stopOwner()
	cancel()
}

// NotifyReplicaID implements the logservice.DataSync interface.
func (s *syncer) NotifyReplicaID(shardID uint64, replicaID uint64, typ logservice.ChangeType) {
	if typ == logservice.AddReplica {
		s.common.shardReplicaID.Store(shardID, replicaID)
	} else {
		s.common.shardReplicaID.Delete(shardID)
	}
}

// Close implements the logservice.DataSync interface.
func (s *syncer) Close() error {
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.closed.Store(true)
		cancel := s.cancel
		admitted := s.admitted
		s.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
		}
		s.appendWG.Wait()
		if admitted {
			<-s.startDone
		}
		if s.producer != nil {
			s.producer.Close()
		}
		if s.truncation != nil {
			s.truncation.Close()
		}
		if s.consumer != nil {
			s.consumer.Close()
		}
	})
	return nil
}

// start starts the goroutines in the syncer module:
func (s *syncer) start(ctx context.Context) {
	s.lifecycleMu.Lock()
	if s.closed.Load() {
		s.lifecycleMu.Unlock()
		s.finishStart()
		return
	}
	ownerCtx := s.ctx
	s.lifecycleMu.Unlock()
	defer s.finishStart()
	workerCtx, cancelWorker := context.WithCancel(ctx)
	stopOwner := context.AfterFunc(ownerCtx, cancelWorker)
	defer stopOwner()
	defer cancelWorker()

	var workers sync.WaitGroup
	startWorker := func(worker Worker) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			worker.Start(workerCtx)
		}()
	}
	startWorker(s.truncation)
	startWorker(s.consumer)
	startWorker(s.producer)
	<-workerCtx.Done()
	workers.Wait()
}

func (s *syncer) finishStart() {
	s.startDoneOnce.Do(func() {
		close(s.startDone)
	})
}
