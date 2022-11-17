// Copyright 2021 Matrix Origin
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

package logservicedriver

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

const (
	ReplayReadSize = common.M * 2
)

func RetryWithTimeout(timeoutDuration time.Duration, fn func() (shouldReturn bool)) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return ErrRetryTimeOut
		default:
			if fn() {
				return nil
			}
		}
	}
}

type LogServiceDriver struct {
	clientPool *clientpool
	config     *Config
	appendable *driverAppender
	*driverInfo
	*readCache

	closeCtx        context.Context
	closeCancel     context.CancelFunc
	preAppendLoop   sm.Queue
	appendQueue     chan any
	appendedQueue   chan any
	appendedLoop    *sm.Loop
	postAppendQueue chan any
	postAppendLoop  *sm.Loop

	truncateQueue sm.Queue

	flushtimes  int
	appendtimes int

	readDuration time.Duration
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	clientpoolConfig := &clientConfig{
		cancelDuration:        cfg.NewClientDuration,
		recordSize:            cfg.NewRecordSize,
		clientFactory:         cfg.ClientFactory,
		GetClientRetryTimeOut: cfg.GetClientRetryTimeOut,
		retryDuration:         cfg.RetryTimeout,
	}
	d := &LogServiceDriver{
		clientPool:      newClientPool(cfg.ClientPoolMaxSize, cfg.ClientPoolMaxSize, clientpoolConfig),
		config:          cfg,
		appendable:      newDriverAppender(),
		driverInfo:      newDriverInfo(),
		readCache:       newReadCache(),
		appendQueue:     make(chan any, 10000),
		appendedQueue:   make(chan any, 10000),
		postAppendQueue: make(chan any, 10000),
	}
	d.closeCtx, d.closeCancel = context.WithCancel(context.Background())
	d.preAppendLoop = sm.NewSafeQueue(10000, 10000, d.onPreAppend)
	d.preAppendLoop.Start()
	d.appendedLoop = sm.NewLoop(d.appendedQueue, d.postAppendQueue, d.onAppendedQueue, 10000)
	d.appendedLoop.Start()
	d.postAppendLoop = sm.NewLoop(d.postAppendQueue, nil, d.onPostAppendQueue, 10000)
	d.postAppendLoop.Start()
	d.truncateQueue = sm.NewSafeQueue(10000, 10000, d.onTruncate)
	d.truncateQueue.Start()
	return d
}

func (d *LogServiceDriver) Close() error {
	logutil.Infof("append%d,flush%d", d.appendtimes, d.flushtimes)
	d.clientPool.Close()
	d.closeCancel()
	d.preAppendLoop.Stop()
	d.appendedLoop.Stop()
	d.postAppendLoop.Stop()
	d.truncateQueue.Stop()
	close(d.appendQueue)
	close(d.appendedQueue)
	close(d.postAppendQueue)
	return nil
}

func (d *LogServiceDriver) Replay(h driver.ApplyHandle) error {
	d.PreReplay()
	r := newReplayer(h, ReplayReadSize, d)
	r.replay()
	d.onReplay(r)
	r.d.resetReadCache()
	d.PostReplay()
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("backend", "logservice"),
		common.AnyField("apply cost", r.applyDuration),
		common.AnyField("read cost", d.readDuration))

	return nil
}
