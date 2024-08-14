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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
)

type truncationConfig struct {
	interval time.Duration
}

type truncation struct {
	common
	client *logClient
	// cfg is truncation module configuration.
	cfg truncationConfig
	// lastTruncateLsn is the Lsn which is last truncated.
	lastTruncateLsn uint64
	// syncedLSN the LSN that has been synced.
	syncedLSN *atomic.Uint64
}

type truncationOption func(*truncation)

func withTruncateInterval(v time.Duration) truncationOption {
	return func(t *truncation) {
		t.cfg.interval = v
	}
}

func newTruncation(
	common common, syncedLSN *atomic.Uint64, opts ...truncationOption,
) Worker {
	t := &truncation{
		common:    common,
		client:    newLogClient(common, logShardID),
		syncedLSN: syncedLSN,
	}
	if syncedLSN == nil {
		t.log.Error("syncedLSN is nil")
		return nil
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *truncation) truncate(ctx context.Context) error {
	lsn := t.syncedLSN.Load()
	if lsn == 0 {
		return nil
	}
	if t.lastTruncateLsn == lsn {
		return nil
	}
	err := t.client.truncate(ctx, lsn)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
			t.lastTruncateLsn = lsn
			return nil
		}
		return err
	}
	t.lastTruncateLsn = lsn
	return nil
}

func (t *truncation) Start(ctx context.Context) {
	timer := time.NewTimer(t.cfg.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
			if err := t.truncate(ctx); err != nil {
				t.log.Error("failed to truncate log", zap.Error(err))
			}
			timer.Reset(t.cfg.interval)
		}
	}
}

func (t *truncation) Close() {
	t.client.close()
}
