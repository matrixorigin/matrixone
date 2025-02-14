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

package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func Test_RestartFlusher(t *testing.T) {
	var cfg FlushCfg
	cfg.ForceFlushTimeout = time.Millisecond * 7
	cfg.ForceFlushCheckInterval = time.Millisecond * 9
	cfg.FlushInterval = time.Millisecond * 11
	cfg.CronPeriod = time.Millisecond * 2
	f := NewFlusher(
		nil, nil, nil, nil, false,
		WithFlusherInterval(cfg.FlushInterval),
		WithFlusherCronPeriod(cfg.CronPeriod),
		WithFlusherForceTimeout(cfg.ForceFlushTimeout),
		WithFlusherForceCheckInterval(cfg.ForceFlushCheckInterval),
	)

	fCfg := f.GetCfg()
	assert.Equal(t, cfg, fCfg)
	assert.False(t, f.IsNoop())

	f.Stop()
	assert.True(t, f.IsNoop())

	ctx := context.Background()
	var ts types.TS

	assert.Equal(t, ErrFlusherStopped, f.FlushTable(ctx, 0, 0, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlush(ctx, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlushWithInterval(ctx, ts, time.Millisecond))
	f.ChangeForceCheckInterval(time.Millisecond)
	f.ChangeForceFlushTimeout(time.Millisecond)

	f.Restart(WithFlusherCfg(cfg))
	assert.False(t, f.IsNoop())
	fCfg = f.GetCfg()
	assert.Equal(t, cfg, fCfg)
}
