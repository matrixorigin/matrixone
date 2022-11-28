// Copyright 2022 Matrix Origin
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

package main

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"go.uber.org/zap"
)

const (
	localClockBackend = "LOCAL"
	hlcClockBackend   = "HLC"
)

var (
	supportTxnClockBackends = map[string]struct{}{
		localClockBackend: {},
		hlcClockBackend:   {},
	}
)

var (
	setupRuntimeOnce sync.Once
)

func setupProcessLevelRuntime(cfg *Config, stopper *stopper.Stopper) error {
	var e error
	setupRuntimeOnce.Do(func() {
		mpool.InitCap(int64(cfg.Limit.Memory))

		clock, err := getClock(cfg, stopper)
		if err != nil {
			e = err
			return
		}

		logger, err := getLogger(cfg)
		if err != nil {
			e = err
			return
		}

		logutil.GetGlobalLogger()
		r := runtime.NewRuntime(cfg.mustGetServiceType(),
			cfg.mustGetServiceUUID(),
			logger,
			runtime.WithClock(clock))
		runtime.SetupProcessLevelRuntime(r)
	})
	return e
}

func getClock(cfg *Config, stopper *stopper.Stopper) (clock.Clock, error) {
	var c clock.Clock
	switch cfg.Clock.Backend {
	case localClockBackend:
		c = newLocalClock(cfg, stopper)
	default:
		return nil, moerr.NewInternalError("not implment for %s", cfg.Clock.Backend)
	}
	c.SetNodeID(cfg.hashNodeID())

	// TODO: after unifying the use of Runtime, remove the GlobalClock
	clock.SetupDefaultClock(c)
	return c, nil
}

func getLogger(cfg *Config) (*zap.Logger, error) {
	// TODO: after unifying the use of Runtime, remove the GlobalLogger
	logutil.SetupMOLogger(&cfg.Log)
	logger := logutil.GetGlobalLogger()
	return logger, nil
}

func newLocalClock(cfg *Config, stopper *stopper.Stopper) clock.Clock {
	return clock.NewUnixNanoHLCClockWithStopper(stopper, cfg.Clock.MaxClockOffset.Duration)
}
