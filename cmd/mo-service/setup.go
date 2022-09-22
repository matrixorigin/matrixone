package main

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

const (
	localClockBackend = "LOCAL"
	hlcClockBackend   = "HLC"
)

var (
	defaultMaxClockOffset = time.Millisecond * 500

	supportTxnClockBackends = map[string]struct{}{
		localClockBackend: {},
		hlcClockBackend:   {},
	}
)

var (
	setupLoggerOnce sync.Once
	setupClockOnce  sync.Once
)

func setupGlobalComponents(cfg *Config, stopper *stopper.Stopper) error {
	if err := setupClock(cfg, stopper); err != nil {
		return err
	}

	setupLogger(cfg)
	return nil
}

func setupClock(cfg *Config, stopper *stopper.Stopper) error {
	var err error
	setupClockOnce.Do(func() {
		var defaultClock clock.Clock
		switch cfg.Clock.Backend {
		case localClockBackend:
			defaultClock = newLocalClock(cfg, stopper)
		default:
			err = moerr.NewInternalError("not implment for %s", cfg.Clock.Backend)
			return
		}
		clock.SetupDefaultClock(defaultClock)
	})
	return err
}

func setupLogger(cfg *Config) {
	setupLoggerOnce.Do(func() {
		logutil.SetupMOLogger(&cfg.Log)
	})
}

func newLocalClock(cfg *Config, stopper *stopper.Stopper) clock.Clock {
	return clock.NewUnixNanoHLCClockWithStopper(stopper, cfg.Clock.MaxClockOffset.Duration)
}
