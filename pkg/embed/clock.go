package embed

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

const (
	localClockBackend = "LOCAL"
	hlcClockBackend   = "HLC"
)

func newClock(
	cfg ServiceConfig,
	stopper *stopper.Stopper,
) (clock.Clock, error) {
	var c clock.Clock
	switch cfg.Clock.Backend {
	case localClockBackend:
		c = newLocalClock(cfg, stopper)
	default:
		return nil, moerr.NewInternalError(context.Background(), "not implement for %s", cfg.Clock.Backend)
	}
	c.SetNodeID(cfg.hashNodeID())
	return c, nil
}

func newLocalClock(
	cfg ServiceConfig,
	stopper *stopper.Stopper,
) clock.Clock {
	return clock.NewUnixNanoHLCClockWithStopper(
		stopper,
		cfg.Clock.MaxClockOffset.Duration,
	)
}
