// Copyright 2021-2024 Matrix Origin
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
		return nil, moerr.NewInternalErrorf(context.Background(), "not implement for %s", cfg.Clock.Backend)
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
