// Copyright 2023 Matrix Origin
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

package goroutine

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"go.uber.org/zap"
)

var (
	creatorGroupTop = 5
	methodGroupTop  = 5
	leakOnce        sync.Once
)

func (c *Config) adjust() {
	if c.CheckDuration.Duration == 0 {
		c.CheckDuration.Duration = time.Hour
	}
	if c.SuspectLeakCount == 0 {
		c.SuspectLeakCount = 10000
	}
}

func StartLeakCheck(
	stopper *stopper.Stopper,
	cfg Config) {
	if !cfg.EnableLeakCheck {
		return
	}

	leakOnce.Do(func() {
		stopper.RunTask(
			func(ctx context.Context) {
				startLeakCheckTask(ctx, cfg)
			})
	})
}

func startLeakCheckTask(
	ctx context.Context,
	cfg Config) {
	cfg.adjust()
	ticker := time.NewTicker(time.Duration(cfg.CheckDuration.Duration))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doLeakCheck(cfg)
		}
	}
}

func doLeakCheck(cfg Config) {
	values := GetAnalyzer().ParseSystem()
	if len(values) < cfg.SuspectLeakCount {
		return
	}

	getLogger().Warn("goroutine suspect leak detected",
		zap.Int("total", len(values)))
	res := GetAnalyzer().GroupAnalyze(values)
	res.read(
		func(
			group int,
			g Goroutine,
			count int) {
			if group < creatorGroupTop {
				m, f := g.CreateBy()
				getLogger().Warn("goroutine suspect leak group by creator",
					zap.Int("group", group),
					zap.String("create-method", m),
					zap.String("create-file", f),
					zap.Int("group-count", count))
			}
		},
		func(
			group int,
			methodGroup int,
			g Goroutine,
			count int) {
			if group < creatorGroupTop &&
				methodGroup < methodGroupTop {
				getLogger().Warn("goroutine suspect leak group by current method",
					zap.Int("group", group),
					zap.String("goroutine state", g.rawState),
					zap.String("last-method", g.methods[0]),
					zap.String("last-file", g.files[0]),
					zap.Int("count", count))
			}
		})

}
