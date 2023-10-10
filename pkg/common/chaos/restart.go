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

package chaos

import (
	"context"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

type restartTester struct {
	cfg     Config
	stopper *stopper.Stopper
}

func newRestartTester(cfg Config) tester {
	t := &restartTester{
		cfg: cfg,
	}
	return t
}

func (t *restartTester) start() error {
	t.initStopper()
	return t.stopper.RunTask(t.do)
}

func (t *restartTester) stop() error {
	t.stopper.Stop()
	t.stopper = nil
	return nil
}

func (t *restartTester) name() string {
	return "restart-tester"
}

func (t *restartTester) initStopper() {
	t.stopper = stopper.NewStopper("chaos-restart-tester")
}

func (t *restartTester) do(ctx context.Context) {
	if t.cfg.Restart.KillInterval.Duration == 0 {
		return
	}

	timer := time.NewTimer(t.cfg.Restart.KillInterval.Duration)
	action := 0
	target := -1
	actionFuncs := []func(){
		func() {
			target = t.cfg.Restart.Targets[int(rand.Int31n(int32(len(t.cfg.Restart.Targets))))]
			if err := t.cfg.Restart.KillFunc(target); err != nil {
				panic(err)
			}
			timer.Reset(t.cfg.Restart.RestartInterval.Duration)
		},
		func() {
			if err := t.cfg.Restart.RestartFunc(target); err != nil {
				panic(err)
			}
			timer.Reset(t.cfg.Restart.KillInterval.Duration)
		},
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			actionFuncs[action%len(actionFuncs)]()
			action++
		}
	}
}
