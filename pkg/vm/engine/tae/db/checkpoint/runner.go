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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
)

type Runner interface {
	Start() error
	Stop() error
}

type Option func(*runner)

func WithCollectInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.collectInterval = interval
	}
}

type runner struct {
	options struct {
		collectInterval time.Duration
	}

	source logtail.Collector

	stopper *stopper.Stopper

	onceStart sync.Once
	onceStop  sync.Once
}

func NewRunner(source logtail.Collector, opts ...Option) *runner {
	r := &runner{
		source: source,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()
	r.stopper = stopper.NewStopper("CheckpointRunner")
	return r
}

func (r *runner) fillDefaults() {
	if r.options.collectInterval <= 0 {
		// TODO: define default value
		r.options.collectInterval = time.Second * 5
	}
}

func (r *runner) cronCollect(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(r.options.collectInterval, func() {
		r.source.Run()
		entry := r.source.GetAndRefreshMerged()
		if entry.IsEmpty() {
			logutil.Info("No dirty block found")
			return
		}
		logutil.Infof(entry.String())
	}, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}

func (r *runner) Start() (err error) {
	r.onceStart.Do(func() {
		if err = r.stopper.RunNamedTask("dirty-collector-job", r.cronCollect); err != nil {
			return
		}
	})
	return
}

func (r *runner) Stop() (err error) {
	r.onceStop.Do(func() {
		r.stopper.Stop()
	})
	return
}
