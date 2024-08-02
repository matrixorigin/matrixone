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

package dbutils

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type RuntimeOption func(*Runtime)

func WithRuntimeSmallPool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Small = vp
	}
}

func WithRuntimeTransientPool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Transient = vp
	}
}

func WithRuntimeObjectFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.Fs = fs
	}
}

func WithRuntimeLocalFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.LocalFs = fs
	}
}

func WithRuntimeTransferTable(tt *model.HashPageTable) RuntimeOption {
	return func(r *Runtime) {
		r.TransferTable = tt
	}
}

func WithRuntimeScheduler(s tasks.TaskScheduler) RuntimeOption {
	return func(r *Runtime) {
		r.Scheduler = s
	}
}

func WithRuntimeOptions(opts *options.Options) RuntimeOption {
	return func(r *Runtime) {
		r.Options = opts
	}
}

type Runtime struct {
	Now        func() types.TS
	VectorPool struct {
		Small     *containers.VectorPool
		Transient *containers.VectorPool
	}

	Fs      *objectio.ObjectFS
	LocalFs *objectio.ObjectFS

	TransferTable   *model.HashPageTable
	TransferDelsMap *model.TransDelsForBlks
	Scheduler       tasks.TaskScheduler

	Options *options.Options

	Logtail struct {
		CompactStats stats.Counter
	}
}

func NewRuntime(opts ...RuntimeOption) *Runtime {
	r := new(Runtime)
	r.TransferDelsMap = model.NewTransDelsForBlks()
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()
	return r
}

func (r *Runtime) fillDefaults() {
	if r.VectorPool.Small == nil {
		r.VectorPool.Small = MakeDefaultSmallPool("small-vector-pool")
	}
	if r.VectorPool.Transient == nil {
		r.VectorPool.Transient = MakeDefaultTransientPool("trasient-vector-pool")
	}
}

func (r *Runtime) SID() string {
	if r == nil {
		return ""
	}
	return r.Options.SID
}

func (r *Runtime) PrintVectorPoolUsage() {
	var w bytes.Buffer
	w.WriteString(r.VectorPool.Transient.String())
	w.WriteByte('\n')
	w.WriteString(r.VectorPool.Small.String())
	logutil.Info(w.String())
}

func (r *Runtime) ExportLogtailStats() string {
	return fmt.Sprintf(
		"LogtailStats: Compact[%d|%d]",
		r.Logtail.CompactStats.SwapW(0),
		r.Logtail.CompactStats.Load(),
	)
}
