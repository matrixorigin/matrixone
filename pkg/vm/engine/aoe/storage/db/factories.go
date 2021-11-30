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

package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type nodeFlusher struct {
	opts *storage.Options
}

func (f nodeFlusher) flush(node bb.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) error {
	ctx := &sched.Context{Opts: f.opts, Waitable: true}
	node.Ref()
	defer node.Unref()
	e := sched.NewFlushTransientBlockEvent(ctx, node, data, meta, file)
	f.opts.Scheduler.Schedule(e)
	return e.WaitDone()
}
