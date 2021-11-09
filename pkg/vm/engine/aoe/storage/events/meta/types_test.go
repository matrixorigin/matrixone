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

package meta

import (
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

var (
	workDir = "/tmp/mevents"
)

func TestBasicOps(t *testing.T) {
	os.RemoveAll(workDir)
	opts := config.NewOptions(workDir, config.CST_Customize, config.BST_S, config.SST_S)
	opts.Meta.Catalog, _ = opts.CreateCatalog(workDir)
	opts.Meta.Catalog.Start()
	defer opts.Meta.Catalog.Close()
	opts.Scheduler = sched.NewScheduler(opts, nil)

	now := time.Now()

	catalog := opts.Meta.Catalog
	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	database, err := catalog.SimpleCreateDatabase("db1", gen.Shard(100).First())
	assert.Nil(t, err)
	tbl, err := database.SimpleCreateTable(schema, gen.Next(100))
	assert.Nil(t, err)
	assert.NotNil(t, tbl)

	eCtx := &sched.Context{Opts: opts, Waitable: true}
	createBlkE := NewCreateBlkEvent(eCtx, tbl, nil, nil)
	opts.Scheduler.Schedule(createBlkE)
	err = createBlkE.WaitDone()
	assert.Nil(t, err)

	blk1 := createBlkE.GetBlock()
	assert.NotNil(t, blk1)
	assert.Equal(t, metadata.OpCreate, blk1.CommitInfo.Op)

	err = blk1.SimpleUpgrade(nil)
	assert.NotNil(t, err)

	blk1.SetCount(blk1.Segment.Table.Schema.BlockMaxRows)
	err = blk1.SimpleUpgrade(nil)
	assert.Nil(t, err)
	assert.True(t, blk1.IsFullLocked())

	schedCtx := &sched.Context{
		Opts:     opts,
		Waitable: true,
	}
	commitCtx := &sched.Context{Opts: opts, Waitable: true}
	commitCtx.AddMetaScope()
	commitE := sched.NewCommitBlkEvent(commitCtx, blk1)
	opts.Scheduler.Schedule(commitE)
	err = commitE.WaitDone()
	assert.Nil(t, err)

	blk2, err := tbl.SimpleGetBlock(blk1.Segment.Id, blk1.Id)
	assert.Nil(t, err)
	assert.True(t, blk2.IsFullLocked())

	for i := 0; i < 100; i++ {
		createBlkE = NewCreateBlkEvent(schedCtx, blk1.Segment.Table, nil, nil)
		opts.Scheduler.Schedule(createBlkE)
		err = createBlkE.WaitDone()
		assert.Nil(t, err)
	}
	du := time.Since(now)
	t.Log(du)
	time.Sleep(time.Duration(100) * time.Millisecond)

	opts.Scheduler.Stop()
}
