package meta

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	workDir = "/tmp/mevents"
)

func TestBasicOps(t *testing.T) {
	opts := e.Options{}
	info := md.MockInfo(&opts.Mu, md.BLOCK_ROW_COUNT, md.SEGMENT_BLOCK_COUNT)
	info.Conf.Dir = workDir
	opts.Meta.Info = info
	opts.FillDefaults(info.Conf.Dir)
	opts.Scheduler = dbsched.NewScheduler(&opts, nil)

	now := time.Now()

	tabletInfo := md.MockTableInfo(2)
	eCtx := &dbsched.Context{Opts: &opts, Waitable: true}
	createTblE := NewCreateTableEvent(eCtx, dbi.TableOpCtx{TableName: tabletInfo.Name}, tabletInfo)
	opts.Scheduler.Schedule(createTblE)
	err := createTblE.WaitDone()
	assert.Nil(t, err)

	tbl := createTblE.GetTable()
	assert.NotNil(t, tbl)

	t.Log(info.String())
	createBlkE := NewCreateBlkEvent(eCtx, tbl.ID, nil)
	opts.Scheduler.Schedule(createBlkE)
	err = createBlkE.WaitDone()
	assert.Nil(t, err)

	blk1 := createBlkE.GetBlock()
	assert.NotNil(t, blk1)
	assert.Equal(t, blk1.GetBoundState(), md.Detatched)

	assert.Equal(t, blk1.DataState, md.EMPTY)
	blk1.SetCount(blk1.MaxRowCount)
	assert.Equal(t, blk1.DataState, md.FULL)

	blk2, err := info.ReferenceBlock(blk1.Segment.Table.ID, blk1.Segment.ID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk2.DataState, md.EMPTY)
	assert.Equal(t, blk2.Count, uint64(0))

	schedCtx := &dbsched.Context{
		Opts:     &opts,
		Waitable: true,
	}
	commitCtx := &dbsched.Context{Opts: &opts, Waitable: true}
	commitCtx.AddMetaScope()
	commitE := dbsched.NewCommitBlkEvent(commitCtx, blk1)
	opts.Scheduler.Schedule(commitE)
	err = commitE.WaitDone()
	assert.Nil(t, err)

	blk3, err := info.ReferenceBlock(blk1.Segment.Table.ID, blk1.Segment.ID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk3.DataState, md.FULL)
	assert.Equal(t, blk1.Count, blk3.Count)

	for i := 0; i < 100; i++ {
		createBlkE = NewCreateBlkEvent(schedCtx, blk1.Segment.Table.ID, nil)
		opts.Scheduler.Schedule(createBlkE)
		err = createBlkE.WaitDone()
		assert.Nil(t, err)
	}
	du := time.Since(now)
	t.Log(du)

	ctx := md.CopyCtx{Attached: true}
	info_copy := info.Copy(ctx)

	fpath := path.Join(info.Conf.Dir, "tttttttt")
	w, err := os.Create(fpath)
	assert.Equal(t, err, nil)
	err = info_copy.Serialize(w)
	assert.Nil(t, err)
	w.Close()

	// r, err := os.OpenFile(fpath, os.O_RDONLY, 0666)
	// assert.Equal(t, err, nil)

	// de_info, err := md.Deserialize(r)
	// assert.Nil(t, err)
	// assert.NotNil(t, de_info)
	// assert.Equal(t, info.Sequence.NextBlockID, de_info.Sequence.NextBlockID)
	// assert.Equal(t, info.Sequence.NextSegmentID, de_info.Sequence.NextSegmentID)
	// assert.Equal(t, info.Sequence.NextTableID, de_info.Sequence.NextTableID)

	// r.Close()

	du = time.Since(now)
	t.Log(du)

	opts.Scheduler.Stop()
}
