package meta

import (
	"github.com/stretchr/testify/assert"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"os"
	"testing"
	"time"
	// log "github.com/sirupsen/logrus"
)

func createInfo() *md.MetaInfo {
	conf := &md.Configuration{
		BlockMaxRows:     md.BLOCK_ROW_COUNT,
		SegmentMaxBlocks: md.SEGMENT_BLOCK_COUNT,
		Dir:              "/tmp",
	}
	info := md.NewMetaInfo(conf)
	return info
}

func TestBasicOps(t *testing.T) {
	opts := e.Options{}
	info := createInfo()
	opts.Meta.Info = info
	opts.FillDefaults("/tmp")
	opts.Meta.Updater.Start()

	now := time.Now()

	schema := md.MockSchema(2)
	opCtx := OpCtx{Opts: &opts, Schema: schema}
	op := NewCreateTblOp(&opCtx)
	op.Push()
	err := op.WaitDone()
	assert.Nil(t, err)

	tbl := op.GetTable()
	assert.NotNil(t, tbl)

	t.Log(info.String())
	opCtx = OpCtx{Opts: &opts}
	blkop := NewCreateBlkOp(&opCtx, tbl.ID, nil)
	blkop.Push()
	err = blkop.WaitDone()
	assert.Nil(t, err)

	blk1 := blkop.GetBlock()
	assert.NotNil(t, blk1)
	assert.Equal(t, blk1.GetBoundState(), md.Detatched)

	assert.Equal(t, blk1.DataState, md.EMPTY)
	blk1.SetCount(blk1.MaxRowCount)
	assert.Equal(t, blk1.DataState, md.FULL)

	blk2, err := info.ReferenceBlock(blk1.TableID, blk1.SegmentID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk2.DataState, md.EMPTY)
	assert.Equal(t, blk2.Count, uint64(0))

	opCtx = OpCtx{Block: blk1, Opts: &opts}
	updateop := NewUpdateOp(&opCtx)
	updateop.Push()
	err = updateop.WaitDone()
	assert.Nil(t, err)

	blk3, err := info.ReferenceBlock(blk1.TableID, blk1.SegmentID, blk1.ID)
	assert.Nil(t, err)
	assert.Equal(t, blk3.DataState, md.FULL)
	assert.Equal(t, blk1.Count, blk3.Count)

	for i := 0; i < 100; i++ {
		opCtx = OpCtx{Opts: &opts}
		blkop = NewCreateBlkOp(&opCtx, blk1.TableID, nil)
		blkop.Push()
		err = blkop.WaitDone()
		assert.Nil(t, err)
	}
	du := time.Since(now)
	t.Log(du)

	info_copy := info.Copy()

	path := "/tmp/tttttt"
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	assert.Equal(t, err, nil)
	err = info_copy.Serialize(w)
	assert.Nil(t, err)
	w.Close()

	r, err := os.OpenFile(path, os.O_RDONLY, 0666)
	assert.Equal(t, err, nil)

	de_info, err := md.Deserialize(r)
	assert.Nil(t, err)
	assert.NotNil(t, de_info)
	assert.Equal(t, info.Sequence.NextBlockID, de_info.Sequence.NextBlockID)
	assert.Equal(t, info.Sequence.NextSegmentID, de_info.Sequence.NextSegmentID)
	assert.Equal(t, info.Sequence.NextTableID, de_info.Sequence.NextTableID)

	r.Close()

	du = time.Since(now)
	t.Log(du)

	opts.Meta.Updater.Stop()
}
