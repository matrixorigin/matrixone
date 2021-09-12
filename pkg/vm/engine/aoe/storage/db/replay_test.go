package db

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplay1(t *testing.T) {
	initDBTest()
	inst := initDB(engine.MUTABLE_FT)
	tInfo := metadata.MockTableInfo(2)
	name := "mockcon"
	tid, err := inst.CreateTable(tInfo, dbi.TableOpCtx{TableName: name, OpIndex: metadata.NextGloablSeqnum()})
	assert.Nil(t, err)

	meta, err := inst.Opts.Meta.Info.ReferenceTable(tid)
	assert.Nil(t, err)
	rel, err := inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.MetaInfo.Conf.BlockMaxRows / 2
	ibat := chunk.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   metadata.NextGloablSeqnum(),
			Data:      ibat,
			TableName: meta.Schema.Name,
		})
		assert.Nil(t, err)
	}

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	err = inst.Flush(name)
	assert.Nil(t, err)
	time.Sleep(time.Duration(10) * time.Millisecond)
	err = inst.Flush(name)
	assert.Nil(t, err)

	rel.Close()
	inst.Close()

	time.Sleep(time.Duration(20) * time.Millisecond)

	inst = initDB(engine.MUTABLE_FT)
	rel, err = inst.Relation(meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	t.Log(rel.Rows())

	insertFn()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.Flush(meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	time.Sleep(time.Duration(10) * time.Millisecond)
	defer rel.Close()
	defer inst.Close()
}
