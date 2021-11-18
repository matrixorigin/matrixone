package aoedb

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/stretchr/testify/assert"
)

// -------- Test Description ---------------------------- [LogIndex,Checkpoint]
// 1.  Create db isntance and create a database           [   0,        ?     ]
// 2.  Create a table 1-1                                 [   1,        ?     ]
// 3.  Append 6 block rows into table 1-1                 [   2,        ?     ]
func TestSplit1(t *testing.T) {
	initTestEnv(t)
	inst1, gen, database := initTestDB1(t)

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err := inst1.CreateTable(createCtx)
	assert.Nil(t, err)

	rows := inst1.Store.Catalog.Cfg.BlockMaxRows * 12
	ck := mock.MockBatch(schema.Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schema.Name, ck)
	err = inst1.Append(appendCtx)
	assert.Nil(t, err)

	err = inst1.FlushDatabase(database.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, database.UncheckpointedCnt())
	coarseSize := database.GetSize()
	size := coarseSize * 7 / 8

	_, _, keys, ctx, err := inst1.SpliteDatabaseCheck(database.Name, uint64(size))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))

	newNames := make([]string, len(keys))
	for i, _ := range newNames {
		newNames[i] = fmt.Sprintf("splitted-%d", i)
	}
	renameTable := func(oldName, newDBName string) string {
		return oldName
	}
	splitIdx := gen.Alloc(database.GetShardId())
	err = inst1.SpliteDatabase(database.Name, newNames, renameTable, keys, ctx, splitIdx)
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, database.UncheckpointedCnt())
	t.Log(inst1.Store.Catalog.PString(metadata.PPL1, 0))

	inst1.Close()
}
