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

	prepareCtx := &PrepareSplitCtx{
		DB:   database.Name,
		Size: uint64(size),
	}
	_, _, keys, ctx, err := inst1.PrepareSplitDatabase(prepareCtx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))

	newNames := make([]string, len(keys))
	for i, _ := range newNames {
		newNames[i] = fmt.Sprintf("splitted-%d", i)
	}
	renameTable := func(oldName, newDBName string) string {
		return oldName
	}
	execCtx := &ExecSplitCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		NewNames:      newNames,
		RenameTable:   renameTable,
		SplitKeys:     keys,
		SplitCtx:      ctx,
	}
	err = inst1.ExecSplitDatabase(execCtx)
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		return database.UncheckpointedCnt() == 0
	})
	assert.Equal(t, 0, database.UncheckpointedCnt())
	t.Log(inst1.Store.Catalog.PString(metadata.PPL1, 0))

	inst1.Close()
}
