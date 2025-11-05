// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Test helper constants
const (
	batchCnt = 6
	rowCnt   = 3
)

// testChangesHandle is a test helper for mocking engine.ChangesHandle
var _ engine.ChangesHandle = new(testChangesHandle)

type testChangesHandle struct {
	dbName, tblName string
	dbId, tblId     uint64
	data            []*batch.Batch
	mp              *mpool.MPool
	packer          *types.Packer
	called          int
	toTs            types.TS
}

// newTestChangesHandle creates a test ChangesHandle with mock data
func newTestChangesHandle(
	dbName, tblName string,
	dbId, tblId uint64,
	toTs types.TS,
	mp *mpool.MPool,
	packer *types.Packer,
) *testChangesHandle {
	ret := &testChangesHandle{
		dbName:  dbName,
		tblName: tblName,
		dbId:    dbId,
		tblId:   tblId,
		mp:      mp,
		packer:  packer,
		toTs:    toTs,
	}
	/*
		assume tables looks like:
			test.t*
		same schema :
			create table t1(a int,b int, primary key(a,b))
	*/

	if dbName == "test" && strings.HasPrefix(tblName, "t") {
		ret.makeData()
	}
	return ret
}

func (changes *testChangesHandle) makeData() {
	changes.packer.Reset()
	defer func() {
		changes.packer.Reset()
	}()
	//no checkpoint
	//insert:
	// Correct order: user cols | cpk | commit-ts → a,b,cpk,ts
	//delete:
	//cpk, ts
	for i := 0; i < batchCnt+1; i++ {
		bat := allocTestBatch(
			[]string{
				"a",   // User column
				"b",   // User column
				"cpk", // Composite PK
				"ts",  // Commit timestamp (MUST be last)
			},
			[]types.Type{
				types.T_int32.ToType(),
				types.T_int32.ToType(),
				types.T_varchar.ToType(),
				types.T_TS.ToType(),
			},
			0,
			changes.mp,
		)
		bat.SetRowCount(rowCnt)
		for j := 0; j < rowCnt; j++ {
			//a
			_ = vector.AppendFixed(bat.Vecs[0], int32(j), false, changes.mp)
			//b
			_ = vector.AppendFixed(bat.Vecs[1], int32(j), false, changes.mp)
			//cpk
			changes.packer.Reset()
			changes.packer.EncodeInt32(int32(j))
			changes.packer.EncodeInt32(int32(j))
			_ = vector.AppendBytes(bat.Vecs[2], changes.packer.Bytes(), false, changes.mp)
			//ts
			_ = vector.AppendFixed(bat.Vecs[3], changes.toTs, false, changes.mp)
		}

		changes.data = append(changes.data, bat)
	}
}

func (changes *testChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	if changes.called < 1 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Snapshot, nil
	} else if changes.called >= 1 && changes.called < batchCnt-2 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_wip, nil
	} else if changes.called == batchCnt-2 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_done, nil
	} else if changes.called == batchCnt-1 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_wip, nil
	}
	return nil, nil, engine.ChangesHandle_Tail_wip, err
}

func (changes *testChangesHandle) Close() error {
	return nil
}

// allocTestBatch allocates a test batch with given schema
func allocTestBatch(
	attrName []string,
	tt []types.Type,
	batchSize int,
	mp *mpool.MPool,
) *batch.Batch {
	batchData := batch.New(attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.NewVec(tt[i])
		if err := vec.PreExtend(batchSize, mp); err != nil {
			panic(err)
		}
		vec.SetLength(batchSize)
		batchData.Vecs[i] = vec
	}

	batchData.SetRowCount(batchSize)
	return batchData
}
