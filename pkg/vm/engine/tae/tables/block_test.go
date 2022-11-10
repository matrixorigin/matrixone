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

package tables

import (
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/stretchr/testify/assert"
)

func TestGetActiveRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)
	mvcc := updates.NewMVCCHandle(nil)
	blk := &dataBlock{
		mvcc: mvcc,
	}

	// appendnode1 [0,1)
	an1, _ := mvcc.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = ts1
	an1.Prepare = ts1
	an1.End = ts1

	// appendnode1 [1,2)
	an2, _ := mvcc.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = ts1
	an2.Prepare = ts1
	an2.End = ts1

	// index uint8(1)-0,1
	vec := containers.MakeVector(types.T_int8.ToType(), false)
	vec.Append(int8(1))
	vec.Append(int8(1))
	idx := indexwrapper.NewPkMutableIndex(types.T_int8.ToType())
	keysCtx := &index.KeysCtx{
		Keys: vec,
	}
	keysCtx.SelectAll()
	err := idx.BatchUpsert(keysCtx, 0)
	assert.NoError(t, err)
	blk.pkIndex = idx

	row, err := blk.GetActiveRow(int8(1), ts2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), row)

	//abort appendnode2
	an2.Aborted = true

	row, err = blk.GetActiveRow(int8(1), ts2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), row)
}
