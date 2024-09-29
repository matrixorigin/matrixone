// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/stretchr/testify/assert"
)

type testRows struct {
	id int
}

func (r *testRows) Length() int               { return 1 }
func (r *testRows) Window(_, _ int) *testRows { return nil }

func createBlockFn[R any](_ R) *model.TimedSliceBlock[R] {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return model.NewTimedSliceBlock[R](ts)
}

func TestAOT1(t *testing.T) {
	aot := model.NewAOT(
		10,
		createBlockFn[*testRows],
		func(a, b *model.TimedSliceBlock[*testRows]) bool {
			return a.BornTS.LT(&b.BornTS)
		})
	for i := 0; i < 30; i++ {
		rows := &testRows{id: i}
		err := aot.Append(rows)
		assert.NoError(t, err)
	}
	t.Log(aot.BlockCount())
}

func TestAOT2(t *testing.T) {
	schema := catalog.MockSchemaAll(14, 3)
	factory := func(_ *containers.Batch) *model.BatchBlock {
		id := common.NextGlobalSeqNum()
		return model.NewBatchBlock(id, schema.Attrs(), schema.Types(), containers.Options{})
	}
	aot := model.NewAOT(
		10,
		factory,
		func(a, b *model.BatchBlock) bool {
			return a.ID < b.ID
		})
	defer aot.Close()

	bat := catalog.MockBatch(schema, 42)
	defer bat.Close()

	assert.NoError(t, aot.Append(bat))
	t.Log(aot.BlockCount())
	assert.Equal(t, 5, aot.BlockCount())
	rows := 0
	fn := func(block *model.BatchBlock) bool {
		rows += block.Length()
		return true
	}
	aot.Scan(fn)
	assert.Equal(t, 42, rows)
}
