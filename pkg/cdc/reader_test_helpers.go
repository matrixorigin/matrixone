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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	data       []*batch.Batch
	packer     *types.Packer
	ownsPacker bool
	called     int
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
	if changes.ownsPacker && changes.packer != nil {
		changes.packer.Close()
		changes.packer = nil
	}
	return nil
}
