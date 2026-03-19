// Copyright 2022 Matrix Origin
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

package logtailreplay

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// TestGetObjectsFromCheckpointEntries exposes getObjectsFromCheckpointEntries for tests in other packages.
func TestGetObjectsFromCheckpointEntries(
	ctx context.Context,
	tid uint64,
	sid string,
	start, end types.TS,
	checkpoint []*checkpoint.CheckpointEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj []*objectio.ObjectEntry,
	err error,
) {
	return getObjectsFromCheckpointEntries(ctx, tid, sid, start, end, checkpoint, mp, fs)
}

type CheckpointEntryReader = checkpointEntryReader

// SetCheckpointReaderFactoryForTest overrides the checkpoint reader factory during tests.
// It returns a restore function that should be deferred by callers.
func SetCheckpointReaderFactoryForTest(factory func(uint32, objectio.Location, uint64, *mpool.MPool, fileservice.FileService) checkpointEntryReader) func() {
	old := newCKPReaderWithTableID
	newCKPReaderWithTableID = factory
	return func() {
		newCKPReaderWithTableID = old
	}
}
