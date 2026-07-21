// Copyright 2026 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/stretchr/testify/require"
)

type tombstoneScanCounter struct {
	data.Object
	scans int
}

func (counter *tombstoneScanCounter) CollectObjectTombstoneInRange(
	context.Context,
	types.TS,
	types.TS,
	*types.Objectid,
	**containers.Batch,
	*mpool.MPool,
	*containers.VectorPool,
) error {
	counter.scans++
	return nil
}

func TestTombstoneRangeScanStopsBeforeOldHistory(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)

	oldID := objectio.NewObjectid()
	oldStats := objectio.NewObjectStatsWithObjectID(&oldID, true, false, false)
	oldData := &tombstoneScanCounter{}
	_, err = table.CreateCommittedObject(
		types.BuildTS(1, 0),
		&objectio.CreateObjOpt{Stats: oldStats, IsTombstone: true},
		func(*catalog.ObjectEntry) data.Object { return oldData },
	)
	require.NoError(t, err)

	newerID := objectio.NewObjectid()
	newerStats := objectio.NewObjectStatsWithObjectID(&newerID, true, false, false)
	newer, err := table.CreateCommittedObject(
		types.BuildTS(2, 0),
		&objectio.CreateObjOpt{Stats: newerStats, IsTombstone: true},
		nil,
	)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(newer, types.BuildTS(3, 0))

	targetID := objectio.NewObjectid()
	bat, err := TombstoneRangeScanByObject(
		context.Background(),
		table,
		targetID,
		types.BuildTS(4, 0),
		types.BuildTS(5, 0),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, bat)
	require.Zero(t, oldData.scans)
}
