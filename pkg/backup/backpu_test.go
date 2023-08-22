// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"path"
	"sync"
	"testing"
	"time"
)

const (
	ModuleName = "Backup"
)

func TestBackupData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 10
	db.BindSchema(schema)
	testutil.CreateRelation(t, db.DB, "db", schema, true)

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	{
		txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	db.ForceLongCheckpoint()

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	//db.ForceCheckpoint()
	for _, data := range bats {
		txn, rel := db.GetRelation()
		v := testutil.GetSingleSortKeyValue(data, schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	db.ForceCheckpoint()
	db.BGCheckpointRunner.DisableCheckpoint()
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var locations string
			locations = candidate.GetLocation().String()
			locations += ":"
			locations += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = locations
		}
	}

	locations := make([]string, 0)
	for _, location := range files {
		locations = append(locations, location)
	}
	err = execBackup(ctx, db.Opts.Fs, service, locations)
	assert.Nil(t, err)
	db.Opts.Fs = service
	db.Restart(ctx)
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows-100), true)
	assert.NoError(t, txn.Commit(context.Background()))
}
