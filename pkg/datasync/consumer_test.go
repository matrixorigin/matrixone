// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func newTestFS() fileservice.FileService {
	shared, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	if err != nil {
		panic(err)
	}
	standby, err := fileservice.NewMemoryFS(
		defines.StandbyFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	if err != nil {
		panic(err)
	}
	fs, err := fileservice.NewFileServices(
		defines.SharedFileServiceName,
		shared,
		standby,
	)
	if err != nil {
		panic(err)
	}
	return fs
}

func createTestConsumer(fs fileservice.FileService, db *testutil.TestEngine) *consumer {
	if fs == nil {
		fs = newTestFS()
	}
	rt := runtime.DefaultRuntime()
	var writeLsn, syncedLsn atomic.Uint64
	ms := newMockServer()
	w := newConsumer(
		*newCommon().withLog(rt.Logger()).withRuntime(rt),
		fs,
		&writeLsn,
		&syncedLsn,
		withLogClient(newMockLogClient(ms, logShardID)),
		withUpstreamLogClient(newMockLogClient(ms, upstreamLogShardID)),
		withTxnClient(newMockTxnClient(db)),
		withWaitPermissionInterval(time.Millisecond*10),
	)
	return w.(*consumer)
}

func TestCopyFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := createTestConsumer(nil, nil)
	assert.NotNil(t, c)
	defer c.Close()

	ctx := context.Background()
	fileName := "name1"
	fileContent := "test content"
	vec := fileservice.IOVector{
		FilePath: fileName,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(fileContent)),
				Data:   []byte(fileContent),
			},
		},
	}
	assert.NoError(t, c.srcFS.Write(ctx, vec))
	assert.NoError(t, c.copyFile(ctx, fileName, ""))
	e, err := c.dstFS.StatFile(ctx, fileName)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, fileName, e.Name)
	assert.Equal(t, int64(len(fileContent)), e.Size)
	assert.False(t, e.IsDir)
	// already exists.
	assert.NoError(t, c.copyFile(ctx, fileName, ""))
	// directory not exists.
	assert.Error(t, c.copyFile(ctx, fileName, "/test"))
}

func TestCopyFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("wrong location format", func(t *testing.T) {
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		ctx := context.Background()
		var locations []string
		// invalid file name
		fileName := fmt.Sprintf("%s_aaa", uuid.New().String())
		location := fmt.Sprintf("%s_0_1_1_1_0_0", fileName)
		locations = append(locations, location)
		fileContent := "test content"
		vec := fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   int64(len(fileContent)),
					Data:   []byte(fileContent),
				},
			},
		}
		assert.NoError(t, c.srcFS.Write(ctx, vec))
		assert.Error(t, c.copyFiles(ctx, locations, ""))
		entries, err := c.dstFS.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(entries))
	})

	t.Run("failed to copy", func(t *testing.T) {
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		ctx := context.Background()
		var locations []string
		fileName := fmt.Sprintf("%s_00000", uuid.New().String())
		location := fmt.Sprintf("%s_0_1_1_1_0_0", fileName)
		locations = append(locations, location)
		fileContent := "test content"
		vec := fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   int64(len(fileContent)),
					Data:   []byte(fileContent),
				},
			},
		}
		assert.NoError(t, c.srcFS.Write(ctx, vec))
		assert.Error(t, c.copyFiles(ctx, locations, "/nodir"))
		entries, err := c.dstFS.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(entries))
	})

	t.Run("ok", func(t *testing.T) {
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		ctx := context.Background()
		var locations []string
		for i := 0; i < 100; i++ {
			fileName := fmt.Sprintf("%s_%05d", uuid.New().String(), i)
			location := fmt.Sprintf("%s_0_1_1_1_0_0", fileName)
			locations = append(locations, location)
			fileContent := "test content"
			vec := fileservice.IOVector{
				FilePath: fileName,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   int64(len(fileContent)),
						Data:   []byte(fileContent),
					},
				},
			}
			assert.NoError(t, c.srcFS.Write(ctx, vec))
		}
		assert.NoError(t, c.copyFiles(ctx, locations, ""))
		entries, err := c.dstFS.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 100, len(entries))
	})
}

func withCheckpointData(t *testing.T, f func(
	ctx context.Context,
	fs fileservice.FileService,
	db *testutil.TestEngine,
)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := config.WithLongScanAndCKPOptsAndQuickGC(nil)
	opts.Fs = newTestFS()
	db := testutil.NewTestEngine(ctx, "datasync", t, opts)
	defer func() {
		opts.Fs.Close()
		_ = db.Close()
	}()

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)
	testutil.CreateRelation(t, db.DB, "db", schema, true)

	totalRows := uint64(schema.Extra.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	{
		txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	db.ForceLongCheckpointTruncate()
	db.ForceLongCheckpointTruncate()

	f(ctx, opts.Fs, db)
}

func TestParseCheckpointLocations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty", func(t *testing.T) {
		var c consumer
		locations, err := c.parseCheckpointLocations(context.Background(), "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(locations))
	})

	t.Run("wrong format 1", func(t *testing.T) {
		var c consumer
		locations, err := c.parseCheckpointLocations(context.Background(), "1:2:3")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(locations))
	})

	t.Run("wrong format 2", func(t *testing.T) {
		var c consumer
		locations, err := c.parseCheckpointLocations(context.Background(), "1:aaa")
		assert.Error(t, err)
		assert.Equal(t, 0, len(locations))
	})

	t.Run("wrong format 3", func(t *testing.T) {
		var c consumer
		locations, err := c.parseCheckpointLocations(context.Background(),
			"0191b752-b7af-76d9-af25-e702423dc08a_00a00_1_2364_1046_9541_0_0:12")
		assert.Error(t, err)
		assert.Equal(t, 0, len(locations))
	})

	t.Run("not eixsts", func(t *testing.T) {
		var c consumer
		c.srcFS = newTestFS()
		locations, err := c.parseCheckpointLocations(context.Background(),
			"0191b752-b7af-76d9-af25-e702423dc08a_00000_1_2364_1046_9541_0_0:12")
		assert.Error(t, err)
		assert.Equal(t, 0, len(locations))
	})

	t.Run("ok", func(t *testing.T) {
		withCheckpointData(t, func(ctx context.Context, fs fileservice.FileService, db *testutil.TestEngine) {
			c := createTestConsumer(fs, db)
			assert.NotNil(t, c)
			defer c.Close()
			ckp, err := c.txnClient.getLatestCheckpoint(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, ckp)
			locations, err := c.parseCheckpointLocations(ctx, ckp.Location)
			assert.NoError(t, err)
			assert.Equal(t, 35, len(locations))
		})
	})
}

func TestFullSync(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("error", func(t *testing.T) {
		var c consumer
		err := c.fullSync(context.Background(), "1:aaa")
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		withCheckpointData(t, func(ctx context.Context, fs fileservice.FileService, db *testutil.TestEngine) {
			c := createTestConsumer(fs, db)
			assert.NotNil(t, c)
			defer c.Close()
			ckp, err := c.txnClient.getLatestCheckpoint(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, ckp)
			err = c.fullSync(ctx, ckp.Location)
			assert.NoError(t, err)
			files, err := c.dstFS.List(ctx, "")
			assert.NoError(t, err)
			assert.Equal(t, 35, len(files))
		})
	})
}

func TestConsume(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("already consumed", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		assert.NoError(t, c.consume(ctx, logservice.LogRecord{
			Lsn: 6,
		}, false))
		assert.Equal(t, initSyncedLsn, c.syncedLsn.Load())
	})

	t.Run("empty data", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		assert.NoError(t, c.consume(ctx, logservice.LogRecord{
			Lsn: 10,
		}, false))
		assert.Equal(t, initSyncedLsn, c.syncedLsn.Load())
	})

	t.Run("invalid location", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		assert.NoError(t, c.consume(ctx, logservice.LogRecord{
			Lsn:  10,
			Data: []byte("invalid"),
		}, false))
		assert.Equal(t, initSyncedLsn, c.syncedLsn.Load())
	})

	t.Run("file not found", func(t *testing.T) {
		rec, fileName, err := genRecordWithLocation(8, 0)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		assert.Error(t, c.consume(ctx, logservice.LogRecord{
			Lsn:  10,
			Data: rec.Data,
		}, false))
		assert.Equal(t, initSyncedLsn, c.syncedLsn.Load())
	})

	t.Run("ok", func(t *testing.T) {
		rec, fileName, err := genRecordWithLocation(8, 0)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("test"),
				},
			},
		}))
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		assert.NoError(t, c.consume(ctx, logservice.LogRecord{
			Lsn:  10,
			Data: rec.Data,
		}, false))
		assert.Equal(t, uint64(10), c.syncedLsn.Load())
	})
}

func TestConsumeEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("not upstream, failed", func(t *testing.T) {
		var upstreamLsn uint64 = 100
		rec, fileName, err := genRecordWithLocation(8, upstreamLsn)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		err = c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: rec.Data,
			},
		}, false)
		assert.Error(t, err)
		assert.Equal(t, uint64(8), c.syncedLsn.Load())
		assert.Equal(t, uint64(0), c.lastRequiredLsn)
	})

	t.Run("not upstream, log api error", func(t *testing.T) {
		var upstreamLsn uint64 = 100
		rec, fileName, err := genRecordWithLocation(8, upstreamLsn)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("test"),
				},
			},
		}))
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("setRequiredLsn")
		defer lc.clearFakeError("setRequiredLsn")
		err = c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: rec.Data,
			},
		}, false)
		assert.Error(t, err)
		assert.Equal(t, uint64(10), c.syncedLsn.Load())
		assert.Equal(t, uint64(0), c.lastRequiredLsn)
	})

	t.Run("not upstream, ok", func(t *testing.T) {
		var upstreamLsn uint64 = 100
		rec, fileName, err := genRecordWithLocation(8, upstreamLsn)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("test"),
				},
			},
		}))
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		err = c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: rec.Data,
			},
		}, false)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), c.syncedLsn.Load())
		assert.Equal(t, upstreamLsn+1, c.lastRequiredLsn)
	})

	t.Run("not upstream, not location entry", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		err := c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: make([]byte, 100),
			},
		}, false)
		assert.NoError(t, err)
		assert.Equal(t, uint64(8), c.syncedLsn.Load())
		assert.Equal(t, uint64(0), c.lastRequiredLsn)
	})

	t.Run("upstream, log api error", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("setRequiredLsn")
		defer lc.clearFakeError("setRequiredLsn")
		err := c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: make([]byte, 100),
			},
		}, true)
		assert.Error(t, err)
		assert.Equal(t, uint64(8), c.syncedLsn.Load())
		assert.Equal(t, uint64(0), c.lastRequiredLsn)
	})

	t.Run("upstream, ok", func(t *testing.T) {
		var upstreamLsn uint64 = 100
		rec, fileName, err := genRecordWithLocation(8, upstreamLsn)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)

		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
			FilePath: fileName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("test"),
				},
			},
		}))
		var initSyncedLsn uint64 = 8
		c.syncedLsn.Store(initSyncedLsn)
		err = c.consumerEntries(ctx, []logservice.LogRecord{
			{
				Lsn:  10,
				Data: rec.Data,
			},
		}, true)
		assert.NoError(t, err)
		assert.Equal(t, uint64(8), c.syncedLsn.Load())
		assert.Equal(t, uint64(10+1), c.lastRequiredLsn)
	})
}

func TestCheckRole(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("no leader ID", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		c.common.setShardReplicaID(logShardID, 10)
		assert.Equal(t, false, c.checkRole(ctx))
	})

	t.Run("shard not start", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)
		assert.Equal(t, false, c.checkRole(ctx))
	})

	t.Run("log api error", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("getLeaderID")
		defer lc.clearFakeError("getLeaderID")
		assert.Equal(t, false, c.checkRole(ctx))
	})

	t.Run("ok", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)
		assert.Equal(t, true, c.checkRole(ctx))
	})
}

func TestCleanState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("log api error", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		count := 10
		for i := 0; i < count; i++ {
			lsn, err := c.logClient.write(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i)+1, lsn)
		}
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("getLatestLsn")
		go func() {
			time.Sleep(time.Second)
			lc.clearFakeError("getLatestLsn")
		}()
		c.cleanState(ctx, time.Millisecond*200)
		assert.Equal(t, uint64(count), c.syncedLsn.Load())
		requiredLsn, err := c.upstreamLogClient.getRequiredLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), requiredLsn)
	})

	t.Run("upstream log api error", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		count := 10
		for i := 0; i < count; i++ {
			lsn, err := c.logClient.write(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i)+1, lsn)
		}
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("setRequiredLsn")
		go func() {
			time.Sleep(time.Second)
			lc.clearFakeError("setRequiredLsn")
		}()
		c.cleanState(ctx, time.Millisecond*200)
		assert.Equal(t, uint64(count), c.syncedLsn.Load())
		requiredLsn, err := c.logClient.getRequiredLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), requiredLsn)
	})

	t.Run("ok", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		count := 10
		for i := 0; i < count; i++ {
			lsn, err := c.logClient.write(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i)+1, lsn)
		}
		c.cleanState(ctx, time.Millisecond)
		assert.Equal(t, uint64(count), c.syncedLsn.Load())
		requiredLsn, err := c.logClient.getRequiredLsnWithRetry(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), requiredLsn)
	})
}

func TestInitSyncedLsn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("log api error", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("getTruncatedLsn")
		defer lc.clearFakeError("getTruncatedLsn")
		assert.Error(t, c.initSyncedLsn(ctx))
	})

	t.Run("ok", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		count := 10
		for i := 0; i < count; i++ {
			lsn, err := c.logClient.write(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i)+1, lsn)
		}
		assert.NoError(t, c.logClient.truncate(ctx, 5))
		assert.NoError(t, c.initSyncedLsn(ctx))
		assert.Equal(t, uint64(5), c.syncedLsn.Load())
	})
}

func TestWaitPermission(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		cancel()
		assert.Error(t, c.waitPermission(ctx, time.Millisecond*10))
	})

	t.Run("ok", func(t *testing.T) {
		ctx := context.Background()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		go func() {
			time.Sleep(time.Millisecond * 200)
			c.common.setShardReplicaID(logShardID, 10)
			lc := c.logClient.(*mockLogClient)
			lc.setLeaderID(10)
		}()
		assert.NoError(t, c.waitPermission(ctx, time.Millisecond*10))
	})
}

func TestLoopWork(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		cancel()
		assert.Equal(t, context.Canceled, c.loop(ctx, time.Second))
	})

	t.Run("wrong role", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		assert.NoError(t, c.loop(ctx, time.Second))
	})

	t.Run("empty entries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		c.syncedLsn.Store(10)
		go func() {
			time.Sleep(time.Millisecond * 200)
			c.writeLsn.Store(20)
			time.Sleep(time.Millisecond * 200)
			cancel()
		}()
		assert.Equal(t, context.Canceled, c.loop(ctx, time.Millisecond*10))
	})

	t.Run("file not found entries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		for i := 1; i <= 30; i++ {
			rec, _, err := genRecordWithLocation(0, uint64(i))
			assert.NoError(t, err)
			lsn, err := c.logClient.write(ctx, rec.Data)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i), lsn)
		}

		c.syncedLsn.Store(10)
		go func() {
			time.Sleep(time.Millisecond * 200)
			c.writeLsn.Store(20)
			time.Sleep(time.Millisecond * 200)
			cancel()
		}()
		assert.True(t, moerr.IsMoErrCode(c.loop(ctx, time.Millisecond), moerr.ErrFileNotFound))
		assert.Equal(t, uint64(10), c.syncedLsn.Load())
		requiredLsn, err := c.logClient.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), requiredLsn)
	})

	t.Run("log api error, then context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		c.syncedLsn.Store(10)
		go func() {
			time.Sleep(time.Millisecond * 200)
			time.Sleep(time.Millisecond * 200)
			lc.fakeError("readEntries")
			defer lc.clearFakeError("readEntries")
			c.writeLsn.Store(20)
			time.Sleep(time.Millisecond * 200)
			cancel()
		}()
		assert.Equal(t, context.Canceled, c.loop(ctx, time.Millisecond))
		assert.Equal(t, uint64(10), c.syncedLsn.Load())
		requiredLsn, err := c.logClient.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), requiredLsn)
	})

	t.Run("valid entries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		for i := 1; i <= 30; i++ {
			rec, fileName, err := genRecordWithLocation(0, uint64(i))
			assert.NoError(t, err)
			assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
				FilePath: fileName,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   4,
						Data:   []byte("test"),
					},
				},
			}))
			lsn, err := c.logClient.write(ctx, rec.Data)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i), lsn)
		}

		c.syncedLsn.Store(10)
		go func() {
			time.Sleep(time.Millisecond * 200)
			c.writeLsn.Store(20)
			time.Sleep(time.Millisecond * 200)
			cancel()
		}()
		assert.Equal(t, context.Canceled, c.loop(ctx, time.Millisecond*10))
		assert.Equal(t, uint64(30), c.syncedLsn.Load())
		requiredLsn, err := c.logClient.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(31), requiredLsn)
	})
}

func TestCompleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("upstream log api error1", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		lc := c.upstreamLogClient.(*mockLogClient)
		lc.fakeError("getLatestLsn")
		defer lc.clearFakeError("getLatestLsn")
		assert.Error(t, c.completeData(ctx))
	})

	t.Run("log api error2", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		lc := c.logClient.(*mockLogClient)
		lc.fakeError("getRequiredLsn")
		defer lc.clearFakeError("getRequiredLsn")
		assert.Error(t, c.completeData(ctx))
	})

	t.Run("log api error3", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		lc := c.logClient.(*mockLogClient)
		lc.fakeError("setRequiredLsn")
		defer lc.clearFakeError("setRequiredLsn")
		assert.Error(t, c.completeData(ctx))
	})

	t.Run("txn api error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()

		tc := c.txnClient.(*mockTxnClient)
		tc.fakeError()
		defer tc.clearFakeError()
		assert.Error(t, c.completeData(ctx))
	})

	t.Run("no checkpoint", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)
		err := c.completeData(ctx)
		assert.NoError(t, err)
		files, err := c.dstFS.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(files))
		requiredLsn, err := c.logClient.getRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), requiredLsn)
	})

	t.Run("ok, but no entries", func(t *testing.T) {
		withCheckpointData(t, func(ctx context.Context, fs fileservice.FileService, db *testutil.TestEngine) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c := createTestConsumer(fs, db)
			assert.NotNil(t, c)
			defer c.Close()
			// set role
			c.common.setShardReplicaID(logShardID, 10)
			lc := c.logClient.(*mockLogClient)
			lc.setLeaderID(10)
			err := c.completeData(ctx)
			assert.NoError(t, err)
			files, err := c.dstFS.List(ctx, "")
			assert.NoError(t, err)
			assert.Equal(t, 35, len(files))
			requiredLsn, err := c.logClient.getRequiredLsn(ctx)
			assert.NoError(t, err)
			assert.Equal(t, uint64(103), requiredLsn)
		})
	})

	t.Run("ok, there are entries", func(t *testing.T) {
		withCheckpointData(t, func(ctx context.Context, fs fileservice.FileService, db *testutil.TestEngine) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c := createTestConsumer(fs, db)
			assert.NotNil(t, c)
			defer c.Close()
			// set role
			c.common.setShardReplicaID(logShardID, 10)
			lc := c.logClient.(*mockLogClient)
			lc.setLeaderID(10)

			// write entries
			for i := 1; i <= 150; i++ {
				rec, fileName, err := genRecordWithLocation(0, uint64(i))
				assert.NoError(t, err)
				assert.NoError(t, c.srcFS.Write(ctx, fileservice.IOVector{
					FilePath: fileName,
					Entries: []fileservice.IOEntry{
						{
							Offset: 0,
							Size:   4,
							Data:   []byte("test"),
						},
					},
				}))
				lsn, err := c.upstreamLogClient.write(ctx, rec.Data)
				assert.NoError(t, err)
				assert.Equal(t, uint64(i), lsn)
			}

			err := c.completeData(ctx)
			assert.NoError(t, err)
			files, err := c.dstFS.List(ctx, "")
			assert.NoError(t, err)
			assert.Equal(t, 35+(150-102), len(files))
			requiredLsn, err := c.logClient.getRequiredLsn(ctx)
			assert.NoError(t, err)
			assert.Equal(t, uint64(103+(150-102)-1), requiredLsn)
			// don't update synced Lsn.
			assert.Equal(t, uint64(0), c.syncedLsn.Load())
		})
	})
}

func TestInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("log api error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		lc := c.logClient.(*mockLogClient)
		lc.fakeError("getTruncatedLsn")
		defer lc.clearFakeError("getTruncatedLsn")
		assert.Error(t, c.init(ctx))
	})

	t.Run("upstream log api error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		lc := c.upstreamLogClient.(*mockLogClient)
		lc.fakeError("getLatestLsn")
		defer lc.clearFakeError("getLatestLsn")
		assert.Error(t, c.init(ctx))
	})

	t.Run("ok", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		assert.NoError(t, c.init(ctx))
	})
}

func TestConsumerStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("cannot wait permission", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		cancel()
		c.Start(ctx)
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		c.Start(ctx)
	})

	t.Run("init failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		lc.fakeError("getTruncatedLsn")
		defer lc.clearFakeError("getTruncatedLsn")
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		c.Start(ctx)
	})

	t.Run("role changed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := createTestConsumer(nil, nil)
		assert.NotNil(t, c)
		defer c.Close()
		// set role
		c.common.setShardReplicaID(logShardID, 10)
		lc := c.logClient.(*mockLogClient)
		lc.setLeaderID(10)

		go func() {
			time.Sleep(time.Second)
			lc.setLeaderID(20)
			time.Sleep(time.Second)
			cancel()
		}()
		c.Start(ctx)
	})
}

func TestCreateConsumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := newTestFS()
	rt := runtime.DefaultRuntime()
	var writeLsn, syncedLsn atomic.Uint64
	w := newConsumer(
		*newCommon().withLog(rt.Logger()).withRuntime(rt),
		fs,
		&writeLsn,
		&syncedLsn,
	)
	assert.NotNil(t, w)
	w.Close()
}
