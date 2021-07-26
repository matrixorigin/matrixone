package test

import (
	"bytes"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/protocol"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"
	"time"
)


const (
	testDBName = "db1"
	testTableName = "t1"
	colCnt = 4
)

func TestAOEEngine(t *testing.T) {

	c, err := testutil.NewTestClusterStore(t, true, func(path string) (storage.DataStorage, error) {
		opts     := &e.Options{}
		mdCfg := &md.Configuration{
			Dir:              path,
			SegmentMaxBlocks: blockCntPerSegment,
			BlockMaxRows:     blockRows,
		}
		opts.CacheCfg = &e.CacheCfg{
			IndexCapacity:  blockRows * blockCntPerSegment * 80,
			InsertCapacity: blockRows * uint64(colCnt) * 2000,
			DataCapacity:   blockRows * uint64(colCnt) * 2000,
		}
		opts.MetaCleanerCfg = &e.MetaCleanerCfg{
			Interval: time.Duration(1) * time.Second,
		}
		opts.Meta.Conf = mdCfg
		return daoe.NewStorageWithOptions(path, opts)
	})
	require.NoError(t, err)
	defer c.Stop()

	time.Sleep(2 * time.Second)

	require.NoError(t, err)
	stdLog.Printf("app all started.")

	catalog := catalog2.DefaultCatalog(c.Applications[0])
	aoeEngine := engine.Mock(&catalog)

	err = aoeEngine.Create(testDBName, 0)
	require.NoError(t, err)

	dbs := aoeEngine.Databases()
	require.Equal(t, 1, len(dbs))

	err = aoeEngine.Delete(testDBName)
	require.NoError(t, err)

	dbs = aoeEngine.Databases()
	require.Equal(t, 0, len(dbs))

	_, err = aoeEngine.Database(testDBName)
	require.NotNil(t, err)

	err = aoeEngine.Create(testDBName, 0)
	require.NoError(t, err)
	db, err := aoeEngine.Database(testDBName)
	require.NoError(t, err)

	tbls := db.Relations()
	require.Equal(t, 0, len(tbls))

	mockTbl := md.MockTableInfo(colCnt)
	mockTbl.Name = testTableName
	_, _, _, _, comment, defs, pdef, _ := helper.UnTransfer(*mockTbl)
	err = db.Create(testTableName, defs, pdef, nil, comment)
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 1, len(tbls))

	tb, err := db.Relation(mockTbl.Name)
	require.NoError(t, err)
	require.Equal(t, tb.ID(), mockTbl.Name)
	stdLog.Printf("[QQQ]target table is %s", tb.ID())

	attrs := helper.Attribute(*mockTbl)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	ibat := chunk.MockBatch(typs, batchInsertRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	stdLog.Printf("[QQQ]size of batch is  %d", buf.Len())

	err = tb.Write(ibat)
	require.NoError(t, err)


	err = db.Delete(testTableName)
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 0, len(tbls))


	time.Sleep(3 * time.Second )


}


