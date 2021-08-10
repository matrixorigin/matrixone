package test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/metadata"
	"testing"
	"time"
)

var (
	dbName    = "test_db1"
	tableName = "test_tb"
	cols      = []engine.TableDef{
		&engine.AttributeDef{
			Attr: metadata.Attribute{
				Name: "col1",
				Type: types.Type{},
				Alg:  0,
			},
		},
		&engine.AttributeDef{
			Attr: metadata.Attribute{
				Name: "col2",
				Type: types.Type{},
				Alg:  0,
			},
		},
	}
)

func TestCatalog(t *testing.T) {

	c, err := testutil.NewTestClusterStore(t, true, nil)

	defer c.Stop()

	time.Sleep(2 * time.Second)

	require.NoError(t, err)
	stdLog.Printf("app all started.")
	catalog := catalog2.DefaultCatalog(c.Applications[0])
	//testDBDDL(t, catalog)
	testTableDDL(t, catalog)
}

func testTableDDL(t *testing.T, c catalog2.Catalog) {
	tbs, err := c.GetTables(99)
	require.Error(t, catalog2.ErrDBNotExists, err)

	dbid, err := c.CreateDatabase(0, dbName, engine.AOE)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	tbs, err = c.GetTables(dbid)
	require.NoError(t, err)
	require.Nil(t, tbs)

	colCnt := 4
	t1 := md.MockTableInfo(colCnt)
	t1.Name = "t1"

	tid, err := c.CreateTable(1, dbid, *t1)
	require.NoError(t, err)
	require.Less(t, uint64(0), tid)

	tb, err := c.GetTable(dbid, t1.Name)
	require.NoError(t, err)
	require.NotNil(t, tb)
	require.Equal(t, aoe.StatePublic, tb.State)

	t2 := md.MockTableInfo(colCnt)
	t2.Name = "t2"
	_, err = c.CreateTable(2, dbid, *t2)
	require.NoError(t, err)

	tbls, err := c.GetTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 2, len(tbls))

	err = c.DelDatabase(3, dbName)
	require.NoError(t, err)

	dbs, err := c.GetDBs()
	require.NoError(t, err)
	require.Nil(t, dbs)

	kvs, err := c.Driver.Scan(codec.String2Bytes("DeletedTableQueue"), codec.String2Bytes("DeletedTableQueue10"), 0)
	require.NoError(t, err)
	for i := 0; i < len(kvs); i += 2 {
		tbl, err := helper.DecodeTable(kvs[i+1])
		require.NoError(t, err)
		require.Equal(t, uint64(3), tbl.Epoch)
	}

	cnt, err := c.RemoveDeletedTable(10)
	require.NoError(t, err)
	require.Equal(t, 2, cnt)

	cnt, err = c.RemoveDeletedTable(11)
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

	dbid, err = c.CreateDatabase(5, dbName, engine.AOE)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	t1.Name = "t1"
	for i := uint64(10); i < 20; i++ {
		t1.Name = fmt.Sprintf("t%d", i)
		tid, err := c.CreateTable(i, dbid, *t1)
		require.NoError(t, err)
		require.Less(t, uint64(0), tid)
	}

	tbls, err = c.GetTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 10, len(tbls))

	for i := uint64(10); i < 15; i++ {
		_, err = c.DropTable(20+i, dbid, fmt.Sprintf("t%d", i))
		require.NoError(t, err)
	}

	tbls, err = c.GetTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 5, len(tbls))

	cnt, err = c.RemoveDeletedTable(10)
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

	cnt, err = c.RemoveDeletedTable(33)
	require.NoError(t, err)
	require.Equal(t, 4, cnt)

	tablets, err := c.GetTablets(dbid, "t16")
	require.NoError(t, err)
	require.Less(t, 0, len(tablets))

}
