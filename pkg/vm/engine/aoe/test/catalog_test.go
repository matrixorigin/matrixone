package test

import (
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
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

	c, err := testutil.NewTestClusterStore(t)

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

	dbid, err := c.CreateDatabase(dbName, engine.AOE)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	tbs, err = c.GetTables(dbid)
	require.NoError(t, err)
	require.Nil(t, tbs)


	colCnt := 4
	tableInfo := md.MockTableInfo(colCnt)
	tableInfo.Id = 101

	tid, err := c.CreateTable(dbid, *tableInfo)

	require.NoError(t, err)
	require.Less(t, uint64(0), tid)

	completedC := make(chan *aoe.TableInfo, 1)
	defer close(completedC)
	go func() {
		i := 0
		for {
			tb, _ := c.GetTable(dbid, tableInfo.Name)
			if tb != nil {
				completedC <- tb
				break
			}
			i += 1
		}
	}()
	select {
	case <-completedC:
		stdLog.Printf("[QSQ], create %s finished", tableInfo.Name)
		break
	case <-time.After(3 * time.Second):
		stdLog.Printf("[QSQ], create %s failed, timeout", tableInfo.Name)
	}
	tb, err := c.GetTable(dbid, tableInfo.Name)
	require.NoError(t, err)
	require.NotNil(t, tb)
	require.Equal(t, aoe.StatePublic, tb.State)




}
func testDBDDL(t *testing.T, c catalog2.Catalog) {
	dbs, err := c.GetDBs()
	require.NoError(t, err)
	require.Nil(t, dbs)

	id, err := c.CreateDatabase(dbName, engine.AOE)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)

	id, err = c.CreateDatabase(dbName, engine.AOE)
	require.Equal(t, catalog2.ErrDBCreateExists, err)

	dbs, err = c.GetDBs()
	require.NoError(t, err)
	require.Equal(t, 1, len(dbs))

	db, err := c.GetDB(dbName)
	require.NoError(t, err)
	require.Equal(t, dbName, db.Name)

	id, err = c.DelDatabase(dbName)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)

	db, err = c.GetDB(dbName)
	require.Error(t, catalog2.ErrDBNotExists, err)
}