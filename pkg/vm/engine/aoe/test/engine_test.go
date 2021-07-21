package test

import (
	"github.com/stretchr/testify/require"
	stdLog "log"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"testing"
	"time"
)

var (
	testDBName = "db1"
	testTableName = "t1"
	colCnt = 4
)

func TestAOEEngine(t *testing.T) {

	c, err := testutil.NewTestClusterStore(t)
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

	err = db.Delete(testTableName)
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 0, len(tbls))

}


