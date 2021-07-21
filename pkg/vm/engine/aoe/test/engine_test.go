package test

import (
	"github.com/stretchr/testify/require"
	stdLog "log"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	"testing"
	"time"
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

	err = aoeEngine.Create("testdb", 0)

	require.NoError(t, err)

	dbs := aoeEngine.Databases()

	require.Equal(t, 1, len(dbs))

	err = aoeEngine.Delete("testdb")
	require.NoError(t, err)

	dbs1 := aoeEngine.Databases()
	require.Equal(t, 0, len(dbs1))

}


