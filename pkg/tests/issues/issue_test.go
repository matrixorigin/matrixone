package issues

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestWWConflict(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			db := getDatabaseName(t)
			table := "t"

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			cn2, err := c.GetCNService(1)
			require.NoError(t, err)

			createTestDatabase(t, db, cn1)
			waitDatabaseCreated(t, db, cn2)

			execDDL(
				t,
				db,
				"create table "+getFullTableName(db, table)+" (id int primary key, v int)",
				cn1,
			)

			waitTableCreated(
				t,
				db,
				table,
				cn2,
			)
		},
	)
}
