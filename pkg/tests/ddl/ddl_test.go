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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestCreateAndDropPitr(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			pitrName := "pitr_ut"

			// create database
			res, err := exec.Exec(
				ctx,
				"create database "+db,
				executor.Options{},
			)
			require.NoError(t, err)
			res.Close()

			// create pitr
			res, err = exec.Exec(
				ctx,
				"create pitr "+pitrName+" for database "+db+" range 1 'd'",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			// drop pitr
			res, err = exec.Exec(
				ctx,
				"drop pitr "+pitrName,
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()
		},
	)
}

func TestPitrCases(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			table := "table01"
			pitr1 := "pitr01"
			pitr2 := "pitr02"
			pitr3 := "pitr03"

			// create database and table
			res, err := exec.Exec(ctx, "create database "+db, executor.Options{})
			require.NoError(t, err)
			res.Close()
			res, err = exec.Exec(ctx, "create table "+table+" (col1 int)", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// create pitr with different units
			for _, unit := range []string{"h", "d", "mo", "y"} {
				name := "pitr_" + unit
				res, err = exec.Exec(ctx, "drop pitr if exists "+name+" internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
				res, err = exec.Exec(ctx, "create pitr "+name+" for database "+db+" range 1 '"+unit+"' internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
				res, err = exec.Exec(ctx, "drop pitr if exists "+name+" internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
			}

			// create pitr for table
			res, err = exec.Exec(ctx, "drop pitr if exists "+pitr1+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()
			res, err = exec.Exec(ctx, "create pitr "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// create pitr with if not exists
			res, err = exec.Exec(ctx, "create pitr if not exists "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// error: duplicate create
			_, err = exec.Exec(ctx, "create pitr "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// error: invalid unit
			_, err = exec.Exec(ctx, "create pitr "+pitr2+" for database "+db+" range 1 'yy' internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// drop pitr
			res, err = exec.Exec(ctx, "drop pitr "+pitr1+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// drop non-existent pitr (should error)
			_, err = exec.Exec(ctx, "drop pitr "+pitr3+" internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// drop non-existent pitr with if exists (should not error)
			res, err = exec.Exec(ctx, "drop pitr if exists "+pitr3+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// show pitr
			res, err = exec.Exec(ctx, "show pitr", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			rowCount := 0
			for _, b := range res.Batches {
				rowCount += b.RowCount()
			}
			require.Equal(t, rowCount, 0)
			res.Close()
		},
	)
}
