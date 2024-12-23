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

package dml

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestDeleteAndSelect(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			table := "t"

			res, err := exec.Exec(
				ctx,
				"create database "+db,
				executor.Options{},
			)
			require.NoError(t, err)
			res.Close()

			res, err = exec.Exec(
				ctx,
				"create table "+table+" (a varchar primary key, b varchar)",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			//insert 513 blocks into t;
			res, err = exec.Exec(
				ctx,
				"insert into "+table+" select *, * from generate_series(1,4202496)g",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			res, err = exec.Exec(
				ctx,
				"delete from "+table+" where a > 3",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			//select b from t2 where a between 1 and 3 order by b asc;
			res, err = exec.Exec(
				ctx,
				"select b from "+table+" where a between 1 and 3 order by b asc",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			rows := 0
			for _, b := range res.Batches {
				rows += b.RowCount()
			}
			require.Equal(t, 3, rows)
			res.Close()
		},
	)

}
