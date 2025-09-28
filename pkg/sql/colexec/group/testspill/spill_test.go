// Copyright 2025 Matrix Origin
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

package testspill

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSpill(t *testing.T) {

	// start cluster
	cluster, err := embed.NewCluster(
		embed.WithCNCount(3),
		embed.WithTesting(),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			service.Adjust(func(config *embed.ServiceConfig) {
				config.Log.Level = "debug"
			})
		}),
	)
	require.NoError(t, err)
	err = cluster.Start()
	require.NoError(t, err)
	defer cluster.Close()

	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
		cn0.GetServiceConfig().CN.Frontend.Port,
	)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// database
	_, err = db.Exec(` create database test `)
	require.NoError(t, err)

	// table
	_, err = db.Exec(`
		use test;
		create table sales (
			id int,
			product_id int,
			customer_id int,
			sale_date date,
			amount decimal(10, 2)
		);
		`)
	require.NoError(t, err)

	// data
	_, err = db.Exec(`
		use test;
		insert into sales (id, product_id, customer_id, sale_date, amount)
		select g.result as id,
		floor(1 + (rand() * 100000)) as product_id,
		floor(1 + (rand() * 100000)) as customer_id,
		current_date - interval floor(rand() * 365) day as sale_date,
		floor(rand() * 1000) as amount
		from generate_series(2000 * 10000) g
		`,
	)
	require.NoError(t, err)

	// query
	var count int
	err = db.QueryRow(`
		select count(sha2(product_id * customer_id, 256))
		from sales
		`,
	).Scan(&count)
	require.NoError(t, err)
	t.Logf("count: %v", count)

}
