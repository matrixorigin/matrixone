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

func TestGroupSpillLargeGroups(t *testing.T) {
	cluster, err := embed.NewCluster(
		embed.WithCNCount(1),
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

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`USE test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS large_group_test (
			id BIGINT PRIMARY KEY,
			group_col1 INT,
			group_col2 VARCHAR(100),
			value_col BIGINT
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO large_group_test (id, group_col1, group_col2, value_col)
		SELECT 
			g.result as id,
			FLOOR(RAND() * 100000) as group_col1,  -- 100k distinct values
			CONCAT('group_', FLOOR(RAND() * 50000)) as group_col2,  -- 50k distinct values
			FLOOR(RAND() * 1000) as value_col
		FROM generate_series(1000000) g  -- 1M rows creating many distinct combinations
	`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT 
			group_col1, 
			group_col2, 
			COUNT(*) as cnt, 
			SUM(value_col) as sum_val
		FROM large_group_test
		GROUP BY group_col1, group_col2
		ORDER BY cnt DESC
		LIMIT 10
	`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	t.Logf("Successfully processed %d results from spilled group by operation", count)
}

func TestGroupSpillWithStrings(t *testing.T) {
	cluster, err := embed.NewCluster(
		embed.WithCNCount(1),
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

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`USE test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS string_group_test (
			id BIGINT PRIMARY KEY,
			category VARCHAR(50),
			description TEXT
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO string_group_test (id, category, description)
		SELECT 
			g.result as id,
			CONCAT('category_', FLOOR(RAND() * 10000)) as category,  -- 10k categories
			CONCAT('description_', g.result, '_long_text_data_for_spill_testing') as description
		FROM generate_series(500000) g  -- 500k rows
	`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT 
			category, 
			GROUP_CONCAT(description ORDER BY id SEPARATOR ', ') as concatenated_desc,
			COUNT(*) as cnt
		FROM string_group_test
		GROUP BY category
		HAVING cnt > 1
		ORDER BY cnt DESC
		LIMIT 5
	`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	t.Logf("Successfully processed %d results from string group concat spill operation", count)
}

func TestGroupSpillApproxCountDistinct(t *testing.T) {
	cluster, err := embed.NewCluster(
		embed.WithCNCount(1),
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

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`USE test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS approx_count_test (
			id BIGINT PRIMARY KEY,
			group_col VARCHAR(100),
			value_col BIGINT
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO approx_count_test (id, group_col, value_col)
		SELECT 
			g.result as id,
			CONCAT('group_', FLOOR(RAND() * 5000)) as group_col,  -- 5k groups
			FLOOR(RAND() * 1000000) as value_col  -- many distinct values per group
		FROM generate_series(800000) g  -- 800k rows
	`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT 
			group_col,
			APPROX_COUNT_DISTINCT(value_col) as approx_distinct_count,
			COUNT(*) as total_count
		FROM approx_count_test
		GROUP BY group_col
		ORDER BY approx_distinct_count DESC
		LIMIT 10
	`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	t.Logf("Successfully processed %d results from approx count distinct spill operation", count)
}

func TestGroupSpillMixedAggregations(t *testing.T) {
	cluster, err := embed.NewCluster(
		embed.WithCNCount(1),
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

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`USE test_spill`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS mixed_agg_test (
			id BIGINT PRIMARY KEY,
			dept VARCHAR(50),
			product VARCHAR(100),
			sales_amount DECIMAL(15,2),
			quantity INT,
			sale_date DATE
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO mixed_agg_test (id, dept, product, sales_amount, quantity, sale_date)
		SELECT 
			g.result as id,
			CONCAT('dept_', FLOOR(RAND() * 2000)) as dept,  -- 2k departments
			CONCAT('product_', FLOOR(RAND() * 50000)) as product,  -- 50k products
			RAND() * 1000 as sales_amount,
			FLOOR(RAND() * 100) as quantity,
			DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) as sale_date
		FROM generate_series(600000) g  -- 600k rows
	`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT 
			dept,
			product,
			COUNT(*) as transaction_count,
			SUM(sales_amount) as total_sales,
			AVG(sales_amount) as avg_sales,
			MIN(sales_amount) as min_sales,
			MAX(sales_amount) as max_sales,
			SUM(quantity) as total_quantity
		FROM mixed_agg_test
		GROUP BY dept, product
		ORDER BY total_sales DESC
		LIMIT 10
	`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	t.Logf("Successfully processed %d results from mixed aggregations spill operation", count)
}
