// Copyright 2022 Matrix Origin
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
package metric

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetric(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)

	withModifiedConfig(func() {
		pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, config.ClusterCatalog)
		_ = pu.SV.SetHost("0.0.0.0")
		_ = pu.SV.SetStatusPort(7001)
		_ = pu.SV.SetMetricToProm(true)
		defer setGatherInterval(setGatherInterval(30 * time.Millisecond))
		defer setRawHistBufLimit(setRawHistBufLimit(5))
		InitMetric(factory, pu, 0, "test")
		defer StopMetricSync()

		const (
			none      = "--None"
			createDB  = "create database"
			createTbl = "create table"
			insertRow = "insert into"
		)
		prevSqlKind := none
		for sql := range sqlch {
			if strings.HasPrefix(sql, prevSqlKind) {
				continue
			}
			switch prevSqlKind {
			case none:
				require.True(t, strings.HasPrefix(sql, createDB), "income sql: %s", sql)
				prevSqlKind = createDB
			case createDB:
				require.True(t, strings.HasPrefix(sql, createTbl), "income sql: %s", sql)
				prevSqlKind = createTbl
			case createTbl:
				require.True(t, strings.HasPrefix(sql, insertRow), "income sql: %s", sql)
				goto GOON
			default:
				require.True(t, false, "unknow sql kind %s", sql)
			}
		}
	GOON:
		client := http.Client{
			Timeout: 120 * time.Second,
		}
		r, err := client.Get("http://127.0.0.1:7001/metrics")
		require.Nil(t, err)
		require.Equal(t, r.StatusCode, 200)

		content, _ := io.ReadAll(r.Body)
		require.Contains(t, string(content), "sql_latency_seconds")
	})
}

func TestMetricNoProm(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)

	withModifiedConfig(func() {
		pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, config.ClusterCatalog)
		_ = pu.SV.SetHost("0.0.0.0")
		_ = pu.SV.SetStatusPort(7001)
		_ = pu.SV.SetMetricToProm(false)

		defer setGatherInterval(setGatherInterval(30 * time.Millisecond))
		defer setRawHistBufLimit(setRawHistBufLimit(5))
		InitMetric(factory, pu, 0, "test")
		defer StopMetricSync()

		client := http.Client{
			Timeout: 120 * time.Second,
		}
		_, err := client.Get("http://127.0.0.1:7001/metrics")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "connection refused")

		// make static-check(errcheck) happay
		_ = pu.SV.SetMetricToProm(true)
	})
}

func TestCreateTable(t *testing.T) {
	buf := new(bytes.Buffer)
	name := "sql_test_counter"
	l1, v1 := "time", "12"
	counterV := 123.0
	sql := createTableSqlFromMetricFamily(&dto.MetricFamily{
		Name: &name,
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			{Label: []*dto.LabelPair{{Name: &l1, Value: &v1}}, Counter: &dto.Counter{Value: &counterV}},
		},
	}, buf)
	assert.Equal(t, sql, fmt.Sprintf(
		"create table if not exists %s.%s (`%s` datetime, `%s` double, `%s` int, `%s` varchar(20), `time` varchar(20))",
		METRIC_DB, name, LBL_TIME, LBL_VALUE, LBL_NODE, LBL_ROLE,
	))

	sql = createTableSqlFromMetricFamily(&dto.MetricFamily{
		Name:   &name,
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: &counterV}}},
	}, buf)

	assert.Equal(t, sql, fmt.Sprintf(
		"create table if not exists %s.%s (`%s` datetime, `%s` double, `%s` int, `%s` varchar(20))",
		METRIC_DB, name, LBL_TIME, LBL_VALUE, LBL_NODE, LBL_ROLE,
	))
}
