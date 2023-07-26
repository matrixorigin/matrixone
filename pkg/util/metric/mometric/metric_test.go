// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mometric

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/metric"

	"github.com/matrixorigin/matrixone/pkg/config"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetric(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)

	runtime.SetupProcessLevelRuntime(runtime.NewRuntime(metadata.ServiceType_CN, "test", logutil.GetGlobalLogger()))

	withModifiedConfig(func() {
		SV := config.NewObservabilityParameters()
		SV.SetDefaultValues("test")
		SV.Host = "0.0.0.0"
		SV.StatusPort = 7001
		SV.EnableMetricToProm = true
		SV.MetricExportInterval = 1
		defer metric.SetGatherInterval(metric.SetGatherInterval(30 * time.Millisecond))
		defer metric.SetRawHistBufLimit(metric.SetRawHistBufLimit(5))
		InitMetric(context.TODO(), factory, SV, "node_uuid", "test", WithInitAction(true))
		defer StopMetricSync()

		const (
			none       = "--None"
			createDB   = "create database"
			createTbl  = "CREATE TABLE"
			createView = "CREATE VIEW"
			insertRow  = "insert into"
		)
		prevSqlKind := none
		for sql := range sqlch {
			t.Logf("sql: %s", sql)
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
				require.True(t, strings.HasPrefix(sql, createView), "income sql: %s", sql)
				prevSqlKind = createView
			case createView:
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
		require.Contains(t, string(content), "# HELP") // check we have metrics output
	})
}

func TestMetricNoProm(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)

	withModifiedConfig(func() {
		SV := config.NewObservabilityParameters()
		SV.SetDefaultValues("test")
		SV.Host = "0.0.0.0"
		SV.StatusPort = 7001
		SV.EnableMetricToProm = false

		defer metric.SetGatherInterval(metric.SetGatherInterval(30 * time.Millisecond))
		defer metric.SetRawHistBufLimit(metric.SetRawHistBufLimit(5))
		InitMetric(context.TODO(), factory, SV, "node_uuid", "test", WithInitAction(true))
		defer StopMetricSync()

		client := http.Client{
			Timeout: 120 * time.Second,
		}
		_, err := client.Get("http://127.0.0.1:7001/metrics")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "connection refused")

		// make static-check(errcheck) happay
		SV.EnableMetricToProm = true
	})
}

func TestDescExtra(t *testing.T) {
	desc := prom.NewDesc("sys_xxx_yyy_FEFA", "help info", []string{"is_internal", "xy"}, map[string]string{"node": "1"})
	extra := newDescExtra(desc)
	assert.Equal(t, extra.fqName, "sys_xxx_yyy_FEFA")
	assert.Equal(t, extra.labels[0].GetName(), "is_internal")
	assert.Equal(t, extra.labels[1].GetName(), "node")
	assert.Equal(t, extra.labels[2].GetName(), "xy")
}

func TestGetSchemaForAccount(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name     string
		args     args
		wantPath string
		wantSche int
	}{
		{
			name: "test_account_user1",
			args: args{
				account: "user1",
			},
			wantPath: "CREATE TABLE IF NOT EXISTS",
			wantSche: 7,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemas := GetSchemaForAccount(ctx, tt.args.account)
			found := false

			if strings.Contains(schemas[0], tt.wantPath) {
				found = true
			}
			require.Equal(t, true, found)
		})
	}
}
