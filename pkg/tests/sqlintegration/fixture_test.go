// Copyright 2024 Matrix Origin
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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// These tests share the canonical one-CN configuration and run sequentially.
// SharedTestCluster owns failure quarantine after scenario cleanup; this
// wrapper only records the scenario's SQL execution time.
func runSQLIntegration(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	embed.RunSingleCNBaseClusterTests(t, func(c embed.Cluster) {
		started := time.Now()
		defer func() {
			status := "ready"
			if t.Failed() {
				status = "error"
			}
			t.Logf("MO_UT_SETUP fixture=sql-integration phase=scenario-body duration=%s status=%s", time.Since(started), status)
		}()
		fn(c)
	})
}

// The UT runner may split roots into fresh processes: this TestMain only
// closes the canonical fixture and propagates cleanup failure. It must not
// accumulate cross-root assertions or depend on another root having executed.
func TestMain(m *testing.M) {
	code := m.Run()
	if err := embed.CloseSingleCNBaseClusterTests(); err != nil {
		fmt.Fprintf(os.Stderr, "close SQL integration fixture: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

// Run synchronously inside the scenario, before releasing the fixture mutex.
// A fresh context lets cleanup finish even when the scenario timed out.
func cleanupSQLIntegration(t *testing.T, cn embed.ServiceOperator, statements ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	if err != nil {
		t.Errorf("open SQL integration cleanup connection: %v", err)
		return
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Errorf("SQL integration cleanup %q: %v", statement, err)
		}
	}
}
