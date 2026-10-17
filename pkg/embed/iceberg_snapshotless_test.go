// Copyright 2026 Matrix Origin
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

package embed

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/stretchr/testify/require"
)

type snapshotlessSQLPlanner struct {
	api.ScanPlanner
	objectRef string
}

func (p snapshotlessSQLPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	plan, err := p.ScanPlanner.PlanScan(ctx, req)
	if err == nil {
		plan.ObjectIORef = p.objectRef
	}
	return plan, err
}

type snapshotlessObjectReader struct{ calls atomic.Int64 }

func (r *snapshotlessObjectReader) Read(context.Context, string, int64, int64) ([]byte, error) {
	r.calls.Add(1)
	return nil, fmt.Errorf("snapshot-less scan must not read objects")
}

// #30435: reuse the existing cluster, tenant and access fixture, but execute
// the original schema and merge-on-read query through MySQL and the native planner.
func runIcebergSQLEngineEmbeddedSnapshotlessTable(t *testing.T, c Cluster, db *embeddedSQLSession, fixture *embeddedIcebergFixture) {
	wireDB := openIcebergTenantFrontendDB(t, c, db.accountName)
	defer wireDB.Close()
	for _, current := range []string{"-1", "null", "omitted"} {
		t.Run(current, func(t *testing.T) {
			currentField := `"current-snapshot-id": ` + current + `,`
			if current == "omitted" {
				currentField = ""
			}
			payload := []byte(fmt.Sprintf(`{
				"format-version": 2, "location": "s3://warehouse/embed/empty",
				"current-schema-id": 0, "schemas": [{"schema-id": 0, "fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "hidden_key", "required": false, "type": "long"},
					{"id": 3, "name": "bucket", "required": false, "type": "int"},
					{"id": 4, "name": "amount", "required": false, "type": "long"}
				]}], "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}],
				%s "snapshots": []
			}`, currentField))
			reader := &snapshotlessObjectReader{}
			native := icebergmetadata.LocalScanPlanner{
				Metadata: icebergmetadata.NativeFacade{}, ObjectReader: reader,
				Catalog: &catalog.MockClient{LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
					return &api.LoadTableResponse{MetadataLocation: "s3://warehouse/embed/empty/metadata.json", MetadataJSON: payload}, nil
				}},
			}
			installEmbeddedIcebergPlanner(t, c, snapshotlessSQLPlanner{ScanPlanner: native, objectRef: fixture.objectRef})
			table := fixture.databaseName + ".empty_orders"
			mustExecSQL(t, db, fmt.Sprintf("create external table %s (id bigint, hidden_key bigint, bucket int, amount bigint) engine = iceberg with ('catalog'='%s','namespace'='sales','table'='empty_orders','ref'='main','read_mode'='merge_on_read','write_mode'='merge_on_read')", table, fixture.catalogName))
			t.Cleanup(func() { mustExecSQL(t, db, "drop table "+table) })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var count int64
			require.NoError(t, wireDB.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
			require.Zero(t, count)
			stmt, err := wireDB.PrepareContext(ctx, "select count(*) from "+table)
			require.NoError(t, err)
			defer stmt.Close()
			for range 2 {
				require.NoError(t, stmt.QueryRowContext(ctx).Scan(&count))
				require.Zero(t, count)
			}
			rows, err := wireDB.QueryContext(ctx, "select id, hidden_key, bucket, amount from "+table+" where id > 0")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"id", "hidden_key", "bucket", "amount"}, columns)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.Zero(t, reader.calls.Load())
		})
	}
}
