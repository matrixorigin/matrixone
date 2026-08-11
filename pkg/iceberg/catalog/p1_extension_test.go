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

package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestRESTClientPlanScanExtension(t *testing.T) {
	var captured planScanRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/warehouse_a/namespaces/sales%1Fgold/tables/orders/plan-scan" {
			t.Fatalf("unexpected plan-scan path: %s", r.URL.EscapedPath())
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(planScanResponseWire{
			Snapshot: api.SnapshotPlan{SnapshotID: 44, SchemaID: 7, PlanningMode: "server-side"},
			DataTasks: []api.DataFileTask{{
				DataFile:  api.DataFile{FilePath: "s3://warehouse/orders/data/part-0.parquet", FileFormat: "parquet", RecordCount: 10, FileSizeInBytes: 100},
				RowGroups: []api.RowGroupSplit{{Ordinal: 2, StartRowOrdinal: 200, RowCount: 10}},
			}},
			DeleteTasks: []api.DeleteFileTask{{
				DataFile:      api.DataFile{Content: api.DataFileContentPositionDelete, FilePath: "s3://warehouse/orders/delete/part-0.parquet"},
				AppliesToPath: "s3://warehouse/orders/data/part-0.parquet",
			}},
			ColumnMapping:       []api.IcebergColumnMapping{{FieldID: 1, ColumnName: "id", Projected: true}},
			Profile:             api.PlanningProfile{PlanningMode: "server-side", DataFilesSelected: 1},
			PredicateEquivalent: true,
		})
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	plan, err := client.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales", "gold"},
		Table:          "orders",
		Ref:            "main",
		ProjectionIDs:  []int{1},
		PrunePredicates: []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGT,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 10},
		}},
		EnableRowGroupPlanning: true,
		EnableDeleteApply:      true,
		DeleteMaxMemoryBytes:   1024,
	})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if captured.Identifier.Name != "orders" || strings.Join(captured.Identifier.Namespace, ".") != "sales.gold" {
		t.Fatalf("unexpected captured identifier: %+v", captured.Identifier)
	}
	if !captured.EnableRowGroupPlanning || !captured.EnableDeleteApply || captured.DeleteMaxMemoryBytes != 1024 {
		t.Fatalf("missing planning flags: %+v", captured)
	}
	if len(plan.DataTasks) != 1 || plan.Snapshot.SnapshotID != 44 || plan.Profile.PlanningMode != "server-side" {
		t.Fatalf("unexpected scan plan: %+v", plan)
	}
	if len(plan.DeleteTasks) != 1 || len(plan.DataTasks[0].RowGroups) != 1 || !plan.ServerPredicateEquivalent {
		t.Fatalf("server plan did not preserve delete tasks, row groups, and predicate equivalence: %+v", plan)
	}
}

func TestRESTClientReportsMetrics(t *testing.T) {
	var captured metricsReportRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables/orders/metrics" {
			t.Fatalf("unexpected metrics path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode metrics request: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	err := client.ReportMetrics(context.Background(), api.MetricsReportRequest{
		CatalogRequest:       api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:            api.Namespace{"sales"},
		Table:                "orders",
		Ref:                  "main",
		SnapshotID:           44,
		QueryID:              "q-1",
		Kind:                 api.MetricsReportScan,
		PlanningProfile:      api.PlanningProfile{DataFilesSelected: 3},
		MetadataLocationHash: "hash-1",
		Rows:                 10,
		Files:                3,
		Extra:                map[string]string{"engine": "matrixone"},
	})
	if err != nil {
		t.Fatalf("report metrics: %v", err)
	}
	if captured.Identifier.Name != "orders" || captured.Kind != api.MetricsReportScan || captured.Rows != 10 || captured.Extra["engine"] != "matrixone" {
		t.Fatalf("unexpected captured metrics: %+v", captured)
	}
}

func TestRESTCatalogCompatibilityProfilesUseStandardPlanScanSurface(t *testing.T) {
	profiles := []struct {
		name     string
		basePath string
		prefix   string
	}{
		{name: "nessie", basePath: "/iceberg", prefix: "warehouse"},
		{name: "polaris-open-catalog", basePath: "", prefix: "prod"},
		{name: "gravitino", basePath: "/catalog", prefix: "lakehouse"},
	}
	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wantSuffix := "/v1/" + profile.prefix + "/namespaces/sales/tables/orders/plan-scan"
				if !strings.HasSuffix(r.URL.Path, wantSuffix) {
					t.Fatalf("unexpected %s path: %s want suffix %s", profile.name, r.URL.Path, wantSuffix)
				}
				_ = json.NewEncoder(w).Encode(planScanResponseWire{
					Snapshot: api.SnapshotPlan{SnapshotID: 1, SchemaID: 1},
					Profile:  api.PlanningProfile{PlanningMode: "server-side"},
				})
			}))
			defer server.Close()
			client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
			cat := testCatalog(server.URL + profile.basePath)
			_, err := client.PlanScan(context.Background(), api.ScanPlanRequest{
				CatalogRequest: api.CatalogRequest{Catalog: cat, Prefix: profile.prefix},
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
			})
			if err != nil {
				t.Fatalf("%s plan scan: %v", profile.name, err)
			}
		})
	}
}

func TestAdapterFeasibilityMatrix(t *testing.T) {
	for _, typ := range []string{"glue", "hive", "unity-catalog"} {
		item, ok := AdapterFeasibilityForType(typ)
		if !ok || !item.FacadeReusable || item.Status == "" {
			t.Fatalf("missing feasibility for %s: %+v ok=%v", typ, item, ok)
		}
	}
	icebergGo := IcebergGoAdapterFeasibility()
	if icebergGo.Name != AdapterIcebergGo || !icebergGo.RequiresBuildTag || !icebergGo.FacadeReusable {
		t.Fatalf("unexpected iceberg-go feasibility: %+v", icebergGo)
	}
}
