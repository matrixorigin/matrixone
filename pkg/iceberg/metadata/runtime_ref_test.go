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

package metadata

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

func TestRuntimeScanPlannerTypedReadRefs(t *testing.T) {
	for _, env := range []struct {
		name     string
		nessie   bool
		supplied string
	}{
		{name: "Nessie", nessie: true},
		{name: "generic REST"},
		{name: "supplied prefix", nessie: true, supplied: "caller|s3://warehouse"},
	} {
		for _, ref := range []string{"main", "branch:main", "snapshot:11"} {
			t.Run(env.name+"/"+ref, func(t *testing.T) {
				fixture, planner := newRuntimeRefTestPlanner(t, sampleMetadataJSON)
				configs := 0
				fixture.client.GetConfigFunc = func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
					configs++
					return &api.ConfigResponse{Prefix: "main|s3://warehouse", Overrides: map[string]string{
						"nessie.is-nessie-catalog": map[bool]string{true: "true", false: "false"}[env.nessie],
					}}, nil
				}
				var credentialPrefix string
				fixture.client.LoadCredentialsFunc = func(_ context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
					credentialPrefix = req.Prefix
					return &api.LoadCredentialsResponse{}, nil
				}
				var readerReq api.ScanPlanRequest
				planner.ObjectReader = func(ctx context.Context, client api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
					readerReq = req
					_, err := client.LoadCredentials(ctx, api.LoadCredentialsRequest{CatalogRequest: req.CatalogRequest, Namespace: req.Namespace, Table: req.Table})
					return fixture.reader, ObjectReaderContext{}, err
				}
				var tableReq api.LoadTableRequest
				load := fixture.client.LoadTableFunc
				fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
					tableReq = req
					return load(ctx, req)
				}
				req := runtimeRefTestRequest(ref)
				req.Prefix = env.supplied
				original := req
				plan, err := planner.PlanScan(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, original, req, "normalization must remain request scoped")
				wantPrefix := "main|s3://warehouse"
				if env.supplied != "" {
					wantPrefix = env.supplied
					require.Zero(t, configs, "a caller-supplied prefix is authoritative")
				} else {
					require.Equal(t, 1, configs)
				}
				require.Equal(t, wantPrefix, credentialPrefix)
				require.Equal(t, wantPrefix, tableReq.Prefix)
				require.Equal(t, model.DefaultRefMain, readerReq.Ref)
				require.Equal(t, model.DefaultRefMain, readerReq.Snapshot.RefName)
				require.Equal(t, ref, plan.Snapshot.RefName, "display labels are separate from wire selectors")
				if ref == "snapshot:11" {
					require.True(t, readerReq.Snapshot.HasSnapshotID)
					require.Equal(t, int64(11), readerReq.Snapshot.SnapshotID)
					require.Equal(t, "all", tableReq.Snapshots)
					require.Equal(t, int64(11), plan.Snapshot.SnapshotID)
				} else {
					require.False(t, readerReq.Snapshot.HasSnapshotID)
					require.Equal(t, "main", tableReq.Snapshots)
					require.Equal(t, int64(22), plan.Snapshot.SnapshotID)
				}
			})
		}
	}
}

func TestRuntimeScanPlannerReadRefPrecedenceAndRouting(t *testing.T) {
	metadataJSON := strings.Replace(sampleMetadataJSON, `"refs": {`, `"refs": {"snapshot:11": {"snapshot-id": 22, "type": "branch"},`, 1)
	for _, tc := range []struct {
		name, ref, selectorRef, prefix, configPrefix string
		nessie                                       bool
		snapshot                                     api.SnapshotSelector
		wantPrefix, wantRef                          string
		wantID                                       int64
		wantCode                                     api.ErrorCode
	}{
		{name: "selector only", selectorRef: "branch:main", wantRef: "main", wantID: 22},
		{name: "request only", ref: "branch:main", wantRef: "main", wantID: 22},
		{name: "request wins conflicting selector", ref: "main", selectorRef: "snapshot:11", wantRef: "main", wantID: 22},
		{name: "snapshot ID overrides default pin", ref: "snapshot:11", snapshot: api.SnapshotSelector{HasSnapshotID: true, SnapshotID: 22}, wantRef: "main", wantID: 22},
		{name: "timestamp overrides default pin", ref: "snapshot:22", snapshot: api.SnapshotSelector{HasTimestampMS: true, TimestampMS: 1710000000000}, wantRef: "main", wantID: 11},
		{name: "explicit ID precedes timestamp", ref: "snapshot:11", snapshot: api.SnapshotSelector{HasSnapshotID: true, SnapshotID: 22, HasTimestampMS: true, TimestampMS: 1710000000000}, wantRef: "main", wantID: 22},
		{name: "shared parser whitespace and case", ref: "  SnApShOt : 11  ", wantRef: "main", wantID: 11},
		{name: "branch name parsed once", ref: "branch:snapshot:11", wantRef: "snapshot:11", wantID: 22},
		{name: "Nessie branch name parsed once", ref: "branch:snapshot:11", nessie: true, configPrefix: "main|warehouse", wantPrefix: "snapshot:11|warehouse", wantID: 22},
		{name: "generic tag", ref: "tag:audit", wantRef: "audit", wantID: 11},
		{name: "generic hash keeps existing lookup", ref: "hash:abcdef", wantRef: "hash:abcdef", wantCode: api.ErrTableNotFound},
		{name: "unknown typed prefix remains a name", ref: "custom:missing", wantRef: "custom:missing", wantCode: api.ErrTableNotFound},
		{name: "supplied opaque prefix", ref: "tag:audit", prefix: "opaque-route", nessie: true, wantPrefix: "opaque-route", wantRef: "audit", wantID: 11},
		{name: "Nessie opaque prefix is not rewritten", ref: "tag:audit", nessie: true, configPrefix: "opaque-route", wantPrefix: "opaque-route", wantRef: "audit", wantID: 11},
		{name: "typed main leaves configured catalog ref", ref: "branch:main", nessie: true, configPrefix: "dev|warehouse", wantPrefix: "dev|warehouse", wantRef: "main", wantID: 22},
		{name: "Nessie branch with explicit snapshot", ref: "branch:dev", nessie: true, configPrefix: "main|warehouse", snapshot: api.SnapshotSelector{HasSnapshotID: true, SnapshotID: 11}, wantPrefix: "dev|warehouse", wantID: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, planner := newRuntimeRefTestPlanner(t, metadataJSON)
			fixture.client.GetConfigFunc = func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
				return &api.ConfigResponse{Prefix: tc.configPrefix, Overrides: map[string]string{
					"nessie.is-nessie-catalog": map[bool]string{true: "true", false: "false"}[tc.nessie],
				}}, nil
			}
			var readerReq api.ScanPlanRequest
			planner.ObjectReader = func(_ context.Context, _ api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
				readerReq = req
				return fixture.reader, ObjectReaderContext{}, nil
			}
			req := runtimeRefTestRequest(tc.ref)
			req.Prefix = tc.prefix
			req.Snapshot = tc.snapshot
			req.Snapshot.RefName = tc.selectorRef
			plan, err := planner.PlanScan(context.Background(), req)
			require.Equal(t, tc.wantPrefix, readerReq.Prefix)
			require.Equal(t, tc.wantRef, readerReq.Ref)
			selectorRef := tc.wantRef
			if selectorRef == "" { // A successfully routed Nessie ref selects table main.
				selectorRef = model.DefaultRefMain
			}
			require.Equal(t, selectorRef, readerReq.Snapshot.RefName)
			if tc.wantCode != "" {
				var iceErr *api.IcebergError
				require.ErrorAs(t, err, &iceErr)
				require.Equal(t, tc.wantCode, iceErr.Code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantID, plan.Snapshot.SnapshotID)
			if strings.HasPrefix(tc.ref, "branch:snapshot:") {
				require.False(t, readerReq.Snapshot.HasSnapshotID, "a branch name must not become a snapshot pin")
			}
		})
	}
}

func TestRuntimeScanPlannerReadRefErrors(t *testing.T) {
	for _, raw := range []string{"branch:", "tag: ", "hash: ", "snapshot:", "snapshot:bad", "snapshot:9223372036854775808"} {
		t.Run(raw, func(t *testing.T) {
			fixture, planner := newRuntimeRefTestPlanner(t, sampleMetadataJSON)
			fixture.client.GetConfigFunc = func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
				t.Fatal("invalid typed ref must fail before catalog IO")
				return nil, nil
			}
			planner.ObjectReader = func(context.Context, api.CatalogClient, api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
				t.Fatal("invalid typed ref must fail before provider creation")
				return nil, ObjectReaderContext{}, nil
			}
			_, err := planner.PlanScan(context.Background(), runtimeRefTestRequest(raw))
			var iceErr *api.IcebergError
			require.ErrorAs(t, err, &iceErr)
			require.Equal(t, api.ErrConfigInvalid, iceErr.Code)
		})
	}
	for _, raw := range []string{"snapshot:99", "snapshot:0", "snapshot:-1"} {
		t.Run(raw, func(t *testing.T) {
			_, planner := newRuntimeRefTestPlanner(t, sampleMetadataJSON)
			_, err := planner.PlanScan(context.Background(), runtimeRefTestRequest(raw))
			var iceErr *api.IcebergError
			require.ErrorAs(t, err, &iceErr)
			require.Equal(t, api.ErrTableNotFound, iceErr.Code)
			require.Contains(t, iceErr.Fields, "snapshot_id", "missing IDs must not fall back to current or named refs")
		})
	}
	_, planner := newRuntimeRefTestPlanner(t, emptySnapshotMetadataJSON)
	plan, err := planner.PlanScan(context.Background(), runtimeRefTestRequest("branch:main"))
	require.NoError(t, err)
	require.Equal(t, int64(NoCurrentSnapshotID), plan.Snapshot.SnapshotID)
	_, err = planner.PlanScan(context.Background(), runtimeRefTestRequest("snapshot:11"))
	var iceErr *api.IcebergError
	require.ErrorAs(t, err, &iceErr)
	require.Equal(t, api.ErrTableNotFound, iceErr.Code)
}

func TestRuntimeScanPlannerPinnedSchemaAndCacheIsolation(t *testing.T) {
	metadataJSON := strings.Replace(sampleMetadataJSON, `"current-schema-id": 1`, `"current-schema-id": 2`, 1)
	metadataJSON = strings.Replace(metadataJSON, `"schemas": [`, `"schemas": [{"schema-id":2,"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":5,"name":"added","required":false,"type":"long"}]},`, 1)
	fixture, planner := newRuntimeRefTestPlanner(t, metadataJSON)
	planner.Cache = fixture.cache
	loads := 0
	fixture.client.LoadTableFunc = func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
		loads++
		return &api.LoadTableResponse{MetadataLocation: "s3://warehouse/metadata.json", MetadataJSON: []byte(metadataJSON)}, nil
	}
	for _, tc := range []struct {
		ref    string
		id     int64
		schema int
	}{
		{ref: "snapshot:11", id: 11, schema: 1},
		{ref: "branch:main", id: 22, schema: 2},
		{ref: "snapshot:22", id: 22, schema: 1},
	} {
		plan, err := planner.PlanScan(context.Background(), runtimeRefTestRequest(tc.ref))
		require.NoError(t, err)
		require.Equal(t, tc.id, plan.Snapshot.SnapshotID)
		require.Equal(t, tc.schema, plan.Snapshot.SchemaID)
		require.Len(t, plan.ColumnMapping, map[int]int{1: 4, 2: 2}[tc.schema])
	}
	require.Equal(t, 3, loads, "different selectors need distinct cache identity")
	metadataJSON = strings.Replace(metadataJSON, `"current-snapshot-id": 22`, `"current-snapshot-id": 33`, 1)
	metadataJSON = strings.Replace(metadataJSON, `"snapshots": [`, `"snapshots": [{"snapshot-id":33,"sequence-number":8,"timestamp-ms":1710000300000,"manifest-list":"s3://warehouse/sales/orders/metadata/snap-22.avro","schema-id":2},`, 1)
	metadataJSON = strings.Replace(metadataJSON, `"main": {"snapshot-id": 22`, `"main": {"snapshot-id": 33`, 1)
	for _, tc := range []struct {
		ref string
		id  int64
	}{{"snapshot:11", 11}, {"branch:main", 33}, {"snapshot:22", 22}} {
		plan, err := planner.PlanScan(context.Background(), runtimeRefTestRequest(tc.ref))
		require.NoError(t, err)
		require.Equal(t, tc.id, plan.Snapshot.SnapshotID)
	}
	require.Equal(t, 4, loads, "main revalidates while immutable pins reuse their entries")
}

func TestRuntimeScanPlannerTypedRefServerPlanning(t *testing.T) {
	for _, mode := range []api.ServerPlanningMode{api.ServerPlanningRequired, api.ServerPlanningAuto} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := newPlannerFixture(t, 2)
			fixture.reader.data["s3://warehouse/sales/orders/metadata/snap-11.avro"] = []byte("manifest-list")
			client := &scanCapableCatalogClient{MockClient: fixture.client, plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 11}}}
			client.GetConfigFunc = func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
				return &api.ConfigResponse{Prefix: "main|warehouse", Overrides: map[string]string{"nessie.is-nessie-catalog": "true"}, Capabilities: api.CatalogCapabilities{ServerSidePlanning: true}}, nil
			}
			planner := RuntimeScanPlanner{
				CatalogFactory: &recordingCatalogFactory{client: client}, Metadata: fixture.facade,
				ObjectReader: func(context.Context, api.CatalogClient, api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
					return fixture.reader, ObjectReaderContext{}, nil
				},
				Config: api.Config{Scan: api.ScanPlanningConfig{ServerPlanningMode: mode, MaxManifestFiles: 1}},
			}
			plan, err := planner.PlanScan(context.Background(), runtimeRefTestRequest("snapshot:11"))
			require.NoError(t, err)
			require.Equal(t, 1, client.planScanCalls)
			require.Equal(t, "main|warehouse", client.lastRequest.Prefix)
			require.Equal(t, "main", client.lastRequest.Ref)
			require.Equal(t, "main", client.lastRequest.Snapshot.RefName)
			require.True(t, client.lastRequest.Snapshot.HasSnapshotID)
			require.Equal(t, int64(11), client.lastRequest.Snapshot.SnapshotID)
			require.Equal(t, int64(11), plan.Snapshot.SnapshotID)
			require.Equal(t, mode == api.ServerPlanningAuto, plan.Profile.ServerPlanningFallback)
		})
	}
}

func runtimeRefTestRequest(ref string) api.ScanPlanRequest {
	return api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, Warehouse: "s3://warehouse"}},
		Namespace:      api.Namespace{"sales"}, Table: "orders", Ref: ref, Snapshot: api.SnapshotSelector{RefName: ref},
	}
}

func newRuntimeRefTestPlanner(t *testing.T, metadataJSON string) (*plannerFixture, RuntimeScanPlanner) {
	t.Helper()
	fixture := newPlannerFixtureWithMetadata(t, 0, metadataJSON)
	fixture.reader.data["s3://warehouse/sales/orders/metadata/snap-11.avro"] = []byte("manifest-list")
	return fixture, RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client}, Metadata: fixture.facade,
		ObjectReader: func(context.Context, api.CatalogClient, api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Config: api.Config{Scan: api.ScanPlanningConfig{ServerPlanningMode: api.ServerPlanningOff}},
	}
}
