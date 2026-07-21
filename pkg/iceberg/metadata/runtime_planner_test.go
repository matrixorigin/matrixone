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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

func TestRuntimeScanPlannerBuildsRequestScopedLocalPlanner(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	factory := &recordingCatalogFactory{client: fixture.client}
	var readerReq api.ScanPlanRequest
	planner := RuntimeScanPlanner{
		CatalogFactory: factory,
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			require.Same(t, fixture.client, catalogClient)
			readerReq = req
			return fixture.reader, ObjectReaderContext{
				CredentialHash:  "runtime-cred-hash",
				CredentialScope: "runtime-cred-scope",
			}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}
	req := api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{
			Catalog: model.Catalog{
				AccountID:      42,
				CatalogID:      7,
				Name:           "prod",
				Type:           "rest",
				URI:            "https://catalog.example.com/iceberg",
				Warehouse:      "warehouse_a",
				AuthMode:       model.AuthModeCredential,
				TokenSecretRef: "secret://catalog/prod",
			},
			ExternalPrincipal: "ksa-analytics",
		},
		Namespace: api.Namespace{"sales"},
		Table:     "orders",
	}

	plan, err := planner.PlanScan(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, req.Catalog, factory.catalog)
	require.Equal(t, req.Catalog, readerReq.Catalog)
	require.Equal(t, "ksa-analytics", readerReq.ExternalPrincipal)
	require.Len(t, plan.DataTasks, 1)
	require.Equal(t, "runtime-cred-scope", plan.DataTasks[0].CredentialScope)
	require.Equal(t, 1, fixture.catalogLoads)
}

func TestRuntimeScanPlannerReleasesObjectIORefOnPlanningFailure(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	fixture.client.LoadTableFunc = func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
		return nil, moerr.NewInternalErrorNoCtx("catalog failed")
	}
	fs, err := fileservice.NewMemoryFS("iceberg-planner-object-io", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	var ref string
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(context.Context, api.CatalogClient, api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			var registerErr error
			ref, registerErr = icebergio.RegisterEphemeralObjectIOProvider(
				context.Background(),
				icebergio.ScopedProvider{FileService: fs},
				nil,
				time.Hour,
			)
			return fixture.reader, ObjectReaderContext{ObjectIORef: ref}, registerErr
		},
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}

	_, err = planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	require.Error(t, err)
	require.NotEmpty(t, ref)
	_, _, err = icebergio.ResolveObjectIORef(context.Background(), ref, "s3://warehouse/orders/data/a.parquet")
	require.Error(t, err, "failed planning must not leave the credential provider registered")
}

func TestRuntimeScanPlannerResolvesRESTPrefixBeforeObjectReader(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	fixture.client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{Prefix: "main"}, nil
	}
	var readerPrefix string
	var loadTablePrefix string
	fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		loadTablePrefix = req.Prefix
		fixture.catalogLoads++
		return &api.LoadTableResponse{
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json",
			MetadataJSON:     []byte(sampleMetadataJSON),
			ETag:             "etag-1",
		}, nil
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			readerPrefix = req.Prefix
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}

	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, URI: "https://catalog.example.com/iceberg", Warehouse: "s3://warehouse"}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	require.NoError(t, err)
	require.Equal(t, "main", readerPrefix)
	require.Equal(t, "main", loadTablePrefix)
}

func TestRuntimeScanPlannerRewritesNessieNamedRefToCatalogPrefix(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	fixture.client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{
			Prefix:    "main|s3://warehouse",
			Overrides: map[string]string{"nessie.is-nessie-catalog": "true"},
		}, nil
	}
	var readerReq api.ScanPlanRequest
	var loadTablePrefix string
	var loadTableSnapshots string
	fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		loadTablePrefix = req.Prefix
		loadTableSnapshots = req.Snapshots
		fixture.catalogLoads++
		return &api.LoadTableResponse{
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json",
			MetadataJSON:     []byte(sampleMetadataJSON),
			ETag:             "etag-1",
		}, nil
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			readerReq = req
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}

	plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, URI: "https://catalog.example.com/iceberg", Warehouse: "s3://warehouse"}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "tag:release",
		Snapshot:       api.SnapshotSelector{RefName: "tag:release"},
	})
	require.NoError(t, err)
	require.Equal(t, "release|s3://warehouse", readerReq.Prefix)
	require.Empty(t, readerReq.Ref)
	require.Equal(t, "main", readerReq.Snapshot.RefName)
	require.True(t, readerReq.Snapshot.AllowMainFallback)
	require.Equal(t, "release|s3://warehouse", loadTablePrefix)
	require.Equal(t, "main", loadTableSnapshots)
	require.Equal(t, "tag:release", plan.Snapshot.RefName)
}

func TestRuntimeScanPlannerCacheSeparatesNessieRefsByEffectivePrefix(t *testing.T) {
	const (
		manifestListPath = "s3://warehouse/sales/orders/metadata/snap-22.avro"
		manifestPath     = "s3://warehouse/sales/orders/metadata/shared.avro"
	)
	fixture := newPlannerFixture(t, 0)
	fixture.facade.manifestLists = map[string][]api.ManifestFile{
		"release-list": {{Path: manifestPath, Content: api.ManifestContentData}},
		"hotfix-list":  {{Path: manifestPath, Content: api.ManifestContentData}},
	}
	fixture.facade.entries = map[string][]api.ManifestEntry{
		"release-manifest": {{
			Status:     api.ManifestEntryAdded,
			SnapshotID: 22,
			DataFile: api.DataFile{
				Content:         api.DataFileContentData,
				FilePath:        "s3://warehouse/sales/orders/data/release.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 10,
			},
		}},
		"hotfix-manifest": {{
			Status:     api.ManifestEntryAdded,
			SnapshotID: 22,
			DataFile: api.DataFile{
				Content:         api.DataFileContentData,
				FilePath:        "s3://warehouse/sales/orders/data/hotfix.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 10,
			},
		}},
	}
	fixture.client.GetConfigFunc = func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{
			Prefix:    "main|s3://warehouse",
			Overrides: map[string]string{"nessie.is-nessie-catalog": "true"},
		}, nil
	}
	var loadPrefixes []string
	fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		fixture.catalogLoads++
		loadPrefixes = append(loadPrefixes, req.Prefix)
		return &api.LoadTableResponse{
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json",
			MetadataJSON:     []byte(sampleMetadataJSON),
			ETag:             "shared-etag",
		}, nil
	}
	readers := map[string]*plannerObjectReader{
		"release|s3://warehouse": {data: map[string][]byte{
			manifestListPath: []byte("release-list"),
			manifestPath:     []byte("release-manifest"),
		}},
		"hotfix|s3://warehouse": {data: map[string][]byte{
			manifestListPath: []byte("hotfix-list"),
			manifestPath:     []byte("hotfix-manifest"),
		}},
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			return readers[req.Prefix], ObjectReaderContext{CredentialHash: "shared-credential"}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}
	request := func(ref string) api.ScanPlanRequest {
		return api.ScanPlanRequest{
			CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{
				AccountID: 42,
				CatalogID: 7,
				URI:       "https://catalog.example.com/iceberg",
				Warehouse: "s3://warehouse",
			}},
			Namespace: api.Namespace{"sales"},
			Table:     "orders",
			Ref:       "tag:" + ref,
			Snapshot:  api.SnapshotSelector{RefName: "tag:" + ref},
		}
	}

	release, err := planner.PlanScan(context.Background(), request("release"))
	require.NoError(t, err)
	require.Len(t, release.DataTasks, 1)
	require.Equal(t, "s3://warehouse/sales/orders/data/release.parquet", release.DataTasks[0].DataFile.FilePath)

	hotfix, err := planner.PlanScan(context.Background(), request("hotfix"))
	require.NoError(t, err)
	require.Len(t, hotfix.DataTasks, 1)
	require.Equal(t, "s3://warehouse/sales/orders/data/hotfix.parquet", hotfix.DataTasks[0].DataFile.FilePath)
	require.Equal(t, []string{"release|s3://warehouse", "hotfix|s3://warehouse"}, loadPrefixes)
	require.Equal(t, 2, fixture.catalogLoads)
	require.Equal(t, 2, readers["release|s3://warehouse"].calls)
	require.Equal(t, 2, readers["hotfix|s3://warehouse"].calls)
	require.Zero(t, hotfix.Profile.PlanningCacheHits)
}

func TestRuntimeScanPlannerRewritesNessieNamedRefForPlainMainPrefix(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	fixture.client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{
			Prefix:    "main",
			Overrides: map[string]string{"nessie.is-nessie-catalog": "true"},
		}, nil
	}
	var readerReq api.ScanPlanRequest
	var loadTablePrefix string
	fixture.client.LoadTableFunc = func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		loadTablePrefix = req.Prefix
		fixture.catalogLoads++
		return &api.LoadTableResponse{
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json",
			MetadataJSON:     []byte(sampleMetadataJSON),
			ETag:             "etag-1",
		}, nil
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			readerReq = req
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}

	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, URI: "https://catalog.example.com/iceberg", Warehouse: "s3://warehouse"}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "audit_branch",
		Snapshot:       api.SnapshotSelector{RefName: "audit_branch"},
	})
	require.NoError(t, err)
	require.Equal(t, "audit_branch", readerReq.Prefix)
	require.Equal(t, "audit_branch", loadTablePrefix)
	require.Empty(t, readerReq.Ref)
	require.Equal(t, "main", readerReq.Snapshot.RefName)
	require.True(t, readerReq.Snapshot.AllowMainFallback)
}

func TestRuntimeScanPlannerHonorsServerPlanningModes(t *testing.T) {
	tests := []struct {
		name            string
		mode            api.ServerPlanningMode
		manifestCount   int
		maxDataFiles    int
		serverCapable   bool
		wantSnapshotID  int64
		wantServerCalls int
		wantCatalogLoad int
		wantFallback    bool
	}{
		{
			name:            "off uses local planner only",
			mode:            api.ServerPlanningOff,
			manifestCount:   1,
			maxDataFiles:    100,
			serverCapable:   true,
			wantSnapshotID:  22,
			wantCatalogLoad: 1,
		},
		{
			name:            "auto uses client while under limits",
			mode:            api.ServerPlanningAuto,
			manifestCount:   1,
			maxDataFiles:    100,
			serverCapable:   true,
			wantSnapshotID:  22,
			wantCatalogLoad: 1,
		},
		{
			name:            "auto falls back after client planning limit",
			mode:            api.ServerPlanningAuto,
			manifestCount:   2,
			maxDataFiles:    1,
			serverCapable:   true,
			wantSnapshotID:  99,
			wantServerCalls: 1,
			wantCatalogLoad: 1,
			wantFallback:    true,
		},
		{
			name:            "required uses server without local metadata load",
			mode:            api.ServerPlanningRequired,
			manifestCount:   2,
			maxDataFiles:    1,
			serverCapable:   true,
			wantSnapshotID:  99,
			wantServerCalls: 1,
			wantCatalogLoad: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlannerFixture(t, tc.manifestCount)
			serverPlan := &api.IcebergScanPlan{
				Snapshot: api.SnapshotPlan{SnapshotID: 99},
				DataTasks: []api.DataFileTask{{
					DataFile: api.DataFile{FilePath: "s3://warehouse/sales/orders/data/server.parquet", FileFormat: "parquet", RecordCount: 1, SpecID: 0},
				}},
			}
			client := &scanCapableCatalogClient{MockClient: fixture.client, plan: serverPlan}
			client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
				return &api.ConfigResponse{Capabilities: api.CatalogCapabilities{ServerSidePlanning: tc.serverCapable}}, nil
			}
			planner := RuntimeScanPlanner{
				CatalogFactory: &recordingCatalogFactory{client: client},
				Metadata:       fixture.facade,
				ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
					return fixture.reader, ObjectReaderContext{}, nil
				},
				Cache: fixture.cache,
				Config: api.Config{Scan: api.ScanPlanningConfig{
					ManifestReadParallelism: 1,
					MaxManifestFiles:        100,
					MaxDataFiles:            tc.maxDataFiles,
					ServerPlanningMode:      tc.mode,
				}},
			}
			plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
				CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, Warehouse: "warehouse_a"}},
				Namespace:      api.Namespace{"sales"},
				Table:          "orders",
				ResidualSQL:    "mo_residual",
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantSnapshotID, plan.Snapshot.SnapshotID)
			require.Equal(t, tc.wantServerCalls, client.planScanCalls)
			require.Equal(t, tc.wantCatalogLoad, fixture.catalogLoads)
			require.Equal(t, tc.wantFallback, plan.Profile.ServerPlanningFallback)
			if tc.wantServerCalls > 0 {
				require.Equal(t, serverPlanningMode, plan.Snapshot.PlanningMode)
				require.Equal(t, serverPlanningMode, plan.Profile.PlanningMode)
			}
		})
	}
}

func TestRuntimeScanPlannerReportsScanMetricsWhenCatalogSupportsIt(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	client := &metricsCatalogClient{MockClient: fixture.client}
	client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{Capabilities: api.CatalogCapabilities{MetricsReport: true}}, nil
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}
	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, Warehouse: "warehouse_a"}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Ref:            "main",
	})
	require.NoError(t, err)
	require.Len(t, client.reports, 1)
	report := client.reports[0]
	require.Equal(t, api.MetricsReportScan, report.Kind)
	require.Equal(t, "orders", report.Table)
	require.Equal(t, "main", report.Ref)
	require.Equal(t, int64(22), report.SnapshotID)
	require.Equal(t, 1, report.Files)
}

func TestRuntimeScanPlannerSkipsScanMetricsWithoutCapability(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	client := &metricsCatalogClient{MockClient: fixture.client}
	client.GetConfigFunc = func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
		return &api.ConfigResponse{}, nil
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache: fixture.cache,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}
	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7, Warehouse: "warehouse_a"}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	require.NoError(t, err)
	require.Empty(t, client.reports)
}

func TestRuntimeScanPlannerPassesRefCacheRefresherToLocalPlanner(t *testing.T) {
	fixture := newPlannerFixture(t, 1)
	refresher := &recordingRefCacheRefresher{}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: fixture.client},
		Metadata:       fixture.facade,
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			return fixture.reader, ObjectReaderContext{}, nil
		},
		Cache:             fixture.cache,
		RefCacheRefresher: refresher,
		Config: api.Config{Scan: api.ScanPlanningConfig{
			ManifestReadParallelism: 1,
			MaxManifestFiles:        100,
			MaxDataFiles:            100,
		}},
	}
	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 42, CatalogID: 7}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	require.NoError(t, err)
	require.Equal(t, 1, refresher.calls)
	require.Len(t, refresher.refs, 2)
	require.Equal(t, uint32(42), refresher.refs[0].AccountID)
	require.Equal(t, uint64(7), refresher.refs[0].CatalogID)
}

func TestRuntimeScanPlannerRequiresFactories(t *testing.T) {
	_, err := RuntimeScanPlanner{}.PlanScan(context.Background(), api.ScanPlanRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
	require.Contains(t, err.Error(), "catalog factory")

	_, err = RuntimeScanPlanner{CatalogFactory: &recordingCatalogFactory{}}.PlanScan(context.Background(), api.ScanPlanRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "object reader factory")
}

func TestRuntimeScanPlannerCatalogValidatorWrapsObjectReaderCatalogClient(t *testing.T) {
	calledUpstream := false
	client := &catalog.MockClient{
		LoadCredentialsFunc: func(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
			calledUpstream = true
			return &api.LoadCredentialsResponse{}, nil
		},
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: client},
		CatalogValidator: func(ctx context.Context, req api.CatalogRequest) error {
			return moerr.NewInvalidInput(ctx, "blocked by catalog validator")
		},
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			_, err := catalogClient.LoadCredentials(ctx, api.LoadCredentialsRequest{CatalogRequest: req.CatalogRequest})
			return nil, ObjectReaderContext{}, err
		},
	}

	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked by catalog validator")
	require.False(t, calledUpstream, "catalog validator must run before upstream catalog IO")
}

func TestRuntimeScanPlannerUsesRequestScopedCatalogValidator(t *testing.T) {
	calledUpstream := false
	client := &catalog.MockClient{
		LoadCredentialsFunc: func(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
			calledUpstream = true
			return &api.LoadCredentialsResponse{}, nil
		},
	}
	planner := RuntimeScanPlanner{
		CatalogFactory: &recordingCatalogFactory{client: client},
		ObjectReader: func(ctx context.Context, catalogClient api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
			_, err := catalogClient.LoadCredentials(ctx, api.LoadCredentialsRequest{CatalogRequest: req.CatalogRequest})
			return nil, ObjectReaderContext{}, err
		},
	}
	req := api.ScanPlanRequest{
		CatalogValidator: func(ctx context.Context, req api.CatalogRequest) error {
			return api.NewError(api.ErrResidencyDenied, "blocked by request residency policy", nil)
		},
	}

	_, err := planner.PlanScan(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrResidencyDenied))
	require.False(t, calledUpstream, "request-scoped catalog validator must run before upstream catalog IO")
}

func TestValidatingCatalogClientForwardsPlanScan(t *testing.T) {
	upstream := &scanCapableCatalogClient{
		MockClient: &catalog.MockClient{},
		plan:       &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 42}},
	}
	validated := false
	client := validatingCatalogClient{
		upstream: upstream,
		validator: func(ctx context.Context, req api.CatalogRequest) error {
			validated = true
			require.Equal(t, uint64(7), req.Catalog.CatalogID)
			return nil
		},
	}
	plan, err := client.PlanScan(context.Background(), api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{CatalogID: 7}},
	})
	require.NoError(t, err)
	require.True(t, validated)
	require.Equal(t, int64(42), plan.Snapshot.SnapshotID)
	require.Equal(t, 1, upstream.planScanCalls)
}

type recordingCatalogFactory struct {
	client  api.CatalogClient
	catalog model.Catalog
}

func (f *recordingCatalogFactory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	f.catalog = catalog
	if f.client == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "test catalog client is missing", nil)
	}
	return f.client, nil
}

type scanCapableCatalogClient struct {
	*catalog.MockClient
	plan          *api.IcebergScanPlan
	planScanCalls int
}

func (c *scanCapableCatalogClient) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	c.planScanCalls++
	return c.plan, nil
}

type metricsCatalogClient struct {
	*catalog.MockClient
	reports []api.MetricsReportRequest
}

func (c *metricsCatalogClient) ReportMetrics(ctx context.Context, req api.MetricsReportRequest) error {
	c.reports = append(c.reports, req)
	return nil
}

type recordingRefCacheRefresher struct {
	calls int
	refs  []model.RefCache
	err   error
}

func (r *recordingRefCacheRefresher) RefreshRefCache(ctx context.Context, refs []model.RefCache) error {
	r.calls++
	r.refs = append([]model.RefCache(nil), refs...)
	return r.err
}
