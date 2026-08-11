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

package maintenance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestProcedureExecutorBuildsDispatcherRequest(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureRewriteManifests, "ksa_gold.sales.orders", "ref=main,idempotency_key=idem-1")
	require.NoError(t, err)
	var runnerReq Request
	recorder := &fakeJobRecorder{}
	executor := ProcedureExecutor{
		Resolver: fakeProcedureCatalogResolver{
			resolution: ProcedureCatalogResolution{CatalogID: 42},
		},
		Authorizer: fakeProcedureAuthorizer{},
		Dispatcher: Dispatcher{
			Runners: map[Operation]Runner{
				OperationRewriteManifests: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
					runnerReq = req
					return Result{SnapshotAfter: "100", RewrittenFileCount: 3}, nil
				}),
			},
			Recorder: recorder,
		},
	}
	result, err := executor.Execute(context.Background(), ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	require.NoError(t, err)
	require.Equal(t, "100", result.SnapshotAfter)
	require.Equal(t, uint32(7), runnerReq.AccountID)
	require.Equal(t, uint64(42), runnerReq.CatalogID)
	require.Equal(t, "sales", runnerReq.Namespace)
	require.Equal(t, "orders", runnerReq.Table)
	require.Equal(t, "main", runnerReq.TargetRef)
	require.Equal(t, "idem-1", runnerReq.IdempotencyKey)
	require.Len(t, recorder.inserted, 1)
	require.Equal(t, "sales", recorder.inserted[0].Namespace)
}

func TestProcedureExecutorAllowsSystemAccountZero(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureRewriteManifests, "ksa_gold.sales.orders", "ref=main,idempotency_key=idem-1")
	require.NoError(t, err)
	var resolvedAccountID uint32
	var runnerReq Request
	executor := ProcedureExecutor{
		Resolver: procedureCatalogResolverFunc(func(ctx context.Context, accountID uint32, catalogName string) (ProcedureCatalogResolution, error) {
			resolvedAccountID = accountID
			return ProcedureCatalogResolution{
				CatalogID: 42,
				Catalog:   model.Catalog{AccountID: 0, CatalogID: 42, Name: catalogName},
			}, nil
		}),
		Dispatcher: Dispatcher{
			Runners: map[Operation]Runner{
				OperationRewriteManifests: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
					runnerReq = req
					return Result{SnapshotAfter: "100"}, nil
				}),
			},
		},
	}
	_, err = executor.Execute(context.Background(), ProcedureExecutionRequest{
		AccountID:   0,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0), resolvedAccountID)
	require.Equal(t, uint32(0), runnerReq.AccountID)
	require.Equal(t, uint64(42), runnerReq.CatalogID)
}

func TestProcedureExecutorUsesStatementIDAsIdempotencyFallback(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureExpireSnapshots, "ksa_gold.sales.orders", "ref=main")
	require.NoError(t, err)
	var runnerReq Request
	executor := ProcedureExecutor{
		Resolver: fakeProcedureCatalogResolver{
			resolution: ProcedureCatalogResolution{CatalogID: 42},
		},
		Dispatcher: Dispatcher{
			Runners: map[Operation]Runner{
				OperationExpireSnapshots: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
					runnerReq = req
					return Result{}, nil
				}),
			},
		},
	}
	_, err = executor.Execute(context.Background(), ProcedureExecutionRequest{
		AccountID:   7,
		StatementID: "stmt-1",
		Parsed:      parsed,
	})
	require.NoError(t, err)
	require.Equal(t, "stmt-1", runnerReq.IdempotencyKey)
}

func TestProcedureExecutorRequiresIdempotencySource(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureExpireSnapshots, "ksa_gold.sales.orders", "ref=main")
	require.NoError(t, err)
	_, err = (ProcedureExecutor{
		Resolver: fakeProcedureCatalogResolver{
			resolution: ProcedureCatalogResolution{CatalogID: 42},
		},
		Dispatcher: Dispatcher{
			Runners: map[Operation]Runner{
				OperationExpireSnapshots: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
					return Result{}, nil
				}),
			},
		},
	}).Execute(context.Background(), ProcedureExecutionRequest{
		AccountID: 7,
		Parsed:    parsed,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires statement id")
}

func TestProcedureExecutorPropagatesAuthorizationError(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureRewriteDataFiles, "ksa_gold.sales.orders", "ref=main,idempotency_key=idem-1")
	require.NoError(t, err)
	_, err = (ProcedureExecutor{
		Resolver: fakeProcedureCatalogResolver{
			resolution: ProcedureCatalogResolution{CatalogID: 42},
		},
		Authorizer: fakeProcedureAuthorizer{
			err: api.NewError(api.ErrResidencyDenied, "blocked", nil),
		},
	}).Execute(context.Background(), ProcedureExecutionRequest{
		AccountID: 7,
		Parsed:    parsed,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrResidencyDenied))
}

func TestProcedureCatalogResolutionFromModel(t *testing.T) {
	resolution, err := ProcedureCatalogResolutionFromModel(model.Catalog{
		CatalogID:        42,
		CapabilitiesJSON: `{"branch-tag":true,"metrics_report":true}`,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), resolution.CatalogID)
	require.True(t, resolution.Capabilities.BranchTag)
	require.True(t, resolution.Capabilities.MetricsReport)
}

func TestProcedureCatalogResolutionFromModelRejectsInvalidCapabilities(t *testing.T) {
	_, err := ProcedureCatalogResolutionFromModel(model.Catalog{
		CatalogID:        42,
		CapabilitiesJSON: `{"branch_tag":"maybe"}`,
	})
	require.Error(t, err)
}

func TestFeatureAuthorizerRequiresMaintenanceSwitches(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureRewriteManifests, "ksa_gold.sales.orders", "ref=main")
	require.NoError(t, err)
	req := ProcedureExecutionRequest{AccountID: 7, Parsed: parsed}
	cfg := api.DefaultConfig()
	err = (FeatureAuthorizer{Config: cfg}).AuthorizeMaintenanceProcedure(context.Background(), req, ProcedureCatalogResolution{CatalogID: 42})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrFeatureDisabled))

	cfg.Enable = true
	cfg.Write.EnableWrite = true
	err = (FeatureAuthorizer{Config: cfg}).AuthorizeMaintenanceProcedure(context.Background(), req, ProcedureCatalogResolution{CatalogID: 42})
	require.Error(t, err)
	require.Contains(t, err.Error(), "maintenance is disabled")

	cfg.Write.EnableMaintenance = true
	err = (FeatureAuthorizer{Config: cfg}).AuthorizeMaintenanceProcedure(context.Background(), req, ProcedureCatalogResolution{CatalogID: 42})
	require.NoError(t, err)
}

func TestFeatureAuthorizerHonorsPerAccountSwitch(t *testing.T) {
	parsed, err := ParseProcedureCall(ProcedureExpireSnapshots, "ksa_gold.sales.orders", "ref=main")
	require.NoError(t, err)
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.EnablePerAccount = true
	cfg.Write.EnableWrite = true
	cfg.Write.EnableMaintenance = true
	req := ProcedureExecutionRequest{AccountID: 7, Parsed: parsed}

	err = (FeatureAuthorizer{Config: cfg, Account: api.AccountConfig{AccountID: 7, Enable: false}}).AuthorizeMaintenanceProcedure(context.Background(), req, ProcedureCatalogResolution{CatalogID: 42})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrFeatureDisabled))

	err = (FeatureAuthorizer{Config: cfg, Account: api.AccountConfig{AccountID: 7, Enable: true}}).AuthorizeMaintenanceProcedure(context.Background(), req, ProcedureCatalogResolution{CatalogID: 42})
	require.NoError(t, err)
}

type fakeProcedureCatalogResolver struct {
	resolution ProcedureCatalogResolution
	err        error
}

func (r fakeProcedureCatalogResolver) ResolveMaintenanceCatalog(ctx context.Context, accountID uint32, catalogName string) (ProcedureCatalogResolution, error) {
	if r.err != nil {
		return ProcedureCatalogResolution{}, r.err
	}
	return r.resolution, nil
}

type procedureCatalogResolverFunc func(ctx context.Context, accountID uint32, catalogName string) (ProcedureCatalogResolution, error)

func (fn procedureCatalogResolverFunc) ResolveMaintenanceCatalog(ctx context.Context, accountID uint32, catalogName string) (ProcedureCatalogResolution, error) {
	return fn(ctx, accountID, catalogName)
}

type fakeProcedureAuthorizer struct {
	err error
}

func (a fakeProcedureAuthorizer) AuthorizeMaintenanceProcedure(ctx context.Context, req ProcedureExecutionRequest, catalog ProcedureCatalogResolution) error {
	return a.err
}
