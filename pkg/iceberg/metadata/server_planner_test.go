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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestServerPlanningAutoFallback(t *testing.T) {
	client := &fakeServerScanPlanner{err: api.NewError(api.ErrPlanningLimitExceeded, "client limit", nil)}
	server := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 2}}}
	planner := ServerPlanningPlanner{
		Server: server,
		Client: client,
		Mode:   api.ServerPlanningAuto,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			return true, nil
		},
	}
	plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if !plan.Profile.ServerPlanningFallback || client.calls != 1 || server.calls != 1 {
		t.Fatalf("expected server fallback after client limit, plan=%+v client=%d server=%d", plan, client.calls, server.calls)
	}
	if plan.Snapshot.PlanningMode != serverPlanningMode || plan.Profile.PlanningMode != serverPlanningMode {
		t.Fatalf("server plan mode not marked: %+v profile=%+v", plan.Snapshot, plan.Profile)
	}
}

func TestServerPlanningAutoUsesClientWhenUnderLimit(t *testing.T) {
	client := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 2}}}
	server := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 3}}}
	planner := ServerPlanningPlanner{
		Server: server,
		Client: client,
		Mode:   api.ServerPlanningAuto,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			t.Fatalf("auto mode must not check server capability before client planning hits a limit")
			return false, nil
		},
	}
	plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if plan.Snapshot.SnapshotID != 2 || client.calls != 1 || server.calls != 0 || plan.Profile.ServerPlanningFallback {
		t.Fatalf("expected pure client plan, plan=%+v client=%d server=%d", plan, client.calls, server.calls)
	}
}

func TestServerPlanningAutoRequiresAdvertisedCapabilityBeforeServerFallback(t *testing.T) {
	client := &fakeServerScanPlanner{err: api.NewError(api.ErrPlanningLimitExceeded, "client limit", nil)}
	server := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 3}}}
	planner := ServerPlanningPlanner{
		Server: server,
		Client: client,
		Mode:   api.ServerPlanningAuto,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			return false, nil
		},
	}
	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{})
	assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
	if client.calls != 1 || server.calls != 0 {
		t.Fatalf("server planner should not run without capability, client=%d server=%d", client.calls, server.calls)
	}
}

func TestServerPlanningRequiredDoesNotFallback(t *testing.T) {
	client := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{Snapshot: api.SnapshotPlan{SnapshotID: 2}}}
	planner := ServerPlanningPlanner{
		Server: &fakeServerScanPlanner{err: api.NewError(api.ErrUnsupportedFeature, "unsupported extension", nil)},
		Client: client,
		Mode:   api.ServerPlanningRequired,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			return true, nil
		},
	}
	_, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{})
	if err == nil {
		t.Fatalf("expected required server planning error")
	}
	if client.calls != 0 {
		t.Fatalf("required server planning must not fallback, calls=%d", client.calls)
	}
}

func TestServerPlanningKeepsResidualUnlessPredicateEquivalent(t *testing.T) {
	server := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{
		Snapshot:       api.SnapshotPlan{SnapshotID: 3},
		ResidualFilter: api.ResidualFilter{ExpressionSQL: "catalog_residual"},
		DataTasks: []api.DataFileTask{{
			DataFile: api.DataFile{FilePath: "s3://warehouse/orders/data.parquet"},
		}},
	}}
	planner := ServerPlanningPlanner{
		Server: server,
		Mode:   api.ServerPlanningRequired,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			return true, nil
		},
	}
	plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{ResidualSQL: "mo_residual"})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if plan.ResidualFilter.ExpressionSQL != "(catalog_residual) AND (mo_residual)" {
		t.Fatalf("expected merged residual, got %+v", plan.ResidualFilter)
	}
	if plan.DataTasks[0].ResidualFilter.ExpressionSQL != "mo_residual" {
		t.Fatalf("expected task residual to keep MO residual, got %+v", plan.DataTasks[0].ResidualFilter)
	}
}

func TestServerPlanningTrustsVerifiedPredicateEquivalent(t *testing.T) {
	server := &fakeServerScanPlanner{plan: &api.IcebergScanPlan{
		Snapshot:                  api.SnapshotPlan{SnapshotID: 3},
		ResidualFilter:            api.ResidualFilter{AlwaysTrue: true},
		ServerPredicateEquivalent: true,
	}}
	planner := ServerPlanningPlanner{
		Server: server,
		Mode:   api.ServerPlanningRequired,
		SupportsServerPlanning: func(context.Context, api.ScanPlanRequest) (bool, error) {
			return true, nil
		},
	}
	plan, err := planner.PlanScan(context.Background(), api.ScanPlanRequest{ResidualSQL: "mo_residual"})
	if err != nil {
		t.Fatalf("plan scan: %v", err)
	}
	if !plan.ResidualFilter.AlwaysTrue || plan.ResidualFilter.ExpressionSQL != "" {
		t.Fatalf("verified equivalent server plan should not re-add MO residual: %+v", plan.ResidualFilter)
	}
}

type fakeServerScanPlanner struct {
	plan  *api.IcebergScanPlan
	err   error
	calls int
}

func (p *fakeServerScanPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	return p.plan, nil
}
