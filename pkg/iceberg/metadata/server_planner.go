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
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const serverPlanningMode = "server-side"

type ServerPlanningCapability func(context.Context, api.ScanPlanRequest) (bool, error)

type ServerPlanningPlanner struct {
	Server                 api.ScanPlanner
	Client                 api.ScanPlanner
	Mode                   api.ServerPlanningMode
	SupportsServerPlanning ServerPlanningCapability
}

func (p ServerPlanningPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	mode := p.Mode
	if mode == "" {
		mode = api.ServerPlanningAuto
	}
	if mode == api.ServerPlanningOff {
		return p.planClient(ctx, req, false)
	}

	if mode == api.ServerPlanningRequired {
		if err := p.ensureServerPlanning(ctx, req); err != nil {
			return nil, err
		}
		return p.planServer(ctx, req)
	}

	clientPlan, clientErr := p.planClient(ctx, req, false)
	if clientErr == nil {
		return clientPlan, nil
	}
	if !clientPlanningLimitExceeded(clientErr) {
		return nil, clientErr
	}
	if err := p.ensureServerPlanning(ctx, req); err != nil {
		return nil, clientErr
	}
	plan, err := p.planServer(ctx, req)
	if err == nil && plan != nil {
		plan.Profile.ServerPlanningFallback = true
		return plan, nil
	}
	if !serverPlanningFallbackable(err) {
		return nil, err
	}
	return nil, clientErr
}

func (p ServerPlanningPlanner) planClient(ctx context.Context, req api.ScanPlanRequest, fallback bool) (*api.IcebergScanPlan, error) {
	if p.Client == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg client-side planner is not configured", nil)
	}
	plan, err := p.Client.PlanScan(ctx, req)
	if err != nil {
		return nil, err
	}
	if fallback && plan != nil {
		plan.Profile.ServerPlanningFallback = true
	}
	return plan, nil
}

func (p ServerPlanningPlanner) planServer(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	if p.Server == nil {
		return nil, api.NewError(api.ErrServerPlanningRequired, "Iceberg server-side planning is required but no server planner is available", nil)
	}
	plan, err := p.Server.PlanScan(ctx, req)
	if err != nil {
		return nil, err
	}
	if plan != nil {
		plan.Snapshot.PlanningMode = serverPlanningMode
		plan.Profile.PlanningMode = serverPlanningMode
		applyServerResidualSafety(req, plan)
	}
	return plan, nil
}

func applyServerResidualSafety(req api.ScanPlanRequest, plan *api.IcebergScanPlan) {
	if plan == nil || plan.ServerPredicateEquivalent {
		return
	}
	residual := strings.TrimSpace(req.ResidualSQL)
	if residual == "" {
		return
	}
	plan.ResidualFilter = mergeResidualFilters(plan.ResidualFilter, api.ResidualFilter{ExpressionSQL: residual})
	for i := range plan.DataTasks {
		plan.DataTasks[i].ResidualFilter = mergeResidualFilters(plan.DataTasks[i].ResidualFilter, api.ResidualFilter{ExpressionSQL: residual})
	}
}

func mergeResidualFilters(existing api.ResidualFilter, required api.ResidualFilter) api.ResidualFilter {
	reqExpr := strings.TrimSpace(required.ExpressionSQL)
	if reqExpr == "" || required.AlwaysTrue {
		return existing
	}
	existingExpr := strings.TrimSpace(existing.ExpressionSQL)
	if existingExpr == "" || existing.AlwaysTrue {
		return api.ResidualFilter{ExpressionSQL: reqExpr}
	}
	if existingExpr == reqExpr {
		return api.ResidualFilter{ExpressionSQL: existingExpr}
	}
	return api.ResidualFilter{ExpressionSQL: "(" + existingExpr + ") AND (" + reqExpr + ")"}
}

func (p ServerPlanningPlanner) ensureServerPlanning(ctx context.Context, req api.ScanPlanRequest) error {
	if p.Server == nil {
		return api.NewError(api.ErrServerPlanningRequired, "Iceberg server-side planning is required but no server planner is available", nil)
	}
	if p.SupportsServerPlanning == nil {
		return nil
	}
	supported, err := p.SupportsServerPlanning(ctx, req)
	if err != nil {
		return err
	}
	if !supported {
		return api.NewError(api.ErrServerPlanningRequired, "Iceberg catalog does not advertise server-side planning capability", nil)
	}
	return nil
}

func clientPlanningLimitExceeded(err error) bool {
	var icebergErr *api.IcebergError
	if !errors.As(err, &icebergErr) {
		return false
	}
	return icebergErr.Code == api.ErrPlanningLimitExceeded
}

func serverPlanningFallbackable(err error) bool {
	var icebergErr *api.IcebergError
	if !errors.As(err, &icebergErr) {
		return false
	}
	switch icebergErr.Code {
	case api.ErrCatalogUnavailable, api.ErrUnsupportedFeature, api.ErrPlanningLimitExceeded:
		return true
	default:
		return false
	}
}

var _ api.ScanPlanner = ServerPlanningPlanner{}
