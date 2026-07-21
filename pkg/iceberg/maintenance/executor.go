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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type ProcedureExecutionRequest struct {
	AccountID    uint32
	RoleID       uint64
	UserID       uint64
	StatementID  string
	AllowTagMove bool
	Parsed       ParsedCall
}

type ProcedureCatalogResolution struct {
	CatalogID    uint64
	Catalog      model.Catalog
	Capabilities api.CatalogCapabilities
}

type ProcedureCatalogResolver interface {
	ResolveMaintenanceCatalog(ctx context.Context, accountID uint32, catalogName string) (ProcedureCatalogResolution, error)
}

type ProcedureAuthorizer interface {
	AuthorizeMaintenanceProcedure(ctx context.Context, req ProcedureExecutionRequest, catalog ProcedureCatalogResolution) error
}

type ProcedureCatalogAccess struct {
	ExternalPrincipal string
	ResidencyPolicies []model.ResidencyPolicy
}

type ProcedureAccessChecker interface {
	CheckMaintenanceCatalogAccess(ctx context.Context, req ProcedureExecutionRequest, catalog ProcedureCatalogResolution) (ProcedureCatalogAccess, error)
}

type FeatureAuthorizer struct {
	Config  api.Config
	Account api.AccountConfig
}

func (a FeatureAuthorizer) AuthorizeMaintenanceProcedure(ctx context.Context, req ProcedureExecutionRequest, catalog ProcedureCatalogResolution) error {
	account := a.Account
	if account.AccountID == 0 {
		account.AccountID = req.AccountID
	}
	cfg := a.Config.EffectiveForAccount(account)
	if !cfg.Enable {
		return api.NewError(api.ErrFeatureDisabled, "Iceberg connector is disabled for this account", map[string]string{
			"account_id": accountIDString(req.AccountID),
		})
	}
	if !cfg.Write.EnableWrite {
		return api.NewError(api.ErrFeatureDisabled, "Iceberg write is disabled", map[string]string{
			"operation": string(req.Parsed.Operation),
		})
	}
	if !cfg.Write.EnableMaintenance {
		return api.NewError(api.ErrFeatureDisabled, "Iceberg maintenance is disabled", map[string]string{
			"operation": string(req.Parsed.Operation),
		})
	}
	switch req.Parsed.Operation {
	case OperationRewriteDataFiles, OperationRewriteManifests, OperationExpireSnapshots:
		return nil
	default:
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg maintenance operation is unsupported", map[string]string{
			"operation": string(req.Parsed.Operation),
		})
	}
}

type ProcedureExecutor struct {
	Resolver   ProcedureCatalogResolver
	Authorizer ProcedureAuthorizer
	Access     ProcedureAccessChecker
	Dispatcher Dispatcher
}

func (e ProcedureExecutor) Execute(ctx context.Context, req ProcedureExecutionRequest) (Result, error) {
	if req.Parsed.Operation == "" {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance procedure requires parsed operation", nil)
	}
	if req.Parsed.TargetID.Catalog == "" || req.Parsed.TargetID.Namespace == "" || req.Parsed.TargetID.Table == "" {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance procedure requires parsed target identifier", nil)
	}
	if e.Resolver == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance procedure requires a catalog resolver", nil)
	}
	catalog, err := e.Resolver.ResolveMaintenanceCatalog(ctx, req.AccountID, req.Parsed.TargetID.Catalog)
	if err != nil {
		return Result{}, err
	}
	if catalog.CatalogID == 0 {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance catalog resolver returned an invalid catalog id", map[string]string{
			"catalog": req.Parsed.TargetID.Catalog,
		})
	}
	if e.Authorizer != nil {
		if err := e.Authorizer.AuthorizeMaintenanceProcedure(ctx, req, catalog); err != nil {
			return Result{}, err
		}
	}
	access := ProcedureCatalogAccess{}
	if e.Access != nil {
		access, err = e.Access.CheckMaintenanceCatalogAccess(ctx, req, catalog)
		if err != nil {
			return Result{}, err
		}
	}
	dispatchReq, err := buildDispatchRequest(req, catalog, access)
	if err != nil {
		return Result{}, err
	}
	return e.Dispatcher.Dispatch(ctx, dispatchReq)
}

func buildDispatchRequest(req ProcedureExecutionRequest, catalog ProcedureCatalogResolution, access ProcedureCatalogAccess) (Request, error) {
	idempotencyKey := strings.TrimSpace(req.Parsed.Options["idempotency_key"])
	jobID := strings.TrimSpace(req.Parsed.Options["job_id"])
	if idempotencyKey == "" {
		idempotencyKey = jobID
	}
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(req.StatementID)
	}
	if idempotencyKey == "" {
		return Request{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance procedure requires statement id, job_id, or idempotency_key", map[string]string{
			"target": req.Parsed.Target,
		})
	}
	targetRef := TargetRef(req.Parsed.Options)
	targetRefType := strings.TrimSpace(req.Parsed.Options["ref_type"])
	allowTagMove := req.AllowTagMove || boolOption(req.Parsed.Options, "allow_tag_move")
	return Request{
		AccountID:           req.AccountID,
		RoleID:              req.RoleID,
		UserID:              req.UserID,
		CatalogID:           catalog.CatalogID,
		Catalog:             catalog.Catalog,
		ExternalPrincipal:   strings.TrimSpace(access.ExternalPrincipal),
		ResidencyPolicies:   append([]model.ResidencyPolicy(nil), access.ResidencyPolicies...),
		Namespace:           req.Parsed.TargetID.Namespace,
		Table:               req.Parsed.TargetID.Table,
		TargetRef:           targetRef,
		TargetRefType:       targetRefType,
		AllowTagMove:        allowTagMove,
		CatalogCapabilities: catalog.Capabilities,
		SnapshotBefore:      strings.TrimSpace(req.Parsed.Options["snapshot_before"]),
		JobID:               jobID,
		IdempotencyKey:      idempotencyKey,
		Operation:           req.Parsed.Operation,
		Options:             cloneOptions(req.Parsed.Options),
	}, nil
}

func boolOption(options map[string]string, key string) bool {
	switch strings.ToLower(strings.TrimSpace(options[key])) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func accountIDString(accountID uint32) string {
	return strconv.FormatUint(uint64(accountID), 10)
}

func cloneOptions(options map[string]string) map[string]string {
	if len(options) == 0 {
		return nil
	}
	out := make(map[string]string, len(options))
	for key, value := range options {
		out[key] = value
	}
	return out
}

func ProcedureExecutionRequestFromParsed(accountID uint32, statementID string, parsed ParsedCall) ProcedureExecutionRequest {
	return ProcedureExecutionRequest{
		AccountID:   accountID,
		StatementID: statementID,
		Parsed:      parsed,
	}
}

func ProcedureCatalogResolutionFromModel(catalog model.Catalog) (ProcedureCatalogResolution, error) {
	caps, err := icebergcatalog.ParseCapabilitiesJSON(catalog.CapabilitiesJSON)
	if err != nil {
		return ProcedureCatalogResolution{}, err
	}
	return ProcedureCatalogResolution{
		CatalogID:    catalog.CatalogID,
		Catalog:      catalog,
		Capabilities: caps,
	}, nil
}
