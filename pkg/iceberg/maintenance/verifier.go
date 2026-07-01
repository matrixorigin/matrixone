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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type CatalogCommitVerifier struct {
	Client  api.CatalogClient
	Catalog api.CatalogRequest
}

type CatalogFactoryCommitVerifier struct {
	CatalogFactory CatalogClientFactory
}

func (v CatalogFactoryCommitVerifier) VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
	if v.CatalogFactory == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := v.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return result, false, err
	}
	return (CatalogCommitVerifier{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: req.Catalog},
	}).VerifyCommittedMaintenance(ctx, req, plan, result)
}

func (v CatalogCommitVerifier) VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
	if v.Client == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires a catalog client", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if result.SnapshotID <= 0 {
		return result, false, nil
	}
	meta, err := (CatalogMetadataLoader{
		Client:  v.Client,
		Catalog: v.Catalog,
	}).LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return result, false, err
	}
	snapshot, err := maintenanceTargetSnapshot(meta, req.TargetRef, "")
	if err != nil {
		return result, false, err
	}
	if snapshot.SnapshotID != result.SnapshotID {
		return result, false, nil
	}
	if strings.TrimSpace(result.MetadataLocationHash) != "" && meta.MetadataLocationHash != result.MetadataLocationHash {
		return result, false, nil
	}
	result.Verified = true
	return result, true, nil
}

var _ CommitResultVerifier = CatalogCommitVerifier{}
var _ CommitResultVerifier = CatalogFactoryCommitVerifier{}
