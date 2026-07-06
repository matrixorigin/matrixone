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

package iceberg

import (
	"context"
	"net"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type ResidencyRequest struct {
	AccountID  uint32
	CatalogID  uint64
	CatalogURI string
	Endpoint   string
	Region     string
	Bucket     string
}

type CatalogResidencyRequest struct {
	AccountID  uint32
	CatalogID  uint64
	CatalogURI string
}

func ValidateResidencyPolicy(ctx context.Context, policy model.ResidencyPolicy) error {
	switch policy.ScopeType {
	case model.ResidencyScopeCluster:
		if policy.AccountID != 0 {
			return moerr.NewInvalidInput(ctx, "cluster iceberg residency policy must use account_id 0")
		}
	case model.ResidencyScopeAccount:
		if policy.AccountID == 0 {
			return moerr.NewInvalidInput(ctx, "account iceberg residency policy requires account_id")
		}
	default:
		return moerr.NewInvalidInput(ctx, "iceberg residency policy scope_type must be cluster or account")
	}
	if _, err := NormalizeCatalogURI(ctx, policy.AllowedCatalogURI); err != nil {
		return err
	}
	if _, err := NormalizeEndpoint(ctx, policy.AllowedEndpoint); err != nil {
		return err
	}
	if policy.AllowedRegion == "" || policy.AllowedBucket == "" {
		return moerr.NewInvalidInput(ctx, "iceberg residency policy requires explicit '*' sentinel or exact region/bucket")
	}
	if policy.AllowedEndpoint == model.ResidencyWildcard || policy.AllowedCatalogURI == model.ResidencyWildcard {
		return moerr.NewInvalidInput(ctx, "iceberg residency policy does not allow wildcard catalog uri or endpoint")
	}
	switch normalizedPolicyState(policy.PolicyState) {
	case model.ResidencyPolicyEnabled, model.ResidencyPolicyDisabled, model.ResidencyPolicyAudit:
	default:
		return moerr.NewInvalidInput(ctx, "iceberg residency policy_state must be enabled, disabled, or audit")
	}
	return nil
}

func ValidateResidencyPolicyPrivilege(ctx context.Context, actorAccountID uint32, isClusterAdmin bool, policy model.ResidencyPolicy) error {
	if err := ValidateResidencyPolicy(ctx, policy); err != nil {
		return err
	}
	switch policy.ScopeType {
	case model.ResidencyScopeCluster:
		if !isClusterAdmin {
			return moerr.NewInvalidInput(ctx, "cluster iceberg residency policy can only be managed by cluster admin")
		}
	case model.ResidencyScopeAccount:
		if isClusterAdmin || actorAccountID == policy.AccountID {
			return nil
		}
		return moerr.NewInvalidInput(ctx, "account iceberg residency policy can only be managed by the target account or cluster admin")
	}
	return nil
}

func CheckResidency(ctx context.Context, policies []model.ResidencyPolicy, req ResidencyRequest) error {
	normalized, err := NormalizeResidencyRequest(ctx, req)
	if err != nil {
		return err
	}
	clusterPolicySeen := false
	clusterAllowed := false
	accountPolicySeen := false
	accountAllowed := false
	for _, policy := range policies {
		if normalizedPolicyState(policy.PolicyState) != model.ResidencyPolicyEnabled {
			continue
		}
		if err := ValidateResidencyPolicy(ctx, policy); err != nil {
			return err
		}
		catalogScopeMatches, err := residencyPolicyCatalogScopeMatches(ctx, policy, normalized.CatalogID, normalized.CatalogURI)
		if err != nil {
			return err
		}
		applies, err := residencyPolicyMatches(ctx, policy, normalized)
		if err != nil {
			return err
		}
		if policy.ScopeType == model.ResidencyScopeCluster && applies {
			clusterAllowed = true
		}
		if policy.ScopeType == model.ResidencyScopeCluster && catalogScopeMatches {
			clusterPolicySeen = true
		}
		if policy.ScopeType == model.ResidencyScopeAccount &&
			policy.AccountID == normalized.AccountID &&
			catalogScopeMatches {
			accountPolicySeen = true
			if applies {
				accountAllowed = true
			}
		}
	}
	if !clusterPolicySeen {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "no cluster residency policy configured", residencyErrorFields(normalized)))
	}
	if !clusterAllowed {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "cluster policy did not allow catalog/object endpoint", residencyErrorFields(normalized)))
	}
	if accountPolicySeen && !accountAllowed {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "account policy narrowed the cluster allow set", residencyErrorFields(normalized)))
	}
	return nil
}

func CheckCatalogResidency(ctx context.Context, policies []model.ResidencyPolicy, req CatalogResidencyRequest) error {
	normalized, err := NormalizeCatalogResidencyRequest(ctx, req)
	if err != nil {
		return err
	}
	clusterPolicySeen := false
	clusterAllowed := false
	accountPolicySeen := false
	accountAllowed := false
	for _, policy := range policies {
		if normalizedPolicyState(policy.PolicyState) != model.ResidencyPolicyEnabled {
			continue
		}
		if err := ValidateResidencyPolicy(ctx, policy); err != nil {
			return err
		}
		applies, err := residencyPolicyCatalogScopeMatches(ctx, policy, normalized.CatalogID, normalized.CatalogURI)
		if err != nil {
			return err
		}
		if !applies {
			continue
		}
		if policy.ScopeType == model.ResidencyScopeCluster {
			clusterPolicySeen = true
			clusterAllowed = true
		}
		if policy.ScopeType == model.ResidencyScopeAccount && policy.AccountID == normalized.AccountID {
			accountPolicySeen = true
			accountAllowed = true
		}
	}
	if !clusterPolicySeen {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "no cluster residency policy configured", map[string]string{
			"catalog_uri": normalized.CatalogURI,
		}))
	}
	if !clusterAllowed {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "cluster policy did not allow catalog endpoint", map[string]string{
			"catalog_uri": normalized.CatalogURI,
		}))
	}
	if accountPolicySeen && !accountAllowed {
		return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "account policy narrowed the cluster allow set", map[string]string{
			"catalog_uri": normalized.CatalogURI,
		}))
	}
	return nil
}

func CatalogRequestResidencyValidator(policies []model.ResidencyPolicy) api.CatalogRequestValidator {
	return func(ctx context.Context, req api.CatalogRequest) error {
		return CheckCatalogResidency(ctx, policies, CatalogResidencyRequest{
			AccountID:  req.Catalog.AccountID,
			CatalogID:  req.Catalog.CatalogID,
			CatalogURI: req.Catalog.URI,
		})
	}
}

func CheckObjectScopeResidency(ctx context.Context, policies []model.ResidencyPolicy, catalogURI string, scope icebergio.ObjectScope) error {
	return CheckResidency(ctx, policies, ResidencyRequest{
		AccountID:  scope.AccountID,
		CatalogID:  scope.CatalogID,
		CatalogURI: catalogURI,
		Endpoint:   scope.Endpoint,
		Region:     scope.Region,
		Bucket:     scope.Bucket,
	})
}

func ObjectScopeResidencyValidator(policies []model.ResidencyPolicy, catalogURI string) icebergio.ResidencyValidator {
	return func(ctx context.Context, scope icebergio.ObjectScope) error {
		return CheckObjectScopeResidency(ctx, policies, catalogURI, scope)
	}
}

func ObjectResidencyRequestValidator(policies []model.ResidencyPolicy, catalogURI string) api.ObjectResidencyValidator {
	return func(ctx context.Context, req api.ObjectResidencyRequest) error {
		return CheckResidency(ctx, policies, ResidencyRequest{
			AccountID:  req.AccountID,
			CatalogID:  req.CatalogID,
			CatalogURI: catalogURI,
			Endpoint:   req.Endpoint,
			Region:     req.Region,
			Bucket:     req.Bucket,
		})
	}
}

func residencyErrorFields(req ResidencyRequest) map[string]string {
	return map[string]string{
		"catalog_uri": req.CatalogURI,
		"endpoint":    req.Endpoint,
		"region":      req.Region,
		"bucket":      req.Bucket,
	}
}

func NormalizeResidencyRequest(ctx context.Context, req ResidencyRequest) (ResidencyRequest, error) {
	catalogURI, err := NormalizeCatalogURI(ctx, req.CatalogURI)
	if err != nil {
		return ResidencyRequest{}, err
	}
	endpoint, err := NormalizeEndpoint(ctx, req.Endpoint)
	if err != nil {
		return ResidencyRequest{}, err
	}
	region := strings.ToLower(strings.TrimSpace(req.Region))
	bucket := strings.TrimSpace(req.Bucket)
	if req.CatalogID == 0 || region == "" || bucket == "" {
		return ResidencyRequest{}, moerr.NewInvalidInput(ctx, "iceberg residency request requires catalog_id, region, and bucket")
	}
	req.CatalogURI = catalogURI
	req.Endpoint = endpoint
	req.Region = region
	req.Bucket = bucket
	return req, nil
}

func NormalizeCatalogResidencyRequest(ctx context.Context, req CatalogResidencyRequest) (CatalogResidencyRequest, error) {
	catalogURI, err := NormalizeCatalogURI(ctx, req.CatalogURI)
	if err != nil {
		return CatalogResidencyRequest{}, err
	}
	if req.CatalogID == 0 {
		return CatalogResidencyRequest{}, moerr.NewInvalidInput(ctx, "iceberg catalog residency request requires catalog_id")
	}
	req.CatalogURI = catalogURI
	return req, nil
}

func NormalizeEndpoint(ctx context.Context, endpoint string) (string, error) {
	value := strings.TrimSpace(endpoint)
	if value == "" || value == model.ResidencyWildcard {
		return "", moerr.NewInvalidInput(ctx, "iceberg residency endpoint must be an exact host")
	}
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" {
			return "", moerr.NewInvalidInput(ctx, "iceberg residency endpoint must be a valid host or URI host")
		}
		if u.Port() != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return "", moerr.NewInvalidInput(ctx, "iceberg residency endpoint must not include port, path, query, or fragment")
		}
		value = u.Hostname()
	}
	if strings.Contains(value, "/") {
		return "", moerr.NewInvalidInput(ctx, "iceberg residency endpoint must not include a path")
	}
	host := strings.ToLower(strings.TrimSuffix(value, "."))
	if host == "" || strings.Contains(host, ":") || net.ParseIP(host) != nil {
		return "", moerr.NewInvalidInput(ctx, "iceberg residency endpoint must be a DNS host without port")
	}
	return host, nil
}

func NormalizeCatalogURI(ctx context.Context, catalogURI string) (string, error) {
	value := strings.TrimSpace(catalogURI)
	if value == "" || value == model.ResidencyWildcard {
		return "", moerr.NewInvalidInput(ctx, "iceberg catalog uri must be exact and non-empty")
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return "", moerr.NewInvalidInput(ctx, "iceberg catalog uri must include scheme and host")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if u.Port() != "" {
		u.Host = net.JoinHostPort(host, u.Port())
	} else {
		u.Host = host
	}
	u.Fragment = ""
	return u.String(), nil
}

func residencyPolicyCatalogScopeMatches(ctx context.Context, policy model.ResidencyPolicy, catalogID uint64, catalogURI string) (bool, error) {
	if policy.CatalogID != 0 && policy.CatalogID != catalogID {
		return false, nil
	}
	allowedCatalogURI, err := NormalizeCatalogURI(ctx, policy.AllowedCatalogURI)
	if err != nil {
		return false, err
	}
	return allowedCatalogURI == catalogURI, nil
}

func residencyPolicyMatches(ctx context.Context, policy model.ResidencyPolicy, req ResidencyRequest) (bool, error) {
	if policy.CatalogID != 0 && policy.CatalogID != req.CatalogID {
		return false, nil
	}
	catalogURI, err := NormalizeCatalogURI(ctx, policy.AllowedCatalogURI)
	if err != nil {
		return false, err
	}
	endpoint, err := NormalizeEndpoint(ctx, policy.AllowedEndpoint)
	if err != nil {
		return false, err
	}
	if catalogURI != req.CatalogURI || endpoint != req.Endpoint {
		return false, nil
	}
	region := strings.ToLower(strings.TrimSpace(policy.AllowedRegion))
	bucket := strings.TrimSpace(policy.AllowedBucket)
	regionMatches := region == model.ResidencyWildcard || region == req.Region
	bucketMatches := bucket == model.ResidencyWildcard || bucket == req.Bucket
	return regionMatches && bucketMatches, nil
}

func normalizedPolicyState(state string) string {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return model.ResidencyPolicyEnabled
	}
	return state
}
