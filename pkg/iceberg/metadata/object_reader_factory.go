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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

type VendedObjectReaderFactory struct {
	BuildFileService       icebergio.ScopedFileServiceBuilder
	BuildSignedFileService icebergio.SignedFileServiceBuilder
	ResidencyValidator     icebergio.ResidencyValidator
	RequireResidencyPolicy bool
	Now                    func() time.Time
	MinTTL                 time.Duration
	PlanID                 string
	ReferencedBy           []string
}

func (f VendedObjectReaderFactory) NewObjectReader(ctx context.Context, catalog api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
	if catalog == nil {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrConfigInvalid, "Iceberg vended object reader requires catalog client", nil)
	}
	if f.BuildFileService == nil {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrConfigInvalid, "Iceberg vended object reader requires scoped FileService builder", nil)
	}
	creds, err := catalog.LoadCredentials(ctx, api.LoadCredentialsRequest{
		CatalogRequest: req.CatalogRequest,
		Namespace:      req.Namespace,
		Table:          req.Table,
		PlanID:         f.PlanID,
		ReferencedBy:   cloneStringSlice(f.ReferencedBy),
	})
	if err != nil {
		return nil, ObjectReaderContext{}, err
	}
	if creds == nil || len(creds.StorageCredentials) == 0 {
		return f.newRemoteSigningObjectReader(ctx, catalog, req)
	}
	return f.newVendedObjectReader(ctx, req, creds.StorageCredentials)
}

func (f VendedObjectReaderFactory) newVendedObjectReader(ctx context.Context, req api.ScanPlanRequest, credentials []api.StorageCredential) (api.ObjectReader, ObjectReaderContext, error) {
	baseScope := icebergio.ObjectScope{
		AccountID: req.Catalog.AccountID,
		CatalogID: req.Catalog.CatalogID,
		Endpoint:  "",
		Region:    "",
		Bucket:    "",
		Principal: req.ExternalPrincipal,
	}
	scopeForLocation := icebergio.S3ObjectScopeForLocation(baseScope, credentials)
	provider := icebergio.VendedCredentialProvider{
		Credentials:            cloneStorageCredentials(credentials),
		BuildFileService:       f.BuildFileService,
		Now:                    f.Now,
		MinTTL:                 f.MinTTL,
		ResidencyValidator:     combineObjectResidencyValidators(f.ResidencyValidator, objectResidencyValidatorFromRequest(req.ObjectResidencyValidator)),
		RequireResidencyPolicy: f.RequireResidencyPolicy,
	}
	objectIORef, err := icebergio.RegisterEphemeralObjectIOProvider(ctx, provider, scopeForLocation, objectIORefTTL(credentials, f.Now))
	if err != nil {
		return nil, ObjectReaderContext{}, err
	}
	credentialHash := api.PathHash(storageCredentialsIdentity(credentials))
	return icebergio.ProviderObjectReader{
			Provider:         provider,
			ScopeForLocation: scopeForLocation,
		}, ObjectReaderContext{
			CredentialHash:  credentialHash,
			CredentialScope: credentialHash,
			ObjectIORef:     objectIORef,
		}, nil
}

func (f VendedObjectReaderFactory) newRemoteSigningObjectReader(ctx context.Context, catalog api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error) {
	table, err := catalog.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: req.CatalogRequest,
		Namespace:      req.Namespace,
		Table:          req.Table,
		Snapshots:      "all",
		ReferencedBy:   cloneStringSlice(f.ReferencedBy),
	})
	if err != nil {
		return nil, ObjectReaderContext{}, err
	}
	if table == nil || !table.Capabilities.RemoteSigning {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrCredentialExpired, "Iceberg catalog did not return storage credentials", map[string]string{
			"catalog": req.Catalog.Name,
			"table":   req.Table,
		})
	}
	if f.BuildSignedFileService == nil {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrConfigInvalid, "Iceberg remote signing object reader requires signed FileService builder", nil)
	}
	signerFactory, ok := catalog.(interface {
		NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner
	})
	if !ok {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrRemoteSigningDenied, "Iceberg catalog client cannot create remote signer", map[string]string{
			"catalog": req.Catalog.Name,
			"table":   req.Table,
		})
	}
	signer := signerFactory.NewRemoteSigner(req.CatalogRequest, table.Config)
	if signer == nil {
		return nil, ObjectReaderContext{}, api.NewError(api.ErrRemoteSigningDenied, "Iceberg catalog client returned empty remote signer", map[string]string{
			"catalog": req.Catalog.Name,
			"table":   req.Table,
		})
	}
	baseScope := icebergio.ObjectScope{
		AccountID: req.Catalog.AccountID,
		CatalogID: req.Catalog.CatalogID,
		Principal: req.ExternalPrincipal,
	}
	scopeForLocation := icebergio.S3ObjectScopeForConfig(baseScope, table.Config)
	provider := icebergio.RemoteSigningProvider{
		Signer:                 signer,
		BuildFileService:       f.BuildSignedFileService,
		Method:                 "GET",
		Now:                    f.Now,
		MinTTL:                 f.MinTTL,
		ResidencyValidator:     combineObjectResidencyValidators(f.ResidencyValidator, objectResidencyValidatorFromRequest(req.ObjectResidencyValidator)),
		RequireResidencyPolicy: f.RequireResidencyPolicy,
	}
	objectIORef, err := icebergio.RegisterEphemeralObjectIOProvider(ctx, provider, scopeForLocation, 0)
	if err != nil {
		return nil, ObjectReaderContext{}, err
	}
	scopeHash := api.PathHash(remoteSigningIdentity(table.Config))
	return icebergio.ProviderObjectReader{
			Provider:         provider,
			ScopeForLocation: scopeForLocation,
		}, ObjectReaderContext{
			CredentialHash:  scopeHash,
			CredentialScope: scopeHash,
			ObjectIORef:     objectIORef,
		}, nil
}

func objectIORefTTL(credentials []api.StorageCredential, nowFunc func() time.Time) time.Duration {
	now := time.Now()
	if nowFunc != nil {
		now = nowFunc()
	}
	var ttl time.Duration
	for _, credential := range credentials {
		if credential.ExpiresAt.IsZero() {
			continue
		}
		candidate := credential.ExpiresAt.Sub(now)
		if candidate <= 0 {
			continue
		}
		if ttl == 0 || candidate < ttl {
			ttl = candidate
		}
	}
	return ttl
}

func objectResidencyValidatorFromRequest(validator api.ObjectResidencyValidator) icebergio.ResidencyValidator {
	if validator == nil {
		return nil
	}
	return func(ctx context.Context, scope icebergio.ObjectScope) error {
		return validator(ctx, api.ObjectResidencyRequest{
			AccountID:           scope.AccountID,
			CatalogID:           scope.CatalogID,
			Endpoint:            scope.Endpoint,
			Region:              scope.Region,
			Bucket:              scope.Bucket,
			StorageLocation:     scope.StorageLocation,
			Principal:           scope.Principal,
			CredentialID:        scope.CredentialID,
			CredentialExpiresAt: scope.CredentialExpiresAt,
		})
	}
}

func combineObjectResidencyValidators(validators ...icebergio.ResidencyValidator) icebergio.ResidencyValidator {
	out := make([]icebergio.ResidencyValidator, 0, len(validators))
	for _, validator := range validators {
		if validator != nil {
			out = append(out, validator)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return func(ctx context.Context, scope icebergio.ObjectScope) error {
		for _, validator := range out {
			if err := validator(ctx, scope); err != nil {
				return err
			}
		}
		return nil
	}
}

func storageCredentialsIdentity(credentials []api.StorageCredential) string {
	if len(credentials) == 0 {
		return ""
	}
	parts := make([]string, 0, len(credentials))
	for _, credential := range credentials {
		keys := make([]string, 0, len(credential.Config))
		for key := range credential.Config {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var part strings.Builder
		writeObjectReaderIdentityPart(&part, credential.Prefix)
		writeObjectReaderIdentityPart(&part, credential.ExpiresAt.UTC().Format(time.RFC3339Nano))
		for _, key := range keys {
			writeObjectReaderIdentityPart(&part, strings.ToLower(strings.TrimSpace(key)))
			writeObjectReaderIdentityPart(&part, strings.TrimSpace(credential.Config[key]))
		}
		parts = append(parts, part.String())
	}
	sort.Strings(parts)
	var out strings.Builder
	for _, part := range parts {
		writeObjectReaderIdentityPart(&out, part)
	}
	return out.String()
}

func remoteSigningIdentity(config map[string]string) string {
	if len(config) == 0 {
		return ""
	}
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		k := strings.ToLower(strings.TrimSpace(key))
		if !strings.HasPrefix(k, "s3.") && !strings.HasPrefix(k, "client.region") {
			continue
		}
		writeObjectReaderIdentityPart(&out, k)
		writeObjectReaderIdentityPart(&out, strings.TrimSpace(config[key]))
	}
	return out.String()
}

func writeObjectReaderIdentityPart(out *strings.Builder, value string) {
	out.WriteString(strconv.Itoa(len(value)))
	out.WriteByte(':')
	out.WriteString(value)
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}

var _ ObjectReaderFactory = VendedObjectReaderFactory{}.NewObjectReader
