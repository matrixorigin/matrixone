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

package compile

import (
	"context"
	"net/url"
	"os"
	"strings"
	"unicode"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

const IcebergTokenResolverRuntimeKey = "iceberg.token.resolver"
const IcebergRefCacheRefresherRuntimeKey = "iceberg.ref.cache.refresher"
const IcebergEnvTokenPrefix = "MO_ICEBERG_TOKEN_"
const IcebergAllowPlainHTTPEnv = "MO_ICEBERG_ALLOW_PLAIN_HTTP"

type IcebergTokenResolver interface {
	ResolveIcebergToken(ctx context.Context, secretRef string) (string, error)
}

type IcebergTokenRefresher interface {
	RefreshIcebergToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error)
}

func RegisterDefaultIcebergScanPlanner(ctx context.Context, serviceID string, params config.IcebergParameters) error {
	rt := moruntime.ServiceRuntime(serviceID)
	if rt == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg scan planner runtime is not available", map[string]string{
			"service": serviceID,
		}))
	}
	if _, ok := icebergTokenResolverFromRuntime(serviceID); !ok {
		rt.SetGlobalVariables(IcebergTokenResolverRuntimeKey, EnvIcebergTokenResolver{})
	}
	planner, err := NewDefaultIcebergScanPlanner(ctx, serviceID, params)
	if err != nil {
		return err
	}
	rt.SetGlobalVariables(IcebergScanPlannerRuntimeKey, planner)
	return nil
}

func NewDefaultIcebergScanPlanner(ctx context.Context, serviceID string, params config.IcebergParameters) (api.ScanPlanner, error) {
	cfg, err := api.NewConfigFromParameters(ctx, params)
	if err != nil {
		return nil, err
	}
	builder := icebergio.NewS3VendedFileServiceBuilder()
	restOptions := []icebergcatalog.RESTClientOption{
		icebergcatalog.WithTokenProvider(runtimeIcebergTokenProvider{serviceID: serviceID}),
	}
	if envFlagEnabled(os.Getenv(IcebergAllowPlainHTTPEnv)) {
		restOptions = append(restOptions, icebergcatalog.WithAllowPlainHTTP(true))
	}
	catalogFactory := icebergcatalog.NewFactory(
		icebergcatalog.WithNativeRESTOptions(restOptions...),
		icebergcatalog.WithAdapter(
			icebergcatalog.AdapterIcebergGo,
			icebergcatalog.UnsupportedAdapterFactory{Name: icebergcatalog.AdapterIcebergGo},
		),
	)
	cache := icebergmetadata.NewCache(cfg.Scan.ManifestCacheTTL)
	if rt := moruntime.ServiceRuntime(serviceID); rt != nil {
		rt.SetGlobalVariables(api.CacheInvalidatorRuntimeKey, cache)
	}
	planner := icebergmetadata.RuntimeScanPlanner{
		CatalogFactory: catalogFactory,
		Metadata:       icebergmetadata.NativeFacade{},
		ObjectReader: icebergmetadata.VendedObjectReaderFactory{
			BuildFileService:       builder.Build,
			BuildSignedFileService: icebergio.SignedHTTPFileServiceBuilder{}.Build,
			RequireResidencyPolicy: true,
		}.NewObjectReader,
		Cache:  cache,
		Config: cfg,
	}
	if refresher, ok := icebergRefCacheRefresherFromRuntime(serviceID); ok {
		planner.RefCacheRefresher = refresher
	}
	return planner, nil
}

func IcebergAllowPlainHTTPFromEnv() bool {
	return envFlagEnabled(os.Getenv(IcebergAllowPlainHTTPEnv))
}

func envFlagEnabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type runtimeIcebergTokenProvider struct {
	serviceID string
}

func NewRuntimeIcebergTokenProvider(serviceID string) icebergcatalog.TokenProvider {
	return runtimeIcebergTokenProvider{serviceID: serviceID}
}

func (p runtimeIcebergTokenProvider) ResolveToken(ctx context.Context, catalog model.Catalog) (string, error) {
	secretRef := strings.TrimSpace(catalog.TokenSecretRef)
	if secretRef == "" {
		return "", nil
	}
	resolver, ok := icebergTokenResolverFromRuntime(p.serviceID)
	if !ok {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST token resolver is not configured", map[string]string{
			"catalog":          catalog.Name,
			"token_secret_ref": secretRef,
		})
	}
	token, err := resolver.ResolveIcebergToken(ctx, secretRef)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(token) == "" {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST token resolver returned empty token", map[string]string{
			"catalog":          catalog.Name,
			"token_secret_ref": secretRef,
		})
	}
	return strings.TrimSpace(token), nil
}

func (p runtimeIcebergTokenProvider) RefreshToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error) {
	resolver, ok := icebergTokenResolverFromRuntime(p.serviceID)
	if !ok {
		return "", false, nil
	}
	refresher, ok := resolver.(IcebergTokenRefresher)
	if !ok {
		return "", false, nil
	}
	return refresher.RefreshIcebergToken(ctx, req, previousToken)
}

func icebergTokenResolverFromRuntime(serviceID string) (IcebergTokenResolver, bool) {
	rt := moruntime.ServiceRuntime(serviceID)
	if rt == nil {
		return nil, false
	}
	value, ok := rt.GetGlobalVariables(IcebergTokenResolverRuntimeKey)
	if !ok || value == nil {
		return nil, false
	}
	resolver, ok := value.(IcebergTokenResolver)
	return resolver, ok
}

func icebergRefCacheRefresherFromRuntime(serviceID string) (icebergmetadata.RefCacheRefresher, bool) {
	rt := moruntime.ServiceRuntime(serviceID)
	if rt == nil {
		return nil, false
	}
	value, ok := rt.GetGlobalVariables(IcebergRefCacheRefresherRuntimeKey)
	if !ok || value == nil {
		return nil, false
	}
	refresher, ok := value.(icebergmetadata.RefCacheRefresher)
	return refresher, ok
}

type EnvIcebergTokenResolver struct {
	Prefix    string
	LookupEnv func(string) (string, bool)
}

func (r EnvIcebergTokenResolver) ResolveIcebergToken(ctx context.Context, secretRef string) (string, error) {
	envName, err := IcebergTokenEnvName(ctx, secretRef, r.Prefix)
	if err != nil {
		return "", err
	}
	lookup := r.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	token, ok := lookup(envName)
	if !ok || strings.TrimSpace(token) == "" {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST token secret is not available", map[string]string{
			"token_secret_ref": secretRef,
			"env_var":          envName,
		})
	}
	return strings.TrimSpace(token), nil
}

func IcebergTokenEnvName(ctx context.Context, secretRef string, prefix string) (string, error) {
	secretRef = strings.TrimSpace(secretRef)
	if secretRef == "" {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST token secret ref is required", nil)
	}
	parsed, err := url.Parse(secretRef)
	if err != nil || strings.ToLower(parsed.Scheme) != "secret" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST token secret ref must use secret://", map[string]string{
			"token_secret_ref": secretRef,
		})
	}
	secretName := strings.Trim(strings.TrimSpace(parsed.Host)+"/"+strings.Trim(parsed.Path, "/"), "/")
	if secretName == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST token secret ref requires a secret name", map[string]string{
			"token_secret_ref": secretRef,
		})
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = IcebergEnvTokenPrefix
	}
	suffix := sanitizeIcebergTokenEnvSuffix(secretName)
	if suffix == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST token secret ref has no usable environment suffix", map[string]string{
			"token_secret_ref": secretRef,
		})
	}
	return prefix + suffix, nil
}

func sanitizeIcebergTokenEnvSuffix(secretName string) string {
	var b strings.Builder
	previousUnderscore := false
	for _, r := range secretName {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToUpper(r))
			previousUnderscore = false
			continue
		}
		if !previousUnderscore {
			b.WriteByte('_')
			previousUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

var _ icebergcatalog.TokenProvider = runtimeIcebergTokenProvider{}
var _ IcebergTokenResolver = EnvIcebergTokenResolver{}
