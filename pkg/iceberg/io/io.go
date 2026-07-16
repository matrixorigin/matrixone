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

package icebergio

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type ObjectIOProvider interface {
	Resolve(ctx context.Context, scope ObjectScope) (fileservice.ETLFileService, string, error)
	Refresh(ctx context.Context, scope ObjectScope) (ObjectScope, error)
	RedactPath(path string) string
}

type ScopedReader interface {
	Read(ctx context.Context, location string, offset, length int64) ([]byte, error)
}

type RemoteSigner interface {
	Sign(ctx context.Context, method, location string) (SignedRequest, error)
}

type SignedRequest struct {
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

type ResidencyValidator func(ctx context.Context, scope ObjectScope) error

type ScopedFileServiceBuilder func(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error)

type SignedFileServiceBuilder func(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error)

type ObjectScopeForLocation func(location string) ObjectScope

type ScopedProvider struct {
	FileService            fileservice.ETLFileService
	Now                    func() time.Time
	MinTTL                 time.Duration
	RefreshFunc            func(ctx context.Context, scope ObjectScope) (ObjectScope, error)
	ResidencyValidator     ResidencyValidator
	RequireResidencyPolicy bool
}

type VendedCredentialProvider struct {
	Credentials            []api.StorageCredential
	BuildFileService       ScopedFileServiceBuilder
	Now                    func() time.Time
	MinTTL                 time.Duration
	RefreshFunc            func(ctx context.Context, scope ObjectScope) (ObjectScope, error)
	ResidencyValidator     ResidencyValidator
	RequireResidencyPolicy bool
}

type RemoteSigningProvider struct {
	Signer                 RemoteSigner
	BuildFileService       SignedFileServiceBuilder
	Method                 string
	Now                    func() time.Time
	MinTTL                 time.Duration
	ResidencyValidator     ResidencyValidator
	RequireResidencyPolicy bool
}

type ProviderObjectReader struct {
	Provider         ObjectIOProvider
	ScopeForLocation ObjectScopeForLocation
}

type ProviderObjectWriter struct {
	Provider         ObjectIOProvider
	ScopeForLocation ObjectScopeForLocation
}

func (r ProviderObjectReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	if r.Provider == nil {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object reader requires ObjectIOProvider", nil))
	}
	if strings.TrimSpace(location) == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object reader requires location", nil))
	}
	if offset < 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object read offset must be non-negative", map[string]string{
			"location": r.Provider.RedactPath(location),
		}))
	}
	if length == 0 || length < -1 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object read length is invalid", map[string]string{
			"location": r.Provider.RedactPath(location),
		}))
	}
	fs, readPath, err := r.resolveRead(ctx, location)
	if err != nil {
		return nil, err
	}
	entry := fileservice.IOEntry{Offset: offset, Size: length}
	vec := fileservice.IOVector{
		FilePath: strings.TrimSpace(readPath),
		Policy:   fileservice.SkipFullFilePreloads,
		Entries:  []fileservice.IOEntry{entry},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg object read failed", map[string]string{
			"location": r.Provider.RedactPath(location),
		}, err))
	}
	if len(vec.Entries) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object read returned no entries", map[string]string{
			"location": r.Provider.RedactPath(location),
		}))
	}
	return append([]byte(nil), vec.Entries[0].Data...), nil
}

// ReadBounded streams an object into memory with a hard byte cap. Requesting a
// max-sized range is not equivalent: FileService correctly reports unexpected
// EOF when the object is shorter than that range.
func (r ProviderObjectReader) ReadBounded(ctx context.Context, location string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg bounded object read requires a positive limit", nil))
	}
	fs, readPath, err := r.resolveRead(ctx, location)
	if err != nil {
		return nil, err
	}
	var stream io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: strings.TrimSpace(readPath),
		Policy:   fileservice.SkipFullFilePreloads,
		Entries: []fileservice.IOEntry{{
			Offset:            0,
			Size:              -1,
			ReadCloserForRead: &stream,
		}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		if stream != nil {
			err = errors.Join(err, stream.Close())
		}
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg bounded object read failed", map[string]string{
			"location": r.Provider.RedactPath(location),
		}, err))
	}
	if stream == nil {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg bounded object read returned no stream", map[string]string{
			"location": r.Provider.RedactPath(location),
		}))
	}
	data, readErr := api.ReadAllBounded(stream, maxBytes)
	if err := errors.Join(readErr, stream.Close()); err != nil {
		if errors.Is(err, api.ErrMaterializationLimitExceeded) {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg object exceeds the materialization limit", map[string]string{
				"location":    r.Provider.RedactPath(location),
				"limit_bytes": strconv.FormatInt(maxBytes, 10),
			}))
		}
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg bounded object stream failed", map[string]string{
			"location": r.Provider.RedactPath(location),
		}, err))
	}
	return data, nil
}

func (r ProviderObjectReader) resolveRead(ctx context.Context, location string) (fileservice.ETLFileService, string, error) {
	if r.Provider == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object reader requires ObjectIOProvider", nil))
	}
	if strings.TrimSpace(location) == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object reader requires location", nil))
	}
	scope := ObjectScope{StorageLocation: strings.TrimSpace(location)}
	if r.ScopeForLocation != nil {
		scope = r.ScopeForLocation(location)
		if strings.TrimSpace(scope.StorageLocation) == "" {
			scope.StorageLocation = strings.TrimSpace(location)
		}
	}
	fs, readPath, err := r.Provider.Resolve(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	if fs == nil || strings.TrimSpace(readPath) == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object reader resolved empty FileService or path", map[string]string{
			"location": r.Provider.RedactPath(location),
		}))
	}
	return fs, readPath, nil
}

func (w ProviderObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	if w.Provider == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object writer requires ObjectIOProvider", nil))
	}
	if strings.TrimSpace(location) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object writer requires location", nil))
	}
	if len(payload) == 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object writer requires non-empty payload", map[string]string{
			"location": w.Provider.RedactPath(location),
		}))
	}
	scope := ObjectScope{StorageLocation: strings.TrimSpace(location)}
	if w.ScopeForLocation != nil {
		scope = w.ScopeForLocation(location)
		if strings.TrimSpace(scope.StorageLocation) == "" {
			scope.StorageLocation = strings.TrimSpace(location)
		}
	}
	fs, writePath, err := w.Provider.Resolve(ctx, scope)
	if err != nil {
		return err
	}
	if fs == nil || strings.TrimSpace(writePath) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg object writer resolved empty FileService or path", map[string]string{
			"location": w.Provider.RedactPath(location),
		}))
	}
	// FileService.Write consumes IOVector synchronously, so the caller-owned
	// payload remains valid for the complete call and does not need a defensive
	// clone. Iceberg materializers already retain it until this method returns.
	// Also skip S3's write-through disk-cache tee: it buffers the full object in
	// a second bytes.Buffer and would bypass the configured DML memory boundary.
	// A future streaming object-writer API can make this ownership contract more
	// explicit while retaining the same bounded behavior.
	vec := fileservice.IOVector{
		FilePath: strings.TrimSpace(writePath),
		Policy:   fileservice.SkipDiskCacheWrites,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}
	if err := fs.Write(ctx, vec); err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg object write failed", map[string]string{
			"location": w.Provider.RedactPath(location),
		}, err))
	}
	return nil
}

func (w ProviderObjectWriter) WriteManifestObject(ctx context.Context, location string, payload []byte) error {
	return w.WriteObject(ctx, location, payload)
}

func (p ScopedProvider) Resolve(ctx context.Context, scope ObjectScope) (fileservice.ETLFileService, string, error) {
	if p.FileService == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg ObjectIOProvider requires scoped file service", nil))
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	if err := CheckCredentialExpiration(ctx, canonical, p.now(), p.MinTTL); err != nil {
		return nil, "", err
	}
	if canonical.StorageLocation == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg ObjectScope storage location is required", nil))
	}
	if p.ResidencyValidator == nil {
		if p.RequireResidencyPolicy {
			return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "Iceberg ObjectIOProvider residency validator is required", map[string]string{
				"endpoint": canonical.Endpoint,
				"region":   canonical.Region,
				"bucket":   canonical.Bucket,
			}))
		}
	} else if err := p.ResidencyValidator(ctx, canonical); err != nil {
		return nil, "", err
	}
	return p.FileService, canonical.StorageLocation, nil
}

func (p VendedCredentialProvider) Resolve(ctx context.Context, scope ObjectScope) (fileservice.ETLFileService, string, error) {
	if p.BuildFileService == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg vended credential provider requires scoped FileService builder", nil))
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	if canonical.StorageLocation == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg ObjectScope storage location is required", nil))
	}
	credential, ok := selectStorageCredential(p.Credentials, canonical.StorageLocation)
	if !ok {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg object credential does not cover storage location", map[string]string{
			"storage_location": RedactObjectPath(canonical.StorageLocation),
		}))
	}
	if canonical.CredentialExpiresAt.IsZero() {
		canonical.CredentialExpiresAt = credential.ExpiresAt
	}
	if err := CheckCredentialExpiration(ctx, canonical, p.now(), p.MinTTL); err != nil {
		return nil, "", err
	}
	if p.ResidencyValidator == nil {
		if p.RequireResidencyPolicy {
			return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "Iceberg ObjectIOProvider residency validator is required", map[string]string{
				"endpoint": canonical.Endpoint,
				"region":   canonical.Region,
				"bucket":   canonical.Bucket,
			}))
		}
	} else if err := p.ResidencyValidator(ctx, canonical); err != nil {
		return nil, "", err
	}
	fs, readPath, err := p.BuildFileService(ctx, canonical, cloneStorageCredential(credential))
	if err != nil {
		return nil, "", api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg scoped FileService build failed", map[string]string{
			"storage_location": RedactObjectPath(canonical.StorageLocation),
		}, err))
	}
	if fs == nil || strings.TrimSpace(readPath) == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg scoped FileService builder returned empty result", nil))
	}
	return fs, strings.TrimSpace(readPath), nil
}

func (p RemoteSigningProvider) Resolve(ctx context.Context, scope ObjectScope) (fileservice.ETLFileService, string, error) {
	if p.Signer == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg remote signing provider requires signer", nil))
	}
	if p.BuildFileService == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg remote signing provider requires scoped FileService builder", nil))
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	if canonical.StorageLocation == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg ObjectScope storage location is required", nil))
	}
	if p.ResidencyValidator == nil {
		if p.RequireResidencyPolicy {
			return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "Iceberg remote signing residency validator is required", map[string]string{
				"endpoint": canonical.Endpoint,
				"region":   canonical.Region,
				"bucket":   canonical.Bucket,
			}))
		}
	} else if err := p.ResidencyValidator(ctx, canonical); err != nil {
		return nil, "", err
	}
	method := strings.TrimSpace(p.Method)
	if method == "" {
		method = "GET"
	}
	signed, err := p.Signer.Sign(ctx, method, canonical.StorageLocation)
	if err != nil {
		return nil, "", api.ToMOErr(ctx, api.WrapError(api.ErrRemoteSigningDenied, "Iceberg remote signing request was denied", map[string]string{
			"location": RedactObjectPath(canonical.StorageLocation),
		}, err))
	}
	signed = cloneSignedRequest(signed)
	if err := CheckSignedRequestExpiration(ctx, signed, p.now(), p.MinTTL); err != nil {
		return nil, "", err
	}
	fs, readPath, err := p.BuildFileService(ctx, canonical, signed)
	if err != nil {
		return nil, "", api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg remote signed FileService build failed", map[string]string{
			"signed_url": RedactObjectPath(signed.URL),
		}, err))
	}
	if fs == nil || strings.TrimSpace(readPath) == "" {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg remote signed FileService builder returned empty result", nil))
	}
	return fs, strings.TrimSpace(readPath), nil
}

func (p ScopedProvider) Refresh(ctx context.Context, scope ObjectScope) (ObjectScope, error) {
	if p.RefreshFunc == nil {
		return ObjectScope{}, api.ToMOErr(ctx, api.NewError(api.ErrCredentialExpired, "Iceberg object credential refresh is not configured", nil))
	}
	refreshed, err := p.RefreshFunc(ctx, scope)
	if err != nil {
		return ObjectScope{}, err
	}
	if err := CheckCredentialExpiration(ctx, refreshed, p.now(), p.MinTTL); err != nil {
		return ObjectScope{}, err
	}
	return refreshed, nil
}

func (p VendedCredentialProvider) Refresh(ctx context.Context, scope ObjectScope) (ObjectScope, error) {
	if p.RefreshFunc == nil {
		return ObjectScope{}, api.ToMOErr(ctx, api.NewError(api.ErrCredentialExpired, "Iceberg object credential refresh is not configured", nil))
	}
	refreshed, err := p.RefreshFunc(ctx, scope)
	if err != nil {
		return ObjectScope{}, err
	}
	if err := CheckCredentialExpiration(ctx, refreshed, p.now(), p.MinTTL); err != nil {
		return ObjectScope{}, err
	}
	return refreshed, nil
}

func (p RemoteSigningProvider) Refresh(ctx context.Context, scope ObjectScope) (ObjectScope, error) {
	if p.Signer == nil {
		return ObjectScope{}, api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg remote signing provider requires signer", nil))
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return ObjectScope{}, err
	}
	return canonical, nil
}

func (p ScopedProvider) RedactPath(path string) string {
	return RedactObjectPath(path)
}

func (p VendedCredentialProvider) RedactPath(path string) string {
	return RedactObjectPath(path)
}

func (p RemoteSigningProvider) RedactPath(path string) string {
	return RedactObjectPath(path)
}

func (p ScopedProvider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p VendedCredentialProvider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p RemoteSigningProvider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func CheckCredentialExpiration(ctx context.Context, scope ObjectScope, now time.Time, minTTL time.Duration) error {
	if scope.CredentialExpiresAt.IsZero() {
		return nil
	}
	if minTTL < 0 {
		minTTL = 0
	}
	if !now.Add(minTTL).Before(scope.CredentialExpiresAt) {
		return api.ToMOErr(ctx, api.NewError(api.ErrCredentialExpired, "Iceberg object credential is expired or too close to expiry", map[string]string{
			"credential_id": scope.CredentialID,
			"expires_at":    scope.CredentialExpiresAt.UTC().Format(time.RFC3339Nano),
		}))
	}
	return nil
}

func CheckSignedRequestExpiration(ctx context.Context, signed SignedRequest, now time.Time, minTTL time.Duration) error {
	if strings.TrimSpace(signed.URL) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg remote signer returned empty signed URL", nil))
	}
	if signed.ExpiresAt.IsZero() {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg remote signer returned signed request without expiration", map[string]string{
			"signed_url": RedactObjectPath(signed.URL),
		}))
	}
	if minTTL < 0 {
		minTTL = 0
	}
	if !now.Add(minTTL).Before(signed.ExpiresAt) {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningExpired, "Iceberg remote signed request is expired or too close to expiry", map[string]string{
			"signed_url": RedactObjectPath(signed.URL),
			"expires_at": signed.ExpiresAt.UTC().Format(time.RFC3339Nano),
		}))
	}
	return nil
}

func selectStorageCredential(credentials []api.StorageCredential, location string) (api.StorageCredential, bool) {
	location = strings.TrimSpace(location)
	bestPrefixLen := -1
	var best api.StorageCredential
	for _, credential := range credentials {
		prefix := strings.TrimSpace(credential.Prefix)
		if prefix != "" && !strings.HasPrefix(location, prefix) {
			continue
		}
		if len(prefix) <= bestPrefixLen {
			continue
		}
		bestPrefixLen = len(prefix)
		best = cloneStorageCredential(credential)
	}
	return best, bestPrefixLen >= 0
}

func cloneStorageCredential(in api.StorageCredential) api.StorageCredential {
	out := in
	if len(in.Config) > 0 {
		out.Config = make(map[string]string, len(in.Config))
		for k, v := range in.Config {
			out.Config[k] = v
		}
	}
	return out
}

func cloneSignedRequest(in SignedRequest) SignedRequest {
	out := in
	if len(in.Headers) > 0 {
		out.Headers = make(map[string]string, len(in.Headers))
		for k, v := range in.Headers {
			out.Headers[k] = v
		}
	}
	out.ExpiresAt = in.ExpiresAt.UTC()
	return out
}

func RedactObjectPath(path string) string {
	return api.RedactPath(redactionPath(path))
}

func redactionPath(path string) string {
	value := strings.TrimSpace(path)
	if value == "" {
		return ""
	}
	if u, err := url.Parse(value); err == nil && u.Scheme != "" && u.Host != "" {
		u.RawQuery = ""
		u.Fragment = ""
		return u.String()
	}
	return value
}

var _ ObjectIOProvider = ScopedProvider{}
var _ ObjectIOProvider = VendedCredentialProvider{}
var _ ObjectIOProvider = RemoteSigningProvider{}
