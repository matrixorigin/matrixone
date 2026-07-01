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
	"net"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type S3FileServiceFactory func(ctx context.Context, args fileservice.ObjectStorageArguments) (fileservice.ETLFileService, error)

type S3VendedFileServiceBuilder struct {
	NewFileService          S3FileServiceFactory
	CacheConfig             fileservice.CacheConfig
	EnableCache             bool
	AllowDefaultCredentials bool
	ValidateBucket          bool
}

func NewS3VendedFileServiceBuilder() S3VendedFileServiceBuilder {
	return S3VendedFileServiceBuilder{
		CacheConfig: fileservice.DisabledCacheConfig,
	}
}

func (b S3VendedFileServiceBuilder) Build(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
	args, readPath, err := BuildS3ObjectStorageArguments(ctx, scope, credential)
	if err != nil {
		return nil, "", err
	}
	args.NoBucketValidation = !b.ValidateBucket
	args.NoDefaultCredentials = !b.AllowDefaultCredentials
	factory := b.NewFileService
	if factory == nil {
		factory = func(ctx context.Context, args fileservice.ObjectStorageArguments) (fileservice.ETLFileService, error) {
			return fileservice.NewS3FS(ctx, args, b.CacheConfig, nil, !b.EnableCache, !b.AllowDefaultCredentials)
		}
	}
	fs, err := factory(ctx, args)
	if err != nil {
		return nil, "", api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg S3 FileService build failed", map[string]string{
			"storage_location": RedactObjectPath(scope.StorageLocation),
		}, err))
	}
	if fs == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg S3 FileService builder returned nil", nil))
	}
	return fs, readPath, nil
}

func BuildS3ObjectStorageArguments(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ObjectStorageArguments, string, error) {
	location, err := parseS3Location(ctx, scope.StorageLocation)
	if err != nil {
		return fileservice.ObjectStorageArguments{}, "", err
	}
	if scope.Bucket != "" && location.bucket != "" && !strings.EqualFold(scope.Bucket, location.bucket) {
		return fileservice.ObjectStorageArguments{}, "", moerr.NewInvalidInput(ctx, "iceberg S3 object scope bucket does not match storage location")
	}
	bucket := firstNonEmpty(scope.Bucket, location.bucket)
	if bucket == "" {
		return fileservice.ObjectStorageArguments{}, "", moerr.NewInvalidInput(ctx, "iceberg S3 FileService requires bucket")
	}

	prefixLocation, err := parseOptionalS3Prefix(ctx, credential.Prefix)
	if err != nil {
		return fileservice.ObjectStorageArguments{}, "", err
	}
	keyPrefix := ""
	if prefixLocation.bucket != "" {
		if !strings.EqualFold(prefixLocation.bucket, bucket) {
			return fileservice.ObjectStorageArguments{}, "", moerr.NewInvalidInput(ctx, "iceberg S3 credential prefix bucket does not match object scope")
		}
		keyPrefix = prefixLocation.key
	}
	readPath := location.key
	if keyPrefix != "" {
		if readPath == keyPrefix {
			readPath = ""
		} else if strings.HasPrefix(readPath, keyPrefix+"/") {
			readPath = strings.TrimPrefix(readPath, keyPrefix+"/")
		}
	}
	readPath = strings.TrimLeft(path.Clean("/"+readPath), "/")
	if readPath == "." {
		readPath = ""
	}
	if readPath == "" {
		return fileservice.ObjectStorageArguments{}, "", moerr.NewInvalidInput(ctx, "iceberg S3 FileService requires object key")
	}

	cfg := normalizedCredentialConfig(credential.Config)
	args := fileservice.ObjectStorageArguments{
		Name:                 "iceberg-s3-" + api.PathHash(scope.StorageLocation),
		Bucket:               bucket,
		Endpoint:             firstNonEmpty(cfg["s3.endpoint"], scope.Endpoint),
		Region:               firstNonEmpty(cfg["s3.region"], cfg["client.region"], scope.Region),
		KeyID:                firstNonEmpty(cfg["s3.access-key-id"], cfg["s3.access_key_id"], cfg["aws.access-key-id"]),
		KeySecret:            firstNonEmpty(cfg["s3.secret-access-key"], cfg["s3.secret_access_key"], cfg["aws.secret-access-key"]),
		SessionToken:         firstNonEmpty(cfg["s3.session-token"], cfg["s3.session_token"], cfg["aws.session-token"]),
		SecurityToken:        firstNonEmpty(cfg["s3.security-token"], cfg["s3.security_token"]),
		KeyPrefix:            keyPrefix,
		NoDefaultCredentials: true,
		NoBucketValidation:   true,
	}
	if parseBoolLike(cfg["s3.path-style-access"]) || parseBoolLike(cfg["s3.path_style_access"]) || parseBoolLike(cfg["s3.is-minio"]) || parseBoolLike(cfg["s3.is_minio"]) {
		args.IsMinio = true
	}
	if args.Endpoint == "" || args.Region == "" {
		return fileservice.ObjectStorageArguments{}, "", moerr.NewInvalidInput(ctx, "iceberg S3 FileService requires endpoint and region")
	}
	if args.KeyID == "" || args.KeySecret == "" {
		return fileservice.ObjectStorageArguments{}, "", api.ToMOErr(ctx, api.NewError(api.ErrCredentialExpired, "Iceberg S3 vended credential is missing access key or secret key", map[string]string{
			"storage_location": RedactObjectPath(scope.StorageLocation),
		}))
	}
	return args, readPath, nil
}

func S3ObjectScopeForLocation(base ObjectScope, credentials []api.StorageCredential) ObjectScopeForLocation {
	return func(location string) ObjectScope {
		scope := base
		scope.StorageLocation = strings.TrimSpace(location)
		if parsed, err := parseS3Location(context.Background(), location); err == nil && parsed.bucket != "" {
			scope.Bucket = parsed.bucket
		}
		credential, ok := selectStorageCredential(credentials, location)
		if !ok {
			return scope
		}
		cfg := normalizedCredentialConfig(credential.Config)
		scope.Endpoint = firstNonEmpty(objectScopeEndpointFromS3Endpoint(cfg["s3.endpoint"]), scope.Endpoint)
		scope.Region = firstNonEmpty(cfg["s3.region"], cfg["client.region"], scope.Region)
		scope.CredentialExpiresAt = credential.ExpiresAt
		scope.CredentialID = api.PathHash(storageCredentialIdentity(credential))
		return scope
	}
}

func S3ObjectScopeForConfig(base ObjectScope, config map[string]string) ObjectScopeForLocation {
	cfg := normalizedCredentialConfig(config)
	return func(location string) ObjectScope {
		scope := base
		scope.StorageLocation = strings.TrimSpace(location)
		if parsed, err := parseS3Location(context.Background(), location); err == nil && parsed.bucket != "" {
			scope.Bucket = parsed.bucket
		}
		scope.Endpoint = firstNonEmpty(objectScopeEndpointFromS3Endpoint(cfg["s3.endpoint"]), scope.Endpoint)
		scope.Region = firstNonEmpty(cfg["s3.region"], cfg["client.region"], scope.Region)
		scope.CredentialID = api.PathHash(remoteSigningConfigIdentity(cfg))
		return scope
	}
}

func BuildS3RemoteSigningRequestURI(ctx context.Context, location string, config map[string]string) (string, string, error) {
	parsed, err := parseS3Location(ctx, location)
	if err != nil {
		return "", "", err
	}
	if parsed.bucket == "" || parsed.key == "" {
		return "", "", moerr.NewInvalidInput(ctx, "iceberg S3 remote signing requires bucket and object key")
	}
	cfg := normalizedCredentialConfig(config)
	endpoint := firstNonEmpty(cfg["s3.endpoint"])
	region := firstNonEmpty(cfg["s3.region"], cfg["client.region"])
	if endpoint == "" || region == "" {
		return "", "", moerr.NewInvalidInput(ctx, "iceberg S3 remote signing requires endpoint and region")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", moerr.NewInvalidInput(ctx, "iceberg S3 remote signing endpoint is invalid")
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	if u.Host == "" {
		u.Host = u.Path
		u.Path = ""
	}
	if u.Host == "" {
		return "", "", moerr.NewInvalidInput(ctx, "iceberg S3 remote signing endpoint requires host")
	}
	if remoteSigningUsePathStyle(u.Host, cfg) {
		setEscapedS3Path(u, path.Join(u.Path, parsed.bucket, parsed.key))
	} else {
		u.Host = parsed.bucket + "." + u.Host
		setEscapedS3Path(u, path.Join(u.Path, parsed.key))
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), region, nil
}

func setEscapedS3Path(u *url.URL, value string) {
	u.Path = path.Clean("/" + strings.TrimLeft(value, "/"))
	if u.Path == "." {
		u.Path = "/"
	}
	escaped := escapePathPreservingSlash(u.Path)
	if escaped != u.Path {
		u.RawPath = escaped
	} else {
		u.RawPath = ""
	}
}

func escapePathPreservingSlash(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = escapeS3PathSegment(parts[i])
	}
	return strings.Join(parts, "/")
}

func escapeS3PathSegment(value string) string {
	if value == "" {
		return ""
	}
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if isS3CanonicalPathUnreserved(c) {
			out.WriteByte(c)
			continue
		}
		const upperhex = "0123456789ABCDEF"
		out.WriteByte('%')
		out.WriteByte(upperhex[c>>4])
		out.WriteByte(upperhex[c&0x0f])
	}
	return out.String()
}

func isS3CanonicalPathUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '.' || c == '_' || c == '~'
}

func remoteSigningUsePathStyle(host string, cfg map[string]string) bool {
	if parseBoolLike(cfg["s3.path-style-access"]) || parseBoolLike(cfg["s3.path_style_access"]) ||
		parseBoolLike(cfg["s3.is-minio"]) || parseBoolLike(cfg["s3.is_minio"]) {
		return true
	}
	hostOnly := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hostOnly = h
	}
	hostOnly = strings.ToLower(strings.Trim(hostOnly, "[]"))
	return hostOnly == "localhost" || hostOnly == "127.0.0.1" || hostOnly == "::1"
}

func objectScopeEndpointFromS3Endpoint(endpoint string) string {
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return ""
	}
	if u, err := url.Parse(value); err == nil && u.Hostname() != "" {
		value = u.Hostname()
	} else if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	value = strings.ToLower(strings.Trim(strings.TrimSuffix(value, "."), "[]"))
	if ip := net.ParseIP(value); ip != nil && ip.IsLoopback() {
		return "localhost"
	}
	return value
}

type s3Location struct {
	bucket string
	key    string
}

func parseS3Location(ctx context.Context, raw string) (s3Location, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return s3Location{}, moerr.NewInvalidInput(ctx, "iceberg S3 location is required")
	}
	u, err := url.Parse(value)
	if err == nil && strings.EqualFold(u.Scheme, "s3") {
		key := strings.TrimLeft(path.Clean("/"+u.Path), "/")
		if key == "." {
			key = ""
		}
		return s3Location{bucket: u.Host, key: key}, nil
	}
	return s3Location{key: strings.TrimLeft(path.Clean("/"+value), "/")}, nil
}

func parseOptionalS3Prefix(ctx context.Context, raw string) (s3Location, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return s3Location{}, nil
	}
	u, err := url.Parse(value)
	if err == nil && strings.EqualFold(u.Scheme, "s3") {
		key := strings.TrimLeft(path.Clean("/"+u.Path), "/")
		if key == "." {
			key = ""
		}
		return s3Location{bucket: u.Host, key: key}, nil
	}
	return s3Location{key: strings.TrimLeft(path.Clean("/"+value), "/")}, nil
}

func normalizedCredentialConfig(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return out
}

func parseBoolLike(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}

func storageCredentialIdentity(credential api.StorageCredential) string {
	keys := make([]string, 0, len(credential.Config))
	for key := range credential.Config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString(credential.Prefix)
	out.WriteByte('|')
	out.WriteString(credential.ExpiresAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
	for _, key := range keys {
		out.WriteByte('|')
		out.WriteString(strings.ToLower(strings.TrimSpace(key)))
		out.WriteByte('=')
		out.WriteString(strings.TrimSpace(credential.Config[key]))
	}
	return out.String()
}

func remoteSigningConfigIdentity(config map[string]string) string {
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
		out.WriteString(strings.ToLower(strings.TrimSpace(key)))
		out.WriteByte('=')
		out.WriteString(strings.TrimSpace(config[key]))
		out.WriteByte('|')
	}
	return out.String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

var _ ScopedFileServiceBuilder = NewS3VendedFileServiceBuilder().Build
