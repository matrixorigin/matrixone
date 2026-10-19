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

package fileservice

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLFileFactoryOwnsAdmission(t *testing.T) {
	local := filepath.Join(t.TempDir(), "uncreated", "object")
	remote := "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret,no-default-credentials=false:object"
	for _, ctx := range []context.Context{nil, context.Background(), context.TODO()} {
		for _, path := range []string{local, remote} {
			fs, _, err := GetForETL(ctx, nil, path)
			require.Nil(t, fs)
			require.ErrorContains(t, err, "policy is required")
		}
	}
	ctx := WithSQLFileAccess(t.Context(), "tenant", "public")
	fs, _, err := GetForETL(ctx, nil, local) // No process/frontend guard.
	require.Nil(t, fs)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = os.Stat(filepath.Dir(local))
	require.ErrorIs(t, err, os.ErrNotExist)
	admin := WithSQLFileAccess(t.Context(), "SYS", "MOADMIN")
	canceled, cancel := context.WithCancel(admin)
	cancel()
	fs, _, err = GetForETL(canceled, nil, local)
	require.Nil(t, fs)
	require.ErrorIs(t, err, context.Canceled)
	backup, err := GetForBackup(canceled, local, "DISK-V2")
	require.Nil(t, backup)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(filepath.Dir(local))
	require.ErrorIs(t, err, os.ErrNotExist)
	fs, path, err := GetForETL(admin, nil, local)
	require.NoError(t, err)
	require.Equal(t, "object", path)
	fs.Close(admin)
	for _, pair := range [][2]string{{local, remote}, {remote, local}, {remote, strings.Replace(remote, "caller-key", "other-key", 1)}} {
		parsed, e := ParsePath(pair[1])
		require.NoError(t, e)
		fs, _, e = GetForETLWithParsedPath(admin, nil, pair[0], parsed)
		require.Nil(t, fs)
		require.ErrorContains(t, e, "disagrees")
	}
}

func TestSQLFileProviderResources(t *testing.T) {
	base := "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret"
	owned := filepath.Join(t.TempDir(), "owned-auxiliary")
	require.NoError(t, os.WriteFile(owned, []byte("owned canary"), 0600))
	for _, identity := range [][2]string{{"sys", "moadmin"}, {"sys", "public"}, {"tenant", "accountadmin"}, {"", ""}} {
		ctx := WithSQLFileAccess(t.Context(), identity[0], identity[1])
		for _, spec := range []string{base + ",cert-files=" + owned, base + ",kerberos-keytab-path=" + owned, "hdfs,endpoint=owned.invalid,user=caller,kerberos-keytab-path=" + owned, base + ",shared-config-profile=owned", base + ",ram-role=owned"} {
			fs, _, err := GetForETL(ctx, nil, spec+":object")
			require.Nil(t, fs)
			require.Error(t, err, "all SQL roles reject auxiliary/default sources")
		}
	}
	for _, tc := range []struct {
		opts    string
		allowed bool
	}{
		{base, true},
		{base + ",role-arn=owned-role", true}, // Classification does not contact STS.
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,no-default-credentials=true", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,is-minio=true,no-default-credentials=true", true},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,is-minio=true", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,is-minio=true,no-default-credentials=true,key=partial", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,is-minio=true,no-default-credentials=true,token=partial", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,bearer-token=owned", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,key=caller-key", false},
		{"s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,role-arn=owned-role", false},
		{"s3-opts,endpoint=https://owned.myqcloud.com,bucket=owned-bucket-123456,key=k,secret=s,role-arn=owned", false},
		{"s3-opts,endpoint=https://owned.aliyuncs.com,bucket=owned-bucket-123456,bearer-token=owned", true},
		{"hdfs,endpoint=owned.invalid,user=caller", true},
		{"hdfs,endpoint=owned.invalid", true},
		{"hdfs,endpoint=owned.invalid,kerberos-username=caller,kerberos-realm=owned,kerberos-password=owned", true},
	} {
		path, err := ParsePath(tc.opts + ":object")
		require.NoError(t, err)
		server, err := SQLFilePathRequiresAdmin(path)
		if tc.allowed {
			require.NoError(t, err)
			require.False(t, server)
		} else {
			require.Error(t, err)
		}
	}
}

type sqlCredentialTransport func(*http.Request) (*http.Response, error)

func (f sqlCredentialTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSQLFileCredentialsDoNotDiscoverServerIdentity(t *testing.T) {
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "MINIO_ROOT_USER", "TENCENTCLOUD_SECRETID"} {
		t.Setenv(name, "ambient-key")
	}
	for _, name := range []string{"AWS_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD", "TENCENTCLOUD_SECRETKEY"} {
		t.Setenv(name, "ambient-secret")
	}
	for _, name := range []string{"AWS_SESSION_TOKEN", "TENCENTCLOUD_SESSIONTOKEN"} {
		t.Setenv(name, "ambient-token")
	}
	invalidConfig := filepath.Join(t.TempDir(), "owned-invalid-config")
	require.NoError(t, os.WriteFile(invalidConfig, []byte("[invalid"), 0600))
	t.Setenv("AWS_CONFIG_FILE", invalidConfig)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", invalidConfig)
	t.Setenv("AWS_PROFILE", "owned-missing-profile")
	old := httpRoundTripper
	t.Cleanup(func() { httpRoundTripper = old })
	calls := 0
	anonymous := false
	httpRoundTripper = sqlCredentialTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		assert.Equal(t, http.MethodHead, r.Method)
		if anonymous {
			assert.Empty(t, r.Header.Get("Authorization"))
		} else {
			assert.Contains(t, r.Header.Get("Authorization"), "caller-key")
		}
		assert.NotContains(t, r.Header.Get("Authorization"), "ambient")
		assert.Empty(t, r.Header.Get("X-Amz-Security-Token"))
		assert.Empty(t, r.Header.Get("X-Cos-Security-Token"))
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": []string{"0"}, "Last-Modified": []string{"Mon, 02 Jan 2006 15:04:05 GMT"}, "Etag": []string{`"owned"`}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})
	ctx := WithSQLFileAccess(t.Context(), "tenant", "public")
	for _, endpoint := range []string{"https://owned.invalid", "https://owned.invalid,is-minio=true", "https://owned.myqcloud.com"} {
		spec := "s3-opts,endpoint=" + endpoint + ",region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret,no-default-credentials=false:object"
		fs, path, err := GetForETL(ctx, nil, spec)
		require.NoError(t, err)
		_, err = fs.StatFile(ctx, path)
		fs.Close(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, 3, calls)
	// The same broken server configuration is still observed by the native owner.
	_, err := NewAwsSDKv2(t.Context(), ObjectStorageArguments{Endpoint: "https://owned.invalid", Region: "us-east-1", Bucket: "b", KeyID: "caller-key", KeySecret: "caller-secret", NoBucketValidation: true}, nil)
	require.Error(t, err)
	require.Equal(t, 3, calls)
	anonymous = true
	fs, path, err := GetForETL(ctx, nil, "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,is-minio=true,no-default-credentials=true:object")
	require.NoError(t, err)
	defer fs.Close(ctx)
	_, err = fs.StatFile(ctx, path)
	require.NoError(t, err)
	require.Equal(t, 4, calls)
}

func TestSQLFileS3UsesEnvironmentRegionWhenNotSpecified(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-2")
	t.Setenv("AWS_DEFAULT_REGION", "us-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	old := httpRoundTripper
	t.Cleanup(func() { httpRoundTripper = old })
	calls := 0
	httpRoundTripper = sqlCredentialTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		assert.Equal(t, http.MethodHead, r.Method)
		authorization := r.Header.Get("Authorization")
		assert.Contains(t, authorization, "Credential=caller-key/")
		assert.Contains(t, authorization, "/us-east-2/s3/aws4_request")
		assert.NotContains(t, authorization, "ambient")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Length": []string{"0"}, "Last-Modified": []string{"Mon, 02 Jan 2006 15:04:05 GMT"}, "Etag": []string{`"owned"`}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    r,
		}, nil
	})

	ctx := WithSQLFileAccess(t.Context(), "tenant", "public")
	spec := "s3-opts,endpoint=https://owned.invalid,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret:object"
	fs, path, err := GetForETL(ctx, nil, spec)
	require.NoError(t, err)
	defer fs.Close(ctx)
	_, err = fs.StatFile(ctx, path)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestExplicitAWSConfigRegionFallback(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-2")
	t.Setenv("AWS_DEFAULT_REGION", "us-west-1")
	require.Equal(t, "us-east-2", explicitAWSConfig(ObjectStorageArguments{}, nil, nil).Region)

	t.Setenv("AWS_REGION", "")
	require.Equal(t, "us-west-1", explicitAWSConfig(ObjectStorageArguments{}, nil, nil).Region)

	require.Equal(t, "eu-west-1", explicitAWSConfig(ObjectStorageArguments{Region: "eu-west-1"}, nil, nil).Region)
}

func TestSQLFileRoleFailureCannotFallBack(t *testing.T) {
	old := httpRoundTripper
	t.Cleanup(func() { httpRoundTripper = old })
	posts, objects := 0, 0
	httpRoundTripper = sqlCredentialTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost {
			posts++
		} else {
			objects++
		}
		return &http.Response{StatusCode: 403, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<Error><Code>AccessDenied</Code><Message>owned denial</Message></Error>`)), Request: r}, nil
	})
	ctx := WithSQLFileAccess(t.Context(), "tenant", "public")
	for _, extra := range []string{"", ",is-minio=true"} {
		fs, _, err := GetForETL(ctx, nil, "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret,role-arn=owned-role"+extra+":object")
		require.Nil(t, fs)
		require.Error(t, err)
	}
	require.Equal(t, 2, posts)
	require.Zero(t, objects)
	args := ObjectStorageArguments{NoDefaultCredentials: true, RoleARN: "owned-role"}
	provider, err := args.credentialsProviderForAwsSDKv2(t.Context())
	require.Nil(t, provider)
	require.Error(t, err)
	require.Equal(t, 2, posts)
}

func TestSQLFileRoleAcquisitionHonorsCancellation(t *testing.T) {
	old := httpRoundTripper
	t.Cleanup(func() { httpRoundTripper = old })
	for _, extra := range []string{"", ",is-minio=true"} {
		ctx, cancel := context.WithCancel(WithSQLFileAccess(t.Context(), "tenant", "public"))
		t.Cleanup(cancel)
		calls := 0
		observed := make(chan error, 1)
		httpRoundTripper = sqlCredentialTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			cancel() // Cancel after acquisition has reached the credential request.
			observed <- r.Context().Err()
			return nil, context.Canceled
		})
		fs, _, err := GetForETL(ctx, nil, "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret,role-arn=owned-role"+extra+":object")
		cancel()
		require.Nil(t, fs)
		require.ErrorIs(t, err, context.Canceled)
		select {
		case err := <-observed:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("credential request did not reach cancellation barrier")
		}
		require.Equal(t, 1, calls)
	}
}

func TestSQLFileRoleUsesExplicitUpstreamOnRefresh(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_SESSION_TOKEN", "ambient-token")
	invalid := filepath.Join(t.TempDir(), "owned-config")
	require.NoError(t, os.WriteFile(invalid, []byte("[invalid"), 0600))
	t.Setenv("AWS_CONFIG_FILE", invalid)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", invalid)
	old := httpRoundTripper
	t.Cleanup(func() { httpRoundTripper = old })
	posts, objects := 0, 0
	httpRoundTripper = sqlCredentialTransport(func(r *http.Request) (*http.Response, error) {
		body := ""
		if r.Method == http.MethodPost {
			posts++
			assert.Contains(t, r.Header.Get("Authorization"), "caller-key")
			assert.Equal(t, "caller-token", r.Header.Get("X-Amz-Security-Token"))
			body = `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>assumed-key</AccessKeyId><SecretAccessKey>assumed-secret</SecretAccessKey><SessionToken>assumed-token</SessionToken><Expiration>` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>`
		} else {
			objects++
			assert.Equal(t, http.MethodHead, r.Method)
			assert.Contains(t, r.Header.Get("Authorization"), "assumed-key")
			assert.Equal(t, "assumed-token", r.Header.Get("X-Amz-Security-Token"))
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Length": []string{"0"}, "Last-Modified": []string{"Mon, 02 Jan 2006 15:04:05 GMT"}, "Etag": []string{`"owned"`}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	ctx := WithSQLFileAccess(t.Context(), "tenant", "public")
	for _, extra := range []string{"", ",is-minio=true"} {
		fs, path, err := GetForETL(ctx, nil, "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned-bucket-123456,key=caller-key,secret=caller-secret,token=caller-token,role-arn=owned-role"+extra+":object")
		require.NoError(t, err)
		_, err = fs.StatFile(ctx, path)
		require.NoError(t, err)
		if sdk, ok := fs.(*S3FS).rawStorage.(*AwsSDKv2); ok {
			cache, ok := sdk.client.Options().Credentials.(*aws.CredentialsCache)
			require.True(t, ok)
			cache.Invalidate()
			_, err = fs.StatFile(ctx, path)
			require.NoError(t, err)
		}
		fs.Close(ctx)
	}
	require.Equal(t, 3, posts)
	require.Equal(t, 3, objects)
}

func TestSQLBackupFactoryOwnsAdmission(t *testing.T) {
	local := filepath.Join(t.TempDir(), "uncreated", "object")
	fs, err := GetForBackup(t.Context(), local, "")
	require.Nil(t, fs)
	require.ErrorContains(t, err, "policy is required")
	ctx := WithSQLFileAccess(t.Context(), "tenant", "accountadmin")
	fs, err = GetForBackup(ctx, local, "")
	require.Nil(t, fs)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	admin := WithSQLFileAccess(t.Context(), "sys", "moadmin")
	// Both factories construct from admitted arguments, while preserving their
	// existing HDFS option semantics. No HDFS connection is needed to prove selection.
	spec, err := ParsePath("s3-opts,endpoint=owned.invalid,is-hdfs=true,bucket=owned,key=caller,secret=owned:object")
	require.NoError(t, err)
	for _, tc := range []struct {
		forETL bool
		want   objectStorageBackend
	}{{true, objectStorageAWS}, {false, objectStorageHDFS}} {
		args, err := admitSQLFilePath(admin, spec, tc.forETL)
		require.NoError(t, err)
		require.Equal(t, tc.want, selectObjectStorageBackend(args))
	}
	backup, err := GetForBackup(admin, "s3-opts,endpoint=disk,bucket="+t.TempDir()+":object", "")
	require.NoError(t, err)
	defer backup.Close(admin)
	require.IsType(t, &diskObjectStorage{}, backup.(*S3FS).rawStorage)
}
