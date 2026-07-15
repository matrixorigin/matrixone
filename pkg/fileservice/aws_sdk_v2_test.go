// Copyright 2023 Matrix Origin
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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Test_NewAwsSDKv2(t *testing.T) {
	_, err := NewAwsSDKv2(context.Background(), ObjectStorageArguments{}, nil)
	assert.Error(t, err)

	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"+
		""))
	defer cancel()

	_, err = NewAwsSDKv2(ctx, ObjectStorageArguments{}, nil)
	assert.Error(t, err)
}

func TestAwsSDKv2BasicObjectOperations(t *testing.T) {
	const body = "hello object"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := ""
		if r.URL.Path != "/bucket" {
			key = strings.TrimPrefix(r.URL.Path, "/bucket/")
		}
		switch {
		case r.Method == http.MethodGet && key == "" && !strings.Contains(r.URL.RawQuery, "marker=page2"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<ListBucketResult><Name>bucket</Name><Prefix>dir/</Prefix><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>dir/file1</Key><Size>11</Size></Contents><CommonPrefixes><Prefix>dir/sub/</Prefix></CommonPrefixes><NextMarker>page2</NextMarker></ListBucketResult>`)
		case r.Method == http.MethodGet && key == "" && strings.Contains(r.URL.RawQuery, "marker=page2"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<ListBucketResult><Name>bucket</Name><Prefix>dir/</Prefix><Marker>page2</Marker><IsTruncated>false</IsTruncated><Contents><Key>dir/file2</Key><Size>7</Size></Contents></ListBucketResult>`)
		case r.Method == http.MethodHead && key == "missing":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && key == "missing":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodGet:
			data := []byte(body)
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" && rangeHeader != "bytes=0-" {
				data = data[1:5]
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>empty-upload</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "delete"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.ReadAll(r.Body)
			_, _ = io.WriteString(w, `<DeleteResult></DeleteResult>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	sdk.listMaxKeys = 2

	var entries []DirEntry
	for entry, err := range sdk.List(context.Background(), "dir/") {
		require.NoError(t, err)
		entries = append(entries, *entry)
	}
	require.Equal(t, []DirEntry{
		{Name: "dir/file1", Size: 11},
		{IsDir: true, Name: "dir/sub/"},
		{Name: "dir/file2", Size: 7},
	}, entries)

	size, err := sdk.Stat(context.Background(), "dir/file1")
	require.NoError(t, err)
	require.EqualValues(t, 12, size)

	_, err = sdk.Stat(context.Background(), "missing")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)

	exists, err := sdk.Exists(context.Background(), "dir/file1")
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = sdk.Exists(context.Background(), "missing")
	require.NoError(t, err)
	require.False(t, exists)

	reader, err := sdk.Read(context.Background(), "dir/file1", nil, nil)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, body, string(data))

	min, max := int64(1), int64(5)
	reader, err = sdk.Read(context.Background(), "dir/file1", &min, &max)
	require.NoError(t, err)
	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "ello", string(data))

	reader, err = sdk.Read(context.Background(), "missing", nil, nil)
	require.Nil(t, reader)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)

	require.True(t, sdk.SupportsParallelMultipart())
	require.NoError(t, sdk.Delete(context.Background()))
	require.NoError(t, sdk.Delete(context.Background(), "dir/file1"))
	require.NoError(t, sdk.Delete(context.Background(), "dir/file1", "dir/file2"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	seen := false
	for entry, err := range sdk.List(ctx, "dir/") {
		require.Nil(t, entry)
		require.ErrorIs(t, err, context.Canceled)
		seen = true
	}
	require.True(t, seen)
	require.ErrorIs(t, sdk.Delete(ctx, "dir/file1"), context.Canceled)

	size = int64(4)
	require.NoError(t, sdk.Write(context.Background(), "dir/file1", bytes.NewReader([]byte("data")), &size, nil))
	require.NoError(t, sdk.Write(context.Background(), "empty", bytes.NewReader(nil), nil, nil))
}

func TestAwsSDKv2ConstructorCredentialsAndRetryer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sdk, err := NewAwsSDKv2(context.Background(), ObjectStorageArguments{
		Name:               "aws-new",
		Bucket:             "bucket",
		Endpoint:           server.URL,
		Region:             "us-east-1",
		KeyID:              "id",
		KeySecret:          "secret",
		SessionToken:       "token",
		NoBucketValidation: true,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "aws-new", sdk.name)
	require.Equal(t, "bucket", sdk.bucket)

	provider, err := (ObjectStorageArguments{
		KeyID:        "id",
		KeySecret:    "secret",
		SessionToken: "token",
	}).credentialsProviderForAwsSDKv2(context.Background())
	require.NoError(t, err)
	creds, err := provider.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "id", creds.AccessKeyID)
	require.Equal(t, "secret", creds.SecretAccessKey)
	require.Equal(t, "token", creds.SessionToken)

	provider, err = (ObjectStorageArguments{
		NoDefaultCredentials: true,
	}).credentialsProviderForAwsSDKv2(context.Background())
	require.Nil(t, provider)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)

	provider, err = (ObjectStorageArguments{}).credentialsProviderForAwsSDKv2(context.Background())
	require.NoError(t, err)
	require.Nil(t, provider)

	retryer := newAWSRetryer()
	release := retryer.GetInitialToken()
	require.NoError(t, release(nil))
	release, err = retryer.GetRetryToken(context.Background(), errors.New("retry me"))
	if err == nil {
		require.NoError(t, release(nil))
	}
	_ = retryer.IsErrorRetryable(errors.New("not retryable"))
	require.Greater(t, retryer.MaxAttempts(), 0)
	_, _ = retryer.RetryDelay(1, errors.New("delay"))
}
