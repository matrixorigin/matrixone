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
	"strconv"
	"strings"
	"sync"
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

	entries := make([]DirEntry, 0, 3)
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

func TestAwsSDKv2WriteUnknownSizeMultipartCompletes(t *testing.T) {
	var uploadedParts int
	var completed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		switch {
		case r.Method == http.MethodPost && key == "large" && strings.Contains(r.URL.RawQuery, "uploads"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<CreateMultipartUploadResult><Bucket>bucket</Bucket><Key>large</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodPut && key == "large" && strings.Contains(r.URL.RawQuery, "partNumber="):
			uploadedParts++
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"etag-1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && key == "large" && strings.Contains(r.URL.RawQuery, "uploadId=upload-1"):
			completed = true
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>large</Key><ETag>"final"</ETag></CompleteMultipartUploadResult>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	err := sdk.Write(context.Background(), "large", bytes.NewReader([]byte("non-empty")), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, uploadedParts)
	require.True(t, completed)
}

func TestAwsSDKv2WriteUnknownSizeEmptyCreatesObject(t *testing.T) {
	var activeUploads, putCount int
	var putBytes int
	objects := make(map[string]int)
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
			mu.Lock()
			activeUploads++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>empty-upload</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId=empty-upload"):
			mu.Lock()
			activeUploads--
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPut && !strings.Contains(r.URL.RawQuery, "uploadId="):
			if r.URL.Path == "/bucket/fail-empty" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			putCount++
			putBytes += len(data)
			objects[r.URL.Path] = len(data)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			mu.Lock()
			size, exists := objects[r.URL.Path]
			mu.Unlock()
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(size))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	require.NoError(t, sdk.Write(context.Background(), "empty", bytes.NewReader(nil), nil, nil))
	exists, err := sdk.Exists(context.Background(), "empty")
	require.NoError(t, err)
	require.True(t, exists, "success must mean the zero-byte object exists")
	fs := &S3FS{name: "s3", storage: sdk, rawStorage: sdk, ioMerger: NewIOMerger(), asyncUpdate: true, parallelMode: ParallelOff}
	vector := IOVector{FilePath: "from-fs", Entries: []IOEntry{{Offset: 0, Size: -1, ReaderForWrite: bytes.NewReader(nil)}}}
	require.NoError(t, fs.Write(context.Background(), vector))
	entry, err := fs.StatFile(context.Background(), "from-fs")
	require.NoError(t, err)
	require.Zero(t, entry.Size)
	require.True(t, moerr.IsMoErrCode(fs.Write(context.Background(), vector), moerr.ErrFileAlreadyExists))
	require.Error(t, sdk.Write(context.Background(), "fail-empty", bytes.NewReader(nil), nil, nil))
	exists, err = sdk.Exists(context.Background(), "fail-empty")
	require.NoError(t, err)
	require.False(t, exists)
	require.ErrorIs(t, sdk.Write(context.Background(), "reader-failure", errReader{}, nil, nil), io.ErrShortWrite)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, putCount)
	require.Zero(t, putBytes)
	require.Zero(t, activeUploads, "empty writes must not leak multipart uploads")
}

func TestAwsSDKv2WriteUnknownSizePartFailureAborts(t *testing.T) {
	var mu sync.Mutex
	var created, aborted, completed int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
			created++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>failed-upload</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber="):
			w.WriteHeader(http.StatusForbidden)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId=failed-upload"):
			aborted++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=failed-upload"):
			completed++
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	err := sdk.Write(context.Background(), "failed", strings.NewReader("non-empty"), nil, nil)
	require.Error(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, created)
	require.Equal(t, 1, aborted)
	require.Zero(t, completed)
}

func TestAwsSDKv2WriteUnknownSizeCanceledBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, (&AwsSDKv2{}).Write(ctx, "canceled", errReader{}, nil, nil), context.Canceled)
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

// Exercise the chunk boundary once, including errors after an upload ID exists.
func TestAwsSDKv2WriteUnknownSizeChunkBoundary(t *testing.T) {
	const chunk = 64 << 20
	data := bytes.Repeat([]byte("x"), chunk+1)
	primary := moerr.NewInternalErrorNoCtx("late source failure")
	for _, mode := range []string{"success", "late-read", "complete-denied"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var lengths []int
			var completed, aborted int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
					_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>boundary</UploadId></CreateMultipartUploadResult>`)
				case r.Method == http.MethodPut && r.URL.Query().Has("partNumber"):
					n, err := io.Copy(io.Discard, r.Body)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					lengths = append(lengths, int(n))
					index := len(lengths)
					mu.Unlock()
					if r.URL.Query().Get("partNumber") != strconv.Itoa(index) {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					w.Header().Set("ETag", `"part"`)
				case r.Method == http.MethodDelete:
					mu.Lock()
					aborted++
					mu.Unlock()
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodPost && r.URL.Query().Has("uploadId"):
					_, _ = io.Copy(io.Discard, r.Body)
					if mode == "complete-denied" {
						w.WriteHeader(http.StatusForbidden)
						_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code></Error>`)
						return
					}
					mu.Lock()
					completed++
					mu.Unlock()
					_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>`)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			var reader io.Reader = bytes.NewReader(data)
			if mode == "late-read" {
				reader = &failAfterBytesReader{r: reader, failAfter: chunk, errAfter: primary}
			}
			err := newTestAWSClient(t, server).Write(context.Background(), "key", reader, nil, nil)
			mu.Lock()
			defer mu.Unlock()
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, completed)
				require.Zero(t, aborted)
			} else {
				require.Error(t, err)
				require.Zero(t, completed)
				require.Equal(t, 1, aborted)
				if mode == "late-read" {
					require.Same(t, primary, err)
				}
			}
			expected := []int{chunk, 1}
			if mode == "late-read" {
				expected = []int{chunk}
			}
			require.Equal(t, expected, lengths)
		})
	}
}
