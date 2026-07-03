// Copyright 2025 Matrix Origin
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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/panjf2000/ants/v2"
	costypes "github.com/tencentyun/cos-go-sdk-v5"
)

// failAfterBytesReader wraps an io.Reader and returns errAfter once
// totalBytes have been read. Reads up to failAfter bytes succeed normally.
type failAfterBytesReader struct {
	r         io.Reader
	readSoFar int64
	failAfter int64
	errAfter  error
}

func (r *failAfterBytesReader) Read(p []byte) (int, error) {
	if r.readSoFar >= r.failAfter {
		return 0, r.errAfter
	}
	remaining := r.failAfter - r.readSoFar
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.r.Read(p)
	r.readSoFar += int64(n)
	return n, err
}

type waitAfterBytesReader struct {
	r         io.Reader
	readSoFar int64
	waitAfter int64
	waitCh    <-chan struct{}
	timeout   time.Duration
}

func (r *waitAfterBytesReader) Read(p []byte) (int, error) {
	if r.readSoFar >= r.waitAfter && r.waitCh != nil {
		select {
		case <-r.waitCh:
			r.waitCh = nil
		case <-time.After(r.timeout):
			return 0, fmt.Errorf("timed out waiting after %d bytes", r.waitAfter)
		}
	}
	n, err := r.r.Read(p)
	r.readSoFar += int64(n)
	return n, err
}

func newMockAWSServer(t *testing.T, failPart int32) (*httptest.Server, *awsServerState) {
	t.Helper()
	state := &awsServerState{
		failPart: failPart,
		parts:    make(map[int32][]byte),
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
			if state.failCreate {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<CreateMultipartUploadResult><UploadId>%s</UploadId></CreateMultipartUploadResult>`, state.uploadID)
		case r.Method == http.MethodPut && !strings.Contains(r.URL.RawQuery, "partNumber") && !strings.Contains(r.URL.RawQuery, "uploadId") && !strings.Contains(r.URL.RawQuery, "uploads"):
			body, _ := io.ReadAll(r.Body)
			state.mu.Lock()
			state.putCount++
			state.putBody = append([]byte{}, body...)
			state.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber"):
			partStr := r.URL.Query().Get("partNumber")
			pn, _ := strconv.Atoi(partStr)
			body, _ := io.ReadAll(r.Body)
			if state.failPart > 0 && int32(pn) == state.failPart {
				if state.failPartSeen != nil {
					state.failPartOnce.Do(func() { close(state.failPartSeen) })
				}
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			state.mu.Lock()
			state.parts[int32(pn)] = body
			state.mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf("\"etag-%d\"", pn))
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId"):
			body, _ := io.ReadAll(r.Body)
			if state.failComplete {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			state.mu.Lock()
			state.completeBody = append([]byte{}, body...)
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Location>loc</Location><Bucket>bucket</Bucket><Key>object</Key><ETag>"etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "delete"):
			_, _ = io.ReadAll(r.Body)
			state.mu.Lock()
			state.deleteMultiCount++
			state.mu.Unlock()
			if state.failDeleteMultiMalformed {
				w.WriteHeader(http.StatusBadRequest)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(awsS3ErrorXML("MalformedXML", "The XML you provided was not well-formed")))
				return
			}
			if state.failDeleteMultiInternal {
				w.WriteHeader(http.StatusInternalServerError)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(awsS3ErrorXML("InternalError", "internal error")))
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<DeleteResult></DeleteResult>`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId"):
			state.aborted.Store(true)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete:
			state.mu.Lock()
			state.deleteSingleKeys = append(state.deleteSingleKeys, awsObjectKeyFromPath(r.URL.Path))
			state.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	return httptest.NewServer(handler), state
}

type awsServerState struct {
	mu                       sync.Mutex
	parts                    map[int32][]byte
	completeBody             []byte
	failPart                 int32
	failPartSeen             chan struct{}
	failPartOnce             sync.Once
	failComplete             bool
	failCreate               bool
	uploadID                 string
	aborted                  atomic.Bool
	putCount                 int
	putBody                  []byte
	deleteMultiCount         int
	deleteSingleKeys         []string
	failDeleteMultiMalformed bool
	failDeleteMultiInternal  bool
}

func awsObjectKeyFromPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func awsS3ErrorXML(code, message string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}

func newTestAWSClient(t *testing.T, srv *httptest.Server) *AwsSDKv2 {
	t.Helper()
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("id", "key", "")),
		HTTPClient:  srv.Client(),
		Retryer: func() aws.Retryer {
			return aws.NopRetryer{}
		},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return &AwsSDKv2{
		name:   "aws-test",
		bucket: "bucket",
		client: client,
	}
}

func TestAwsParallelMultipartSuccess(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-success"

	sdk := newTestAWSClient(t, server)

	data := bytes.Repeat([]byte("a"), int(minMultipartPartSize+1))
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("write failed: %v, parts=%d, complete=%s", err, len(state.parts), string(state.completeBody))
	}
	if len(state.parts) != 1 {
		t.Fatalf("expected merged final part, got %d parts", len(state.parts))
	}
	if len(state.parts[1]) != len(data) {
		t.Fatalf("expected final part size %d, got %d", len(data), len(state.parts[1]))
	}
	if len(state.completeBody) == 0 {
		t.Fatalf("complete body not recorded")
	}
}

func TestAwsParallelMultipartAbortOnError(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-fail"
	state.failComplete = true

	sdk := newTestAWSClient(t, server)

	data := bytes.Repeat([]byte("b"), int(minMultipartPartSize*2))
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !state.aborted.Load() {
		t.Fatalf("expected abort request to be sent")
	}
}

func TestAwsMultipartFallbackSmallSize(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-small"

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("x"), int(minMultipartPartSize-1))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, nil); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if state.putCount != 1 {
		t.Fatalf("expected single PUT fallback, got %d", state.putCount)
	}
	if string(state.putBody) != string(data) {
		t.Fatalf("unexpected put body")
	}
}

func TestAwsMultipartFallbackUnknownSmall(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-unknown"

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("y"), int(minMultipartPartSize-1))
	reader := bytes.NewReader(data)
	if err := sdk.WriteMultipartParallel(context.Background(), "object", reader, nil, nil); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if state.putCount != 1 {
		t.Fatalf("expected fallback PUT for unknown small")
	}
	if string(state.putBody) != string(data) {
		t.Fatalf("unexpected put body")
	}
}

func TestAwsMultipartTooManyParts(t *testing.T) {
	server, _ := newMockAWSServer(t, 0)
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	huge := int64(maxMultipartParts+1) * defaultParallelMultipartPartSize
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(nil), &huge, &ParallelMultipartOption{
		PartSize: defaultParallelMultipartPartSize,
	})
	if err == nil {
		t.Fatalf("expected error for too many parts")
	}
}

func TestAwsMultipartUploadPartError(t *testing.T) {
	server, _ := newMockAWSServer(t, 1)
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("p"), int(minMultipartPartSize*2))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	}); err == nil {
		t.Fatalf("expected upload part error")
	}
}

func TestAwsMultipartSendJobFailureAfterPendingChunk(t *testing.T) {
	server, state := newMockAWSServer(t, 1)
	defer server.Close()
	state.uploadID = "uid-pending-fail"
	state.failPartSeen = make(chan struct{})

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("q"), int(minMultipartPartSize*3))
	reader := &waitAfterBytesReader{
		r:         bytes.NewReader(data),
		waitAfter: minMultipartPartSize * 2,
		waitCh:    state.failPartSeen,
		timeout:   3 * time.Second,
	}
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", reader, &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 1,
	})
	if err == nil {
		t.Fatalf("expected upload part error")
	}
	if !state.aborted.Load() {
		t.Fatalf("expected abort request")
	}
	if len(state.completeBody) > 0 {
		t.Fatalf("expected no complete body")
	}
}

func TestAwsParallelMultipartUnknownSize(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-unknown-size"

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("z"), int(minMultipartPartSize+1))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), nil, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(state.parts) != 1 {
		t.Fatalf("expected multipart upload with merged final part, got %d parts", len(state.parts))
	}
	if len(state.parts[1]) != len(data) {
		t.Fatalf("expected final part size %d, got %d", len(data), len(state.parts[1]))
	}
}

func TestAwsMultipartEmptyReader(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(nil), nil, nil); err != nil {
		t.Fatalf("expected nil error for empty reader, got %v", err)
	}
	if state.putCount != 0 && len(state.parts) != 0 {
		t.Fatalf("no upload should happen for empty reader")
	}
}

func TestAwsMultipartContextCanceled(t *testing.T) {
	server, _ := newMockAWSServer(t, 0)
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sdk.WriteMultipartParallel(ctx, "object", bytes.NewReader([]byte("data")), nil, nil); err == nil {
		t.Fatalf("expected context canceled error")
	}
}

func TestAwsMultipartCreateFail(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.failCreate = true

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("i"), int(minMultipartPartSize+1))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, nil); err == nil {
		t.Fatalf("expected create multipart error")
	}
}

func TestAwsWriteLargeNonSeekableFallsBackToMultipart(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-large-nonseekable"

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("k"), int(smallObjectThreshold+1))
	size := int64(len(data))
	reader := io.LimitReader(bytes.NewReader(data), size)

	if err := sdk.Write(context.Background(), "object", reader, &size, nil); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if state.putCount != 0 {
		t.Fatalf("expected multipart fallback instead of raw put, got %d put requests", state.putCount)
	}
	if len(state.parts) != 1 {
		t.Fatalf("expected merged multipart part, got %d", len(state.parts))
	}
	if len(state.parts[1]) != len(data) {
		t.Fatalf("expected final part size %d, got %d", len(data), len(state.parts[1]))
	}
	if len(state.completeBody) == 0 {
		t.Fatalf("expected multipart complete request")
	}
}

func TestAwsParallelMultipartDoesNotDeadlockOnTinyGlobalPool(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-no-deadlock"

	sdk := newTestAWSClient(t, server)
	data := bytes.Repeat([]byte("m"), int(minMultipartPartSize*2))
	size := int64(len(data))

	oldPool := parallelUploadPool
	tinyPool, err := ants.NewPool(1)
	if err != nil {
		t.Fatalf("create ants pool: %v", err)
	}
	parallelUploadPool = tinyPool
	defer func() {
		tinyPool.Release()
		parallelUploadPool = oldPool
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sdk.WriteMultipartParallel(ctx, "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
			PartSize:    minMultipartPartSize,
			Concurrency: 2,
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("multipart upload timed out, likely deadlocked")
	}

	if len(state.parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(state.parts))
	}
	if len(state.completeBody) == 0 {
		t.Fatalf("expected multipart complete request")
	}
}

func TestAwsDeleteMultiUsesBatchWhenSupported(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()

	sdk := newTestAWSClient(t, server)
	if err := sdk.Delete(context.Background(), "a", "b", "c"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if state.deleteMultiCount != 1 {
		t.Fatalf("expected 1 batch delete, got %d", state.deleteMultiCount)
	}
	if len(state.deleteSingleKeys) != 0 {
		t.Fatalf("expected no single-delete fallback, got %v", state.deleteSingleKeys)
	}
}

func TestAwsDeleteMultiFallsBackToSinglesOnMalformedXML(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.failDeleteMultiMalformed = true

	sdk := newTestAWSClient(t, server)
	if err := sdk.Delete(context.Background(), "a", "b", "c"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if state.deleteMultiCount != 1 {
		t.Fatalf("expected 1 batch delete attempt, got %d", state.deleteMultiCount)
	}
	if strings.Join(state.deleteSingleKeys, ",") != "a,b,c" {
		t.Fatalf("expected single-delete fallback for all keys, got %v", state.deleteSingleKeys)
	}

	if err := sdk.Delete(context.Background(), "d", "e"); err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
	if state.deleteMultiCount != 1 {
		t.Fatalf("expected later deletes to skip batch after malformed xml, got %d batch attempts", state.deleteMultiCount)
	}
	if strings.Join(state.deleteSingleKeys, ",") != "a,b,c,d,e" {
		t.Fatalf("expected later deletes to go directly to single-delete path, got %v", state.deleteSingleKeys)
	}
}

func TestAwsDeleteMultiDoesNotFallbackOnOtherErrors(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.failDeleteMultiInternal = true

	sdk := newTestAWSClient(t, server)
	if err := sdk.Delete(context.Background(), "a", "b"); err == nil {
		t.Fatalf("expected delete error")
	}
	if state.deleteMultiCount != 1 {
		t.Fatalf("expected 1 batch delete attempt, got %d", state.deleteMultiCount)
	}
	if len(state.deleteSingleKeys) != 0 {
		t.Fatalf("expected no single-delete fallback, got %v", state.deleteSingleKeys)
	}
}

func newMockCOSServer(t *testing.T, failPart int) (*httptest.Server, *cosServerState) {
	t.Helper()
	state := &cosServerState{
		failPart:           failPart,
		failPartStatus:     http.StatusBadRequest,
		failCompleteStatus: http.StatusBadRequest,
		failCreateStatus:   http.StatusBadRequest,
		parts:              make(map[int][]byte),
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
			if state.failCreate {
				w.WriteHeader(state.failCreateStatus)
				return
			}
			_, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, state.uploadID)
		case r.Method == http.MethodPut && !strings.Contains(r.URL.RawQuery, "partNumber") && !strings.Contains(r.URL.RawQuery, "uploadId") && !strings.Contains(r.URL.RawQuery, "uploads"):
			body, _ := io.ReadAll(r.Body)
			state.mu.Lock()
			state.putCount++
			state.putBody = append([]byte{}, body...)
			state.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber"):
			partStr := r.URL.Query().Get("partNumber")
			pn, _ := strconv.Atoi(partStr)
			body, _ := io.ReadAll(r.Body)
			if state.failPart > 0 && pn == state.failPart {
				if state.failPartSeen != nil {
					state.failPartOnce.Do(func() { close(state.failPartSeen) })
				}
				w.WriteHeader(state.failPartStatus)
				return
			}
			state.mu.Lock()
			state.parts[pn] = body
			state.mu.Unlock()
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId"):
			body, _ := io.ReadAll(r.Body)
			if state.failComplete {
				w.WriteHeader(state.failCompleteStatus)
				return
			}
			state.mu.Lock()
			state.completeBody = append([]byte{}, body...)
			for partNum := 1; partNum < len(state.parts); partNum++ {
				if len(state.parts[partNum]) < int(minMultipartPartSize) {
					state.mu.Unlock()
					w.WriteHeader(http.StatusBadRequest)
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<Error><Code>EntityTooSmall</Code><Message>Your proposed upload is smaller than the minimum allowed object size.</Message></Error>`))
					return
				}
			}
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			state.respBody = `<CompleteMultipartUploadResult><Location>loc</Location><Bucket>bucket</Bucket><Key>object</Key><ETag>etag</ETag></CompleteMultipartUploadResult>`
			state.completed.Store(true)
			_, _ = w.Write([]byte(state.respBody))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId"):
			state.aborted.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	return httptest.NewServer(handler), state
}

type cosServerState struct {
	mu                 sync.Mutex
	parts              map[int][]byte
	completeBody       []byte
	failPart           int
	failPartSeen       chan struct{}
	failPartOnce       sync.Once
	failPartStatus     int
	failComplete       bool
	failCompleteStatus int
	failCreate         bool
	failCreateStatus   int
	uploadID           string
	aborted            atomic.Bool
	completed          atomic.Bool
	respBody           string
	putCount           int
	putBody            []byte
}

func newTestCOSClient(t *testing.T, srv *httptest.Server) *QCloudSDK {
	t.Helper()
	baseURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	client := costypes.NewClient(
		&costypes.BaseURL{BucketURL: baseURL},
		srv.Client(),
	)
	client.Conf.EnableCRC = false

	return &QCloudSDK{
		name:   "cos-test",
		client: client,
	}
}

func TestCOSParallelMultipartSuccess(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-uid"

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("c"), int(minMultipartPartSize+2))
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("write failed: %v, parts=%d, complete=%s", err, len(state.parts), string(state.completeBody))
	}
	if len(state.parts) != 1 {
		t.Fatalf("expected merged final part, got %d parts", len(state.parts))
	}
	if len(state.parts[1]) != len(data) {
		t.Fatalf("expected final part size %d, got %d", len(data), len(state.parts[1]))
	}
	if len(state.completeBody) == 0 {
		t.Fatalf("complete body not recorded")
	}
}

func TestCOSParallelMultipartAbortOnError(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-uid-fail"
	state.failComplete = true

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("d"), int(minMultipartPartSize*2))
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !state.aborted.Load() {
		t.Fatalf("expected abort request")
	}
}

func TestCOSMultipartFallbackSmallSize(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-small"

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("e"), int(minMultipartPartSize-1))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, nil); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if state.putCount != 1 {
		t.Fatalf("expected single PUT fallback, got %d", state.putCount)
	}
	if string(state.putBody) != string(data) {
		t.Fatalf("unexpected put body")
	}
}

func TestCOSMultipartFallbackUnknownSmall(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-unknown"

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("f"), int(minMultipartPartSize-1))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), nil, nil); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if state.putCount != 1 {
		t.Fatalf("expected fallback PUT for unknown small")
	}
	if string(state.putBody) != string(data) {
		t.Fatalf("unexpected put body")
	}
}

func TestCOSMultipartTooManyParts(t *testing.T) {
	server, _ := newMockCOSServer(t, 0)
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	huge := int64(maxMultipartParts+1) * defaultParallelMultipartPartSize
	err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(nil), &huge, &ParallelMultipartOption{
		PartSize: defaultParallelMultipartPartSize,
	})
	if err == nil {
		t.Fatalf("expected error for too many parts")
	}
}

func TestCOSMultipartUploadPartError(t *testing.T) {
	server, _ := newMockCOSServer(t, 1)
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("h"), int(minMultipartPartSize*2))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	}); err == nil {
		t.Fatalf("expected upload part error")
	}
}

func TestCOSMultipartSendJobFailureAfterPendingChunk(t *testing.T) {
	server, state := newMockCOSServer(t, 1)
	defer server.Close()
	state.uploadID = "cos-uid-pending-fail"
	state.failPartSeen = make(chan struct{})

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("s"), int(minMultipartPartSize*3))
	reader := &waitAfterBytesReader{
		r:         bytes.NewReader(data),
		waitAfter: minMultipartPartSize * 2,
		waitCh:    state.failPartSeen,
		timeout:   3 * time.Second,
	}
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", reader, &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 1,
	})
	if err == nil {
		t.Fatalf("expected upload part error")
	}
	if !state.aborted.Load() {
		t.Fatalf("expected abort request")
	}
	if state.completed.Load() || len(state.completeBody) > 0 {
		t.Fatalf("expected no complete multipart upload")
	}
}

func TestCOSMultipartUploadPartRetriesServerClosedIdleConnection(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-uid-retry-idle-conn"

	baseClient := server.Client()
	baseTransport := baseClient.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	transport := &cosUploadPartIdleConnTransport{
		base: baseTransport,
		part: "1",
	}
	baseClient.Transport = transport

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	client := costypes.NewClient(
		&costypes.BaseURL{BucketURL: baseURL},
		baseClient,
	)
	client.Conf.EnableCRC = false
	sdk := &QCloudSDK{
		name:   "cos-retry-idle-conn-test",
		client: client,
	}

	data := bytes.Repeat([]byte("r"), int(minMultipartPartSize+1))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 1,
	}); err != nil {
		t.Fatalf("write failed after transient upload-part error: %v", err)
	}
	if transport.uploadPartCalls.Load() < 2 {
		t.Fatalf("expected upload part retry, got %d calls", transport.uploadPartCalls.Load())
	}
	if !state.completed.Load() {
		t.Fatalf("expected multipart upload complete")
	}
}

type cosUploadPartIdleConnTransport struct {
	base            http.RoundTripper
	part            string
	uploadPartCalls atomic.Int32
}

func (t *cosUploadPartIdleConnTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut && req.URL.Query().Get("partNumber") == t.part {
		if t.uploadPartCalls.Add(1) == 1 {
			return nil, fmt.Errorf("http: server closed idle connection")
		}
	}
	return t.base.RoundTrip(req)
}

func TestCOSParallelMultipartUnknownSize(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-unknown-size"

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("g"), int(minMultipartPartSize+1))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), nil, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if len(state.parts) != 1 {
		t.Fatalf("expected multipart upload with merged final part, got %d parts", len(state.parts))
	}
	if len(state.parts[1]) != len(data) {
		t.Fatalf("expected final part size %d, got %d", len(data), len(state.parts[1]))
	}
}

func TestCOSParallelMultipartPipeSmallTail(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-pipe-small-tail"

	sdk := newTestCOSClient(t, server)
	pr, pw := io.Pipe()
	writeErrCh := make(chan error, 1)
	go func() {
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for i := 0; i < 10; i++ {
			if _, err := pw.Write(chunk); err != nil {
				writeErrCh <- err
				return
			}
		}
		if _, err := pw.Write([]byte("tail")); err != nil {
			writeErrCh <- err
			return
		}
		writeErrCh <- pw.Close()
	}()

	err := sdk.WriteMultipartParallel(context.Background(), "object", pr, nil, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if writeErr := <-writeErrCh; writeErr != nil {
		t.Fatalf("pipe writer failed: %v", writeErr)
	}
	if err != nil {
		t.Fatalf("write failed: %v, parts=%d, complete=%s", err, len(state.parts), string(state.completeBody))
	}
	if len(state.parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(state.parts))
	}
	if len(state.parts[1]) != int(minMultipartPartSize) {
		t.Fatalf("expected first part size %d, got %d", minMultipartPartSize, len(state.parts[1]))
	}
	if len(state.parts[2]) != int(minMultipartPartSize)+len("tail") {
		t.Fatalf("expected merged final part size %d, got %d", int(minMultipartPartSize)+len("tail"), len(state.parts[2]))
	}
}

func TestCOSMultipartEmptyReader(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(nil), nil, nil); err != nil {
		t.Fatalf("expected nil error for empty reader, got %v", err)
	}
	if state.putCount != 0 && len(state.parts) != 0 {
		t.Fatalf("no upload should happen for empty reader")
	}
}

func TestCOSMultipartContextCanceled(t *testing.T) {
	server, _ := newMockCOSServer(t, 0)
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sdk.WriteMultipartParallel(ctx, "object", bytes.NewReader([]byte("data")), nil, nil); err == nil {
		t.Fatalf("expected context canceled error")
	}
}

func TestCOSMultipartCreateFail(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.failCreate = true

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("j"), int(minMultipartPartSize+1))
	size := int64(len(data))
	if err := sdk.WriteMultipartParallel(context.Background(), "object", bytes.NewReader(data), &size, nil); err == nil {
		t.Fatalf("expected create multipart error")
	}
}

func TestCOSParallelMultipartDoesNotDeadlockOnTinyGlobalPool(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-no-deadlock"

	sdk := newTestCOSClient(t, server)
	data := bytes.Repeat([]byte("n"), int(minMultipartPartSize*2))
	size := int64(len(data))

	oldPool := parallelUploadPool
	tinyPool, err := ants.NewPool(1)
	if err != nil {
		t.Fatalf("create ants pool: %v", err)
	}
	parallelUploadPool = tinyPool
	defer func() {
		tinyPool.Release()
		parallelUploadPool = oldPool
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sdk.WriteMultipartParallel(ctx, "object", bytes.NewReader(data), &size, &ParallelMultipartOption{
			PartSize:    minMultipartPartSize,
			Concurrency: 2,
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("multipart upload timed out, likely deadlocked")
	}

	if len(state.parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(state.parts))
	}
	if len(state.completeBody) == 0 {
		t.Fatalf("expected multipart complete request")
	}
}

// TestAwsParallelMultipartAbortOnReadError verifies that AbortMultipartUpload
// is sent when a read error occurs after multipart upload has been initiated.
// This covers the case where setErr cancels the derived context and the abort
// defer must use a non-canceled context.
func TestAwsParallelMultipartAbortOnReadError(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "uid-read-err"

	sdk := newTestAWSClient(t, server)

	// Provide enough data for the first readChunk to succeed (triggering
	// multipart initiation), then fail on the next read.
	data := bytes.Repeat([]byte("x"), int(minMultipartPartSize*2))
	r := &failAfterBytesReader{
		r:         bytes.NewReader(data),
		failAfter: minMultipartPartSize,
		errAfter:  errors.New("size does not match"),
	}
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", r, &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err == nil {
		t.Fatal("expected read error")
	}
	if len(state.completeBody) > 0 {
		t.Fatal("expected no complete body")
	}
	if !state.aborted.Load() {
		t.Fatal("expected abort request to be sent for size mismatch read error")
	}
}

// TestCOSParallelMultipartAbortOnReadError verifies that AbortMultipartUpload
// is sent when a read error occurs after multipart upload has been initiated
// for the COS SDK.
func TestCOSParallelMultipartAbortOnReadError(t *testing.T) {
	server, state := newMockCOSServer(t, 0)
	defer server.Close()
	state.uploadID = "cos-uid-read-err"

	sdk := newTestCOSClient(t, server)

	data := bytes.Repeat([]byte("y"), int(minMultipartPartSize*2))
	r := &failAfterBytesReader{
		r:         bytes.NewReader(data),
		failAfter: minMultipartPartSize,
		errAfter:  errors.New("size does not match"),
	}
	size := int64(len(data))
	err := sdk.WriteMultipartParallel(context.Background(), "object", r, &size, &ParallelMultipartOption{
		PartSize:    minMultipartPartSize,
		Concurrency: 2,
	})
	if err == nil {
		t.Fatal("expected read error")
	}
	if state.completed.Load() {
		t.Fatal("expected no complete multipart upload")
	}
	if len(state.completeBody) > 0 {
		t.Fatal("expected no complete body")
	}
	if !state.aborted.Load() {
		t.Fatal("expected abort request to be sent for size mismatch read error")
	}
}
