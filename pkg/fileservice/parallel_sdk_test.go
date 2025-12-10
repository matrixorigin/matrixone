package fileservice

import (
	"bytes"
	"context"
	"encoding/xml"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	costypes "github.com/tencentyun/cos-go-sdk-v5"
)

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
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId"):
			state.aborted.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	return httptest.NewServer(handler), state
}

type awsServerState struct {
	mu           sync.Mutex
	parts        map[int32][]byte
	completeBody []byte
	failPart     int32
	failComplete bool
	failCreate   bool
	uploadID     string
	aborted      atomic.Bool
	putCount     int
	putBody      []byte
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
	if len(state.parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(state.parts))
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
	if len(state.parts) != 2 {
		t.Fatalf("expected multipart upload with unknown size")
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

type qcloudResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	UploadID string   `xml:"UploadId"`
}

func newMockCOSServer(t *testing.T, failPart int) (*httptest.Server, *cosServerState) {
	t.Helper()
	state := &cosServerState{
		failPart: failPart,
		parts:    make(map[int][]byte),
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
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			state.mu.Lock()
			state.parts[pn] = body
			state.mu.Unlock()
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
	mu           sync.Mutex
	parts        map[int][]byte
	completeBody []byte
	failPart     int
	failComplete bool
	failCreate   bool
	uploadID     string
	aborted      atomic.Bool
	completed    atomic.Bool
	respBody     string
	putCount     int
	putBody      []byte
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
	if len(state.parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(state.parts))
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
	if len(state.parts) != 2 {
		t.Fatalf("expected multipart upload with unknown size")
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
