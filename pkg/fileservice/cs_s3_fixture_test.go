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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

var ciTestS3PrefixPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*(/[a-zA-Z0-9][a-zA-Z0-9._-]*)+$`)

func validCITestS3Prefix(prefix string) bool {
	return ciTestS3PrefixPattern.MatchString(prefix)
}

// Do not log SDK error strings: they can contain signed URLs or credential data.
// Keep the HTTP status and a known S3 error code so failures remain diagnosable.
func ciTestS3Error(operation string, err error) error {
	response := minio.ToErrorResponse(err)
	code := "unknown"
	switch response.Code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket",
		"RequestTimeTooSkewed", "AuthorizationHeaderMalformed", "InvalidRequest":
		code = response.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		code = "DeadlineExceeded"
	} else if errors.Is(err, context.Canceled) {
		code = "Canceled"
	}
	return fmt.Errorf("CS S3 %s failed (HTTP %d, code %s, type %T)", operation, response.StatusCode, code, err)
}

func listCITestPrefix(ctx context.Context, client *minio.Client, bucket, prefix string) ([]string, error) {
	if !strings.HasSuffix(prefix, "/") || !validCITestS3Prefix(strings.TrimSuffix(prefix, "/")) {
		return nil, errors.New("unsafe CS S3 fixture prefix")
	}
	// The channel-based ListObjects requires draining after cancellation. Use
	// the iterator so rejecting an unsafe result cannot strand its worker.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var keys []string
	for object := range client.ListObjectsIter(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return nil, ciTestS3Error("list fixture prefix", object.Err)
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return nil, errors.New("cannot safely enumerate CS S3 fixture objects")
		}
		keys = append(keys, object.Key)
		if len(keys) > 10000 {
			return nil, errors.New("CS S3 fixture cleanup exceeded safety limit")
		}
	}
	if ctx.Err() != nil {
		return nil, errors.New("CS S3 fixture listing cancelled or timed out")
	}
	return keys, nil
}

func checkEmptyCITestPrefix(ctx context.Context, client *minio.Client, bucket, prefix string) error {
	keys, err := listCITestPrefix(ctx, client, bucket, prefix)
	if err != nil {
		return err
	}
	if len(keys) != 0 {
		return errors.New("CS S3 fixture prefix is not empty")
	}
	return nil
}

func cleanupCITestPrefix(ctx context.Context, client *minio.Client, bucket, prefix string) error {
	// Finish validating the entire list before performing any deletion.
	keys, err := listCITestPrefix(ctx, client, bucket, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return ciTestS3Error("remove fixture object", err)
		}
	}
	return checkEmptyCITestPrefix(ctx, client, bucket, prefix)
}

func TestCSStoragePrefixScopedInitialization(t *testing.T) {
	t.Setenv("MO_CI_S3_PREFIX", "ci-ut/run-123")
	headCalls, listCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			headCalls++
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/ci-test/" ||
			!strings.HasPrefix(r.URL.Query().Get("prefix"), "ci-ut/run-123/") {
			t.Errorf("request escaped the permitted fixture scope: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		listCalls++
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer server.Close()
	t.Setenv("TEST_S3FS_CS", "name=cs,endpoint="+server.URL+",region=us-east-1,bucket=ci-test,key-id=id,key-secret=secret,is-minio=true,no-default-credentials=true")
	for _, args := range objectStorageArgumentsForTest("test", t) {
		if args.Name != "cs-s3" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sdk, err := NewMinioSDK(ctx, args, nil)
		if err != nil {
			t.Fatal(ciTestS3Error("initialize fixture", err))
		}
		if err := checkEmptyCITestPrefix(ctx, sdk.client, args.Bucket, args.KeyPrefix+"/"); err != nil {
			t.Fatal(err)
		}
		if headCalls != 0 || listCalls != 1 {
			t.Fatalf("expected prefix-only validation; HEAD=%d LIST=%d", headCalls, listCalls)
		}
		return
	}
	t.Fatal("CS test storage was not configured")
}

func TestCITestS3ErrorRedactsDetails(t *testing.T) {
	err := minio.ErrorResponse{Code: "AccessDenied", StatusCode: http.StatusForbidden, Message: "secret-token", BucketName: "private-bucket", Key: "private-key", RequestID: "private-request"}
	message := ciTestS3Error("initialize fixture", err).Error()
	if !strings.Contains(message, "HTTP 403, code AccessDenied") {
		t.Fatal("missing safe S3 failure classification")
	}
	for _, secret := range []string{"secret-token", "private-bucket", "private-key", "private-request"} {
		if strings.Contains(message, secret) {
			t.Fatal("S3 failure leaked private details")
		}
	}
}

func TestValidCITestS3Prefix(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		valid  bool
	}{
		{"ci-ut/run-123", true}, {"ci-ut/run-123/456", true},
		{"", false}, {"/ci-ut/run", false}, {".", false}, {"ci-ut", false},
		{"ci-ut/../other", false}, {"ci-ut//run", false}, {"../run", false},
		{"ci-ut/run/", false}, {"ci-ut/\nrun", false}, {"ci-ut/%2e%2e", false},
	} {
		if got := validCITestS3Prefix(tc.prefix); got != tc.valid {
			t.Errorf("validCITestS3Prefix(%q) = %v, want %v", tc.prefix, got, tc.valid)
		}
	}
}

// Exercise cleanup's guard rails without credentials or a real object store.
func TestCITestS3FixtureSafety(t *testing.T) {
	const prefix = "ci-ut/run-123/fixture/"
	for _, scenario := range []string{"empty", "existing", "cleanup", "outside-prefix", "list-denied", "delete-denied", "cancelled", "unsafe-prefix"} {
		t.Run(scenario, func(t *testing.T) {
			deleted, requests := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				switch r.Method {
				case http.MethodGet:
					if r.URL.Query().Get("prefix") != prefix || r.URL.Path != "/ci-test/" {
						t.Errorf("unexpected list scope: %s", r.URL)
					}
					if scenario == "list-denied" {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
						return
					}
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
					if scenario != "empty" && deleted == 0 {
						fmt.Fprintf(w, `<Contents><Key>%sfoo</Key><Size>1</Size></Contents>`, prefix)
						if scenario == "outside-prefix" {
							fmt.Fprint(w, `<Contents><Key>unrelated-sentinel</Key><Size>1</Size></Contents>`)
						}
					}
					fmt.Fprint(w, `</ListBucketResult>`)
				case http.MethodDelete:
					if r.URL.Path != "/ci-test/"+prefix+"foo" {
						t.Errorf("delete escaped fixture: %s", r.URL.Path)
					}
					deleted++
					if scenario == "delete-denied" {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected HTTP operation: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch scenario {
			case "empty", "existing":
				err = checkEmptyCITestPrefix(ctx, client, "ci-test", prefix)
			case "cancelled":
				cancel()
				err = cleanupCITestPrefix(ctx, client, "ci-test", prefix)
			case "unsafe-prefix":
				err = cleanupCITestPrefix(ctx, client, "ci-test", "../run/")
			default:
				err = cleanupCITestPrefix(ctx, client, "ci-test", prefix)
			}
			wantSuccess := scenario == "empty" || scenario == "cleanup"
			if (err == nil) != wantSuccess {
				t.Fatalf("err = %v, want success = %v", err, wantSuccess)
			}
			wantDeletes := 0
			if scenario == "cleanup" || scenario == "delete-denied" {
				wantDeletes = 1
			}
			if deleted != wantDeletes {
				t.Fatalf("delete count = %d, want %d", deleted, wantDeletes)
			}
			if scenario == "unsafe-prefix" && requests != 0 {
				t.Fatal("unsafe prefix must not issue a request")
			}
		})
	}
}

func TestCITestS3FixtureEarlyRejectionReleasesListingWorker(t *testing.T) {
	const prefix = "ci-ut/run-123/fixture/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/ci-test/" || r.URL.Query().Get("prefix") != prefix {
			t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>outside-prefix</Key><Size>1</Size></Contents>`)
		for i := 0; i < 8; i++ {
			fmt.Fprintf(w, `<Contents><Key>%spending-%d</Key><Size>1</Size></Contents>`, prefix, i)
		}
		fmt.Fprint(w, `</ListBucketResult>`)
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	listWorkers := func() int {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), "minio-go/v7.(*Client).ListObjects.func1")
	}
	before := listWorkers()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 32; i++ {
		_, err := listCITestPrefix(ctx, client, "ci-test", prefix)
		if err == nil || !strings.Contains(err.Error(), "cannot safely enumerate") {
			t.Fatalf("early rejection %d: got %v", i, err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && listWorkers() > before {
		time.Sleep(10 * time.Millisecond)
	}
	if got := listWorkers(); got > before {
		t.Fatalf("early rejection left %d MinIO listing workers blocked", got-before)
	}
}

func TestCITestS3FixtureLimitRejectsBeforeCleanup(t *testing.T) {
	const prefix = "ci-ut/run-123/fixture/"
	var deleted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/ci-test/" || r.URL.Query().Get("prefix") != prefix {
			t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
		for i := 0; i < 10002; i++ {
			fmt.Fprintf(w, `<Contents><Key>%sitem-%05d</Key><Size>1</Size></Contents>`, prefix, i)
		}
		fmt.Fprint(w, `</ListBucketResult>`)
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = cleanupCITestPrefix(ctx, client, "ci-test", prefix)
	if err == nil || !strings.Contains(err.Error(), "exceeded safety limit") {
		t.Fatalf("expected safety limit, got %v", err)
	}
	if deleted.Load() {
		t.Fatal("limit rejection must not delete any object")
	}
}
