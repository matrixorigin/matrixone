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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

type awsAbortContextKey struct{}

type awsAbortContextObservation struct {
	deadline    time.Time
	hasDeadline bool
	value       any
	err         error
}

type awsAbortObservingTransport struct {
	base     http.RoundTripper
	observed chan awsAbortContextObservation
	cancel   context.CancelFunc
}

func (t *awsAbortObservingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && r.URL.Query().Has("partNumber") {
		t.cancel()
		return nil, context.Canceled
	}
	if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
		deadline, ok := r.Context().Deadline()
		t.observed <- awsAbortContextObservation{
			deadline:    deadline,
			hasDeadline: ok,
			value:       r.Context().Value(awsAbortContextKey{}),
			err:         r.Context().Err(),
		}
	}
	return t.base.RoundTrip(r)
}

func awsCanceledMultipartWrite(ctx context.Context, sdk *AwsSDKv2, parallel bool) error {
	if !parallel {
		return sdk.Write(ctx, "key", strings.NewReader("nonempty"), nil, nil)
	}
	data := bytes.Repeat([]byte("x"), int(minMultipartPartSize))
	size := int64(len(data))
	return sdk.WriteMultipartParallel(ctx, "key", bytes.NewReader(data), &size, &ParallelMultipartOption{
		PartSize: size, Concurrency: 1,
	})
}

// Both entry points must abort an owned upload even after caller cancellation,
// using a fresh finite budget and preserving the original failure.
func TestAWSMultipartAbortContextAndErrors(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, abortFails := range []bool{false, true} {
			name := "sequential"
			if parallel {
				name = "parallel"
			}
			if abortFails {
				name += "/abort-fails"
			} else {
				name += "/abort-succeeds"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.WithValue(context.Background(), awsAbortContextKey{}, "retained"))
				defer cancel()
				var abortCount atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
						w.Header().Set("Content-Type", "application/xml")
						_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>owned-upload</UploadId></CreateMultipartUploadResult>`)
					case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "owned-upload":
						abortCount.Add(1)
						if abortFails {
							w.Header().Set("Content-Type", "application/xml")
							w.WriteHeader(http.StatusForbidden)
							_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>abort denied</Message></Error>`)
						} else {
							w.WriteHeader(http.StatusNoContent)
						}
					default:
						w.WriteHeader(http.StatusNotFound)
					}
				}))
				defer server.Close()
				probe := &awsAbortObservingTransport{base: server.Client().Transport, observed: make(chan awsAbortContextObservation, 1), cancel: cancel}
				sdk := newTestAWSClientWithTransport(t, server, probe)
				err := awsCanceledMultipartWrite(ctx, sdk, parallel)
				require.ErrorIs(t, err, context.Canceled)
				require.EqualValues(t, 1, abortCount.Load())
				if abortFails {
					var apiErr smithy.APIError
					require.ErrorAs(t, err, &apiErr)
					require.Equal(t, "AccessDenied", apiErr.ErrorCode())
				}
				select {
				case got := <-probe.observed:
					require.NoError(t, got.err)
					require.Equal(t, "retained", got.value)
					require.True(t, got.hasDeadline)
					remaining := time.Until(got.deadline)
					require.Greater(t, remaining, time.Duration(0))
					require.LessOrEqual(t, remaining, awsMultipartAbortTimeout)
				default:
					t.Fatal("abort request did not reach the HTTP transport")
				}
			})
		}
	}
}

// An S3 error response can send headers and then stall while the SDK reads its
// body. Exercise that actual HTTP path with a short helper budget so the test
// does not add the production 30-second cleanup budget to every package run.
func TestAWSMultipartAbortStalledBodyHonorsDeadline(t *testing.T) {
	abortStarted := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Query().Get("uploadId") != "slow-abort" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code>`)
		w.(http.Flusher).Flush()
		close(abortStarted)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() { close(release); server.Close() }()
	sdk := newTestAWSClient(t, server)
	done := make(chan error, 1)
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	go func() { done <- sdk.abortMultipartUpload(parent, "key", ptrTo("slow-abort"), time.Second) }()
	select {
	case <-abortStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("abort request did not reach the server")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("abort did not return after its response-body deadline")
	}
}
