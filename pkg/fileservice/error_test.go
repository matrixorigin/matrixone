// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableStartupErrorTypedFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "dns", err: &net.DNSError{Err: "no such host", Name: "minio"}, want: true},
		{name: "connection refused", err: fmt.Errorf("dial minio: %w", syscall.ECONNREFUSED), want: true},
		{name: "network timeout", err: &net.DNSError{Err: "timeout", Name: "minio", IsTimeout: true}, want: true},
		{name: "request timeout", err: minio.ErrorResponse{StatusCode: http.StatusRequestTimeout}, want: true},
		{name: "too many requests", err: minio.ErrorResponse{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "internal server error", err: minio.ErrorResponse{StatusCode: http.StatusInternalServerError}, want: true},
		{name: "service unavailable", err: fmt.Errorf("bucket validation: %w", minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable}), want: true},
		{
			name: "minio connection closed by foreign host",
			err: &url.Error{
				Op:  http.MethodGet,
				URL: "http://minio/mo-test/?location=",
				Err: errors.New("Connection closed by foreign host http://minio/mo-test/?location=. Retry again."),
			},
			want: true,
		},
		{name: "bad request", err: minio.ErrorResponse{StatusCode: http.StatusBadRequest}, want: false},
		{name: "unauthorized", err: minio.ErrorResponse{StatusCode: http.StatusUnauthorized}, want: false},
		{name: "forbidden", err: minio.ErrorResponse{StatusCode: http.StatusForbidden}, want: false},
		{name: "not found", err: minio.ErrorResponse{StatusCode: http.StatusNotFound}, want: false},
		{
			name: "certificate verification",
			err: &url.Error{
				Op:  http.MethodGet,
				URL: "https://minio/mo-test/?location=",
				Err: &tls.CertificateVerificationError{
					Err: x509.UnknownAuthorityError{},
				},
			},
			want: false,
		},
		{name: "canceled", err: context.Canceled, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsRetryableStartupError(test.err))
		})
	}
}

func TestIsRetryableErrorRejectsStartupOnlyConnectionRefused(t *testing.T) {
	err := fmt.Errorf("dial minio: %w", syscall.ECONNREFUSED)
	require.False(t, IsRetryableError(err))
	require.True(t, IsRetryableStartupError(err))
}

func TestIsRetryableStartupErrorRejectsMinioProtocolMismatch(t *testing.T) {
	tests := []struct {
		name     string
		server   func() *httptest.Server
		endpoint func(string) string
	}{
		{
			name: "https client to http server",
			server: func() *httptest.Server {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.Start()
				return server
			},
			endpoint: func(serverURL string) string {
				return "https://" + strings.TrimPrefix(serverURL, "http://")
			},
		},
		{
			name: "http client to https server",
			server: func() *httptest.Server {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.StartTLS()
				return server
			},
			endpoint: func(serverURL string) string {
				return "http://" + strings.TrimPrefix(serverURL, "https://")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := test.server()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := NewFileService(ctx, Config{
				Name:    "SHARED",
				Backend: "MINIO",
				S3: ObjectStorageArguments{
					Endpoint:             test.endpoint(server.URL),
					Bucket:               "test",
					KeyID:                "id",
					KeySecret:            "secret",
					NoDefaultCredentials: true,
				},
			}, nil)
			require.Error(t, err)
			require.False(t, IsRetryableStartupError(err), "protocol mismatch must fail fast: %v", err)
		})
	}
}
