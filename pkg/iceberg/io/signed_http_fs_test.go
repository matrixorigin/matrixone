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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TestSignedHTTPFileServiceReadsRangeWithSignedHeaders(t *testing.T) {
	payload := []byte("0123456789abcdef")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "AWS4-HMAC-SHA256 signed" {
			t.Fatalf("missing signed authorization header: %v", r.Header)
		}
		if r.Host != "signed.example.test" {
			t.Fatalf("expected signed host header, got %q", r.Host)
		}
		if r.Header.Get("Range") != "bytes=3-7" {
			t.Fatalf("unexpected range header: %q", r.Header.Get("Range"))
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[3:8])
	}))
	defer server.Close()

	fs, readPath, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL: server.URL + "/data.bin",
		Headers: map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 signed",
			"Host":          "signed.example.test",
		},
		ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{{
			Offset: 3,
			Size:   5,
		}},
	}
	if err := fs.Read(context.Background(), &vec); err != nil {
		t.Fatalf("read signed range: %v", err)
	}
	if string(vec.Entries[0].Data) != "34567" {
		t.Fatalf("unexpected data: %q", vec.Entries[0].Data)
	}
}

func TestSignedHTTPFileServiceStatsWithSignedRangeGET(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET stat request, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "AWS4-HMAC-SHA256 signed" {
			t.Fatalf("missing signed authorization header: %v", r.Header)
		}
		if r.Host != "signed.example.test" {
			t.Fatalf("expected signed host header, got %q", r.Host)
		}
		if r.Header.Get("Range") != "bytes=0-0" {
			t.Fatalf("unexpected range header: %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", "bytes 0-0/4096")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("0"))
	}))
	defer server.Close()

	fs, readPath, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL: server.URL + "/data.bin",
		Headers: map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 signed",
			"Host":          "signed.example.test",
		},
		ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	stat, err := fs.StatFile(context.Background(), readPath)
	if err != nil {
		t.Fatalf("stat signed file: %v", err)
	}
	if stat.Size != 4096 {
		t.Fatalf("unexpected size: %+v", stat)
	}
}

func TestSignedHTTPFileServiceWritesWithSignedHeaders(t *testing.T) {
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("expected PUT write request, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "AWS4-HMAC-SHA256 signed" {
			t.Fatalf("missing signed authorization header: %v", r.Header)
		}
		if r.Host != "signed.example.test" {
			t.Fatalf("expected signed host header, got %q", r.Host)
		}
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fs, writePath, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL: server.URL + "/data.bin",
		Headers: map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 signed",
			"Host":          "signed.example.test",
		},
		ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	vec := fileservice.IOVector{
		FilePath: writePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   7,
			Data:   []byte("payload"),
		}},
	}
	if err := fs.Write(context.Background(), vec); err != nil {
		t.Fatalf("write signed object: %v", err)
	}
	if string(received) != "payload" {
		t.Fatalf("unexpected body: %q", received)
	}
}
