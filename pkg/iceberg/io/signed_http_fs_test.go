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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestSignedHTTPFileServiceDoesNotExposeErrorBody(t *testing.T) {
	secret := "AWSSecretAccessKey=super-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(secret))
	}))
	defer server.Close()

	fs, readPath, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL: server.URL + "/data.bin",
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	vec := fileservice.IOVector{FilePath: readPath, Entries: []fileservice.IOEntry{{Offset: 0, Size: 1}}}
	err = fs.Read(context.Background(), &vec)
	if err == nil {
		t.Fatal("expected signed HTTP read error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("signed HTTP response body leaked through the object IO boundary: %v", err)
	}
	if !strings.Contains(err.Error(), "body_hash") {
		t.Fatalf("redacted signed HTTP error should retain a diagnostic hash: %v", err)
	}
}

func TestSignedHTTPFileServiceBoundsRangeReadWhenServerIgnoresRange(t *testing.T) {
	payload := []byte("0123456789abcdef")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=9-11" {
			t.Fatalf("unexpected range header: %q", r.Header.Get("Range"))
		}
		// A signed endpoint is allowed to ignore Range and return 200. The client
		// must skip and bound the stream instead of buffering the whole object.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	fs, readPath, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL: server.URL + "/data.bin",
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	vec := fileservice.IOVector{FilePath: readPath, Entries: []fileservice.IOEntry{{Offset: 9, Size: 3}}}
	if err := fs.Read(context.Background(), &vec); err != nil {
		t.Fatalf("read ignored range response: %v", err)
	}
	if string(vec.Entries[0].Data) != "9ab" {
		t.Fatalf("unexpected ignored-range data: %q", vec.Entries[0].Data)
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

func TestSignedHTTPFileServiceInterfaceMethodsAndReadWriters(t *testing.T) {
	payload := []byte("0123456789")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			switch r.Header.Get("Range") {
			case "bytes=2-5":
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(payload[2:6])
			case "":
				_, _ = w.Write(payload)
			default:
				t.Fatalf("unexpected range header: %q", r.Header.Get("Range"))
			}
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if string(body) != "reader-payload" {
				t.Fatalf("unexpected write body: %q", body)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	fs, path, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{
		URL:     server.URL + "/data.bin",
		Headers: map[string]string{"X-Test": "signed"},
	})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	if fs.Name() == "" || !strings.HasPrefix(fs.Name(), "iceberg-signed-http-") {
		t.Fatalf("unexpected fs name %q", fs.Name())
	}
	if err := fs.ReadCache(context.Background(), &fileservice.IOVector{}); err != nil {
		t.Fatalf("ReadCache should be a no-op: %v", err)
	}
	if err := fs.PrefetchFile(context.Background(), path); err != nil {
		t.Fatalf("PrefetchFile should be a no-op: %v", err)
	}
	if fs.Cost() == nil {
		t.Fatalf("Cost returned nil")
	}
	fs.ETLCompatible()
	fs.Close(context.Background())

	var written bytes.Buffer
	var closer io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset:            2,
			Size:              4,
			WriterForRead:     &written,
			ReadCloserForRead: &closer,
		}},
	}
	if err := fs.Read(context.Background(), &vec); err != nil {
		t.Fatalf("read with writers failed: %v", err)
	}
	if string(vec.Entries[0].Data) != "2345" || written.String() != "2345" {
		t.Fatalf("unexpected read data data=%q written=%q", vec.Entries[0].Data, written.String())
	}
	closerData, _ := io.ReadAll(closer)
	if string(closerData) != "2345" {
		t.Fatalf("unexpected read closer payload %q", closerData)
	}

	write := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset:         0,
			Size:           int64(len("reader-payload")),
			ReaderForWrite: strings.NewReader("reader-payload"),
		}},
	}
	if err := fs.Write(context.Background(), write); err != nil {
		t.Fatalf("write with reader failed: %v", err)
	}
}

func TestSignedHTTPFileServiceUnsupportedAndErrorPaths(t *testing.T) {
	fs := &signedHTTPFileService{name: "test", client: http.DefaultClient}
	if err := fs.Read(context.Background(), nil); err == nil {
		t.Fatalf("expected empty read vector error")
	}
	if err := fs.Write(context.Background(), fileservice.IOVector{}); err == nil {
		t.Fatalf("expected empty write URL error")
	}
	if err := fs.Write(context.Background(), fileservice.IOVector{FilePath: "http://example.invalid", Entries: []fileservice.IOEntry{{}, {}}}); err == nil {
		t.Fatalf("expected multi-entry write error")
	}
	if err := fs.Write(context.Background(), fileservice.IOVector{FilePath: "http://example.invalid", Entries: []fileservice.IOEntry{{Offset: 1}}}); err == nil {
		t.Fatalf("expected non-zero write offset error")
	}
	if _, err := fs.StatFile(context.Background(), ""); err == nil {
		t.Fatalf("expected empty stat URL error")
	}
	if err := fs.Delete(context.Background(), "x"); err == nil {
		t.Fatalf("expected delete unsupported error")
	}
	var listErr error
	for _, err := range fs.List(context.Background(), "dir") {
		listErr = err
	}
	if listErr == nil {
		t.Fatalf("expected list unsupported error")
	}
}

func TestSignedHTTPFileServiceStatFallbacksAndHelpers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Length", "12")
			_, _ = w.Write([]byte("hello world!"))
		case "/empty":
			w.Header().Set("Content-Range", "bytes */0")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("nope"))
		}
	}))
	defer server.Close()

	fs, _, err := SignedHTTPFileServiceBuilder{HTTPClient: server.Client()}.Build(context.Background(), ObjectScope{StorageLocation: "s3://warehouse/data.bin"}, SignedRequest{})
	if err != nil {
		t.Fatalf("build signed http fs: %v", err)
	}
	stat, err := fs.StatFile(context.Background(), server.URL+"/ok")
	if err != nil {
		t.Fatalf("stat OK fallback failed: %v", err)
	}
	if stat.Size != 12 || stat.Name != "ok" {
		t.Fatalf("unexpected OK stat: %+v", stat)
	}
	stat, err = fs.StatFile(context.Background(), server.URL+"/empty")
	if err != nil {
		t.Fatalf("stat empty range failed: %v", err)
	}
	if stat.Size != 0 || stat.Name != "empty" {
		t.Fatalf("unexpected empty stat: %+v", stat)
	}
	if _, err := fs.StatFile(context.Background(), server.URL+"/bad"); err == nil {
		t.Fatalf("expected unusable stat response error")
	}

	ranges := map[[2]int64]string{
		{0, -1}: "",
		{3, -1}: "bytes=3-",
		{3, 5}:  "bytes=3-7",
		{-1, 5}: "",
		{0, 0}:  "",
	}
	for input, want := range ranges {
		if got := httpRangeHeader(input[0], input[1]); got != want {
			t.Fatalf("range %#v got %q want %q", input, got, want)
		}
	}
	for value, wantOK := range map[string]bool{
		"bytes 0-0/12": true,
		"bytes */0":    true,
		"bytes */*":    false,
		"bad":          false,
	} {
		_, ok := contentRangeSize(value)
		if ok != wantOK {
			t.Fatalf("contentRangeSize(%q) ok=%v want %v", value, ok, wantOK)
		}
	}
	clone := cloneStringMap(map[string]string{"a": "b"})
	clone["a"] = "c"
	if cloneStringMap(nil) != nil || clone["a"] != "c" {
		t.Fatalf("cloneStringMap returned unexpected result")
	}
}
