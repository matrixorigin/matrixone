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

package datalink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newOwnedS3Fixture is deliberately small: it implements the object operations
// used by S3FS without relying on an external endpoint. The handler ignores
// signatures because this test is about SQL admission and the existing ETL
// provider contract, not AWS authentication.
func newOwnedS3Fixture(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	var (
		mu      sync.Mutex
		objects = make(map[string][]byte)
		uploads = make(map[string]struct {
			key  string
			data []byte
		})
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			switch {
			case r.URL.Query().Has("uploads"):
				uploads["owned-upload"] = struct {
					key  string
					data []byte
				}{key: key}
				w.Header().Set("Content-Type", "application/xml")
				_, _ = io.WriteString(w, `<CreateMultipartUploadResult><UploadId>owned-upload</UploadId></CreateMultipartUploadResult>`)
			case r.URL.Query().Has("uploadId"):
				uploadID := r.URL.Query().Get("uploadId")
				upload, ok := uploads[uploadID]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				objects[upload.key] = append([]byte(nil), upload.data...)
				delete(uploads, uploadID)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"owned-etag"</ETag></CompleteMultipartUploadResult>`)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
				upload, ok := uploads[uploadID]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				data, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				upload.data = append(upload.data, data...)
				uploads[uploadID] = upload
				w.Header().Set("ETag", `"owned-etag"`)
				w.WriteHeader(http.StatusOK)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			objects[key] = append([]byte(nil), data...)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			data, ok := objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", stringLength(len(data)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, ok := objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", stringLength(len(data)))
			_, _ = w.Write(data)
		case http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return server, func() {
		server.Close()
	}
}

func stringLength(n int) string {
	return strconv.Itoa(n)
}

func TestAuthorizeSQLFileAccess(t *testing.T) {
	newProc := func(account, role string) *process.Process {
		return &process.Process{Base: &process.BaseProcess{SessionInfo: process.SessionInfo{
			Account: account,
			Role:    role,
		}}}
	}

	tests := []struct {
		name    string
		proc    *process.Process
		path    string
		wantErr bool
	}{
		{name: "ASCII account and role names", proc: newProc("SYS", "MOADMIN"), path: "/tmp/owned.txt"},
		{name: "distinct Unicode account is not sys", proc: newProc("ſys", "moadmin"), path: "/tmp/owned.txt", wantErr: true},
		{name: "local path requires system moadmin", proc: newProc("sys", "moadmin"), path: "/tmp/owned.txt"},
		{name: "public local path denied", proc: newProc("sys", "public"), path: "/tmp/owned.txt", wantErr: true},
		{name: "tenant admin local path denied", proc: newProc("tenant", "accountadmin"), path: "/tmp/owned.txt", wantErr: true},
		{name: "named service denied", proc: newProc("sys", "public"), path: "SHARED:owned.txt", wantErr: true},
		{name: "empty service with arguments follows local resolver", proc: newProc("sys", "public"), path: ",endpoint,region,bucket,key,secret,prefix:owned.txt", wantErr: true},
		{name: "missing identity denied", proc: newProc("", ""), path: "/tmp/owned.txt", wantErr: true},
		{name: "dynamic s3 remains available", proc: newProc("sys", "public"), path: "s3,endpoint,region,bucket,key,secret,prefix:owned.txt"},
		{name: "dynamic hdfs remains available", proc: newProc("tenant", "public"), path: "hdfs,endpoint=localhost:9000,user=caller:owned.txt"},
		{name: "endpoint-only hdfs remains available", proc: newProc("tenant", "public"), path: "hdfs,endpoint=localhost:9000:owned.txt"},
		{name: "dynamic disk backend requires system moadmin", proc: newProc("sys", "public"), path: "s3-opts,endpoint=disk,bucket=/tmp/owned:owned.txt", wantErr: true},
		{name: "legacy dynamic disk backend requires system moadmin", proc: newProc("sys", "public"), path: "s3,disk,region,bucket,key,secret,prefix:owned.txt", wantErr: true},
		{name: "dynamic disk endpoint with hdfs option remains local", proc: newProc("sys", "public"), path: "s3-opts,endpoint=disk,is-hdfs=true,bucket=/tmp/owned:owned.txt", wantErr: true},
		{name: "dynamic disk backend allowed for system moadmin", proc: newProc("sys", "moadmin"), path: "s3-opts,endpoint=disk,bucket=/tmp/owned:owned.txt"},
		{name: "nil process denied", proc: nil, path: "/tmp/owned.txt", wantErr: true},
		{name: "nil base denied", proc: &process.Process{}, path: "/tmp/owned.txt", wantErr: true},
		{name: "partial identity denied", proc: newProc("sys", ""), path: "/tmp/owned.txt", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := process.AuthorizeSQLFileAccess(tt.proc, tt.path)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, "only the sys account moadmin role")
			} else {
				require.NoError(t, err)
			}
		})
	}
	t.Run("HDFS URL and stage preserve simple authentication", func(t *testing.T) {
		proc := newProc("tenant", "public")
		_, err := NewDatalink("hdfs://owned.invalid/owned.txt", proc)
		require.NoError(t, err)
		u, err := url.Parse("hdfs://owned.invalid/owned.txt")
		require.NoError(t, err)
		path, _, err := (stage.StageDef{Url: u}).ToPath()
		require.NoError(t, err)
		require.NoError(t, process.AuthorizeSQLFileAccess(proc, path))
	})
}

func TestRemoteStageDatalinkKeepsOrdinaryReadWrite(t *testing.T) {
	server, closeServer := newOwnedS3Fixture(t)
	defer closeServer()

	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.Account = "tenant"
	proc.Base.SessionInfo.Role = "public"
	proc.BuildPipelineContext(context.Background())
	proc.GetStageCache().Set("remote", stage.StageDef{
		Id:   1,
		Name: "remote",
		Url:  &url.URL{Scheme: stage.S3_PROTOCOL, Host: "bucket", Path: "/"},
		Credentials: map[string]string{
			stage.PARAMKEY_AWS_KEY_ID:     "id",
			stage.PARAMKEY_AWS_SECRET_KEY: "secret",
			stage.PARAMKEY_AWS_REGION:     "us-east-1",
			stage.PARAMKEY_PROVIDER:       stage.S3_PROVIDER_AMAZON,
			stage.PARAMKEY_ENDPOINT:       server.URL,
		},
	})

	readPath := "stage://remote/read.txt"
	// Writing and then reading the object proves that remote stage admission
	// does not require sys/moadmin.
	dl, err := NewDatalink(readPath, proc)
	require.NoError(t, err)
	writer, err := dl.NewWriter(proc)
	require.NoError(t, err)
	_, err = writer.Write([]byte("remote content"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	r, err := dl.NewReadCloser(proc)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, []byte("remote content"), data)
}

type borrowedDatalinkService struct {
	fileservice.ETLFileService
	closes int
}

func (f *borrowedDatalinkService) Close(context.Context) { f.closes++ }

func TestDatalinkNamedReadRechecksCurrentIdentity(t *testing.T) {
	ctx := context.Background()
	memory, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { memory.Close(ctx) })
	fs := &borrowedDatalinkService{ETLFileService: memory}
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "owned.txt",
		Entries:  []fileservice.IOEntry{{Data: []byte("owned"), Size: 5}},
	}))
	proc := &process.Process{Base: &process.BaseProcess{
		FileService: fs,
		SessionInfo: process.SessionInfo{Account: "sys", Role: "moadmin"},
	}}
	proc.ReplaceTopCtx(ctx)
	proc.BuildPipelineContext(ctx)
	dl := Datalink{
		Size:   -1,
		MoPath: "shared:owned.txt",
		path:   fileservice.Path{Service: "shared", File: "owned.txt"},
	}
	for range 2 {
		r, err := dl.NewReadCloser(proc)
		require.NoError(t, err)
		data := make([]byte, 5)
		_, err = r.Read(data)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, []byte("owned"), data)
	}
	entry, err := dl.StatFile(proc)
	require.NoError(t, err)
	require.Equal(t, "shared:owned.txt", entry.Name)
	writeDL := Datalink{
		Size:   -1,
		MoPath: "shared:new.txt",
		path:   fileservice.Path{Service: "shared", File: "new.txt"},
	}
	writer, err := writeDL.NewWriter(proc)
	require.NoError(t, err)
	_, err = writer.Write([]byte("next"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	proc.Base.SessionInfo.Role = "public"
	_, err = dl.NewReadCloser(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = dl.StatFile(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = writeDL.NewWriter(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	require.Zero(t, fs.closes, "readers, stat and writers must leave the borrowed service open")
}

func TestInternalStageDatalinkRequiresServerAuthority(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "missing", "stage.txt")
	ctx := context.WithValue(context.Background(), defines.InternalExecutorKey{}, true)
	proc := testutil.NewProc(t)
	proc.BuildPipelineContext(ctx)
	outerURL, err := url.Parse("file://" + filepath.Dir(target))
	require.NoError(t, err)
	innerURL, err := url.Parse("stage://outer/prefix")
	require.NoError(t, err)
	proc.GetStageCache().Set("outer", stage.StageDef{Id: 1, Name: "outer", Url: outerURL})
	proc.GetStageCache().Set("inner", stage.StageDef{Id: 2, Name: "inner", Url: innerURL})
	_, err = NewDatalink("stage://inner/stage.txt", proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = os.Stat(filepath.Dir(target))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDatalinkStageReuseRechecksServerAuthority(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "stage.txt")
	require.NoError(t, os.WriteFile(target, []byte("stage content"), 0o600))
	stageURL, err := url.Parse("file://" + dir)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), defines.InternalExecutorKey{}, true)
	proc := testutil.NewProc(t)
	proc.BuildPipelineContext(ctx)
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	proc.Ctx = process.SQLFileAccessContext(proc.Ctx, proc)
	proc.GetStageCache().Set("owned", stage.StageDef{Id: 1, Name: "owned", Url: stageURL})
	dl, err := NewDatalink("stage://owned/stage.txt", proc)
	require.NoError(t, err)
	r, err := dl.NewReadCloser(proc)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, "stage content", string(data))
	require.NoError(t, r.Close(), "the owned reader/service pair closes only once")

	// Retain the old administrator context while changing the current role.
	// Every I/O owner must rebind from current identity before acquisition.
	proc.Base.SessionInfo.Role = "public"
	_, err = dl.NewReadCloser(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = dl.StatFile(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
	_, err = dl.NewWriter(proc)
	require.ErrorContains(t, err, "only the sys account moadmin role")
}

func TestDatalinkAsyncSourceProviderAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    fileservice.Path
		allowed bool
	}{
		{"local", fileservice.Path{File: "owned.txt"}, false},
		{"configured", fileservice.Path{Service: "SHARED", File: "owned.txt"}, false},
		{"disk", fileservice.Path{Service: "s3-opts", ServiceArguments: []string{"endpoint=disk", "bucket=owned"}, File: "owned.txt"}, false},
		{"remote", fileservice.Path{Service: "s3-opts", ServiceArguments: []string{"endpoint=http://127.0.0.1:1", "bucket=owned", "key=caller", "secret=owned"}, File: "owned.txt"}, true},
		{"malformed", fileservice.Path{Service: "s3", ServiceArguments: []string{"missing"}, File: "owned.txt"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (Datalink{path: tc.path, MoPath: tc.path.File}).validateAsyncSource()
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDatalinkValidationMatchesMaterialization(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.BuildPipelineContext(context.Background())
	credentials := func(secret, endpoint string) map[string]string {
		return map[string]string{
			stage.PARAMKEY_AWS_KEY_ID: "caller", stage.PARAMKEY_AWS_SECRET_KEY: secret,
			stage.PARAMKEY_AWS_REGION: "region", stage.PARAMKEY_ENDPOINT: endpoint,
			stage.PARAMKEY_PROVIDER: stage.S3_PROVIDER_AMAZON,
		}
	}
	for _, s := range []struct {
		name, target, status string
		credentials          map[string]string
	}{
		{"remote", "s3://bucket/base", "enabled", credentials("owned", "https://example.invalid")},
		{"quoted", "s3://bucket/base", "enabled", credentials("a,b\"c", "https://example.invalid")},
		{"crlf", "s3://bucket/base", "enabled", credentials("a\r\nb", "https://example.invalid")},
		{"disk", "s3://bucket/base", "enabled", credentials("owned", "disk")},
		{"local", "file:///owned", "enabled", nil},
		{"named", "file:///SHARED:/owned", "enabled", nil},
		{"alias", "stage://remote/prefix", "enabled", nil},
		{"disabled", "stage://remote/prefix", "disabled", nil},
		{"cycle", "stage://cycle/prefix", "enabled", nil},
		{"missing", "s3://bucket/base", "enabled", nil},
	} {
		u, err := url.Parse(s.target)
		require.NoError(t, err)
		proc.GetStageCache().Set(s.name, stage.StageDef{Name: s.name, Url: u, Status: s.status, Credentials: s.credentials})
	}
	for _, tc := range []struct {
		uri           string
		legacy, async bool
	}{
		{"stage://remote/doc", true, true},
		{"stage://remote/doc?size=0", true, true},
		{"stage://quoted/doc", true, true},
		{"stage://crlf/doc", true, true},
		{"stage://alias/doc?OFFSET=1&SIZE=2", true, true},
		{"hdfs://host:9000/doc", true, true},
		{"file:///owned", true, false},
		{"file://SHARED:/owned", true, false},
		{"stage://local/doc", true, false},
		{"stage://named/doc", true, false},
		{"stage://disk/doc", true, false},
		// Legacy CAST intentionally does not interpret the file-service path.
		{"file:///invalid%20space", true, false},
		{"stage://remote/a%3Ab", true, false},
		{"stage://remote/a%3Ab?offset=-1&size=bad", false, false},
		{"stage://remote/doc?offset=bad&size=invalid", false, false},
		{"stage://remote/doc?offset=-1", false, false},
		{"stage://disabled/doc", false, false},
		{"stage://cycle/doc", false, false},
		{"stage://missing/doc", false, false},
		{"s3://bucket/doc", false, false},
		{"/owned", false, false},
		{"file:///%zz", false, false},
	} {
		t.Run(tc.uri, func(t *testing.T) {
			_, _, legacyErr := ParseDatalink(tc.uri, proc)
			gotLegacyErr := ValidateDatalink(tc.uri, proc)
			require.Equal(t, tc.legacy, gotLegacyErr == nil)
			require.Equal(t, reflect.TypeOf(legacyErr), reflect.TypeOf(gotLegacyErr))
			if legacyErr != nil {
				require.EqualError(t, gotLegacyErr, legacyErr.Error())
			}
			_, asyncErr := NewAsyncDatalink(tc.uri, proc)
			gotAsyncErr := ValidateAsyncDatalinkSource(tc.uri, proc)
			require.Equal(t, tc.async, gotAsyncErr == nil)
			require.Equal(t, reflect.TypeOf(asyncErr), reflect.TypeOf(gotAsyncErr))
			if asyncErr != nil {
				require.EqualError(t, gotAsyncErr, asyncErr.Error())
			}
		})
	}
}
