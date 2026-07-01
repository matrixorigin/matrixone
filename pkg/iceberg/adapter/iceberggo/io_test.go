//go:build iceberggo

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

package iceberggo

import (
	"context"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	mopio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

func TestFileServiceIOReadSeekReadAtAndStat(t *testing.T) {
	ctx := context.Background()
	mem, err := fileservice.NewMemoryFS("iceberg-go-read", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeAdapterMemoryFile(t, ctx, mem, "sales/orders.avro", []byte("manifest-data"))

	iofs := NewFileServiceIO(ctx, mopio.ScopedProvider{FileService: mem}, func(location string) mopio.ObjectScope {
		return objectScopeForAdapterTest(location, strings.TrimPrefix(location, "s3://warehouse/"))
	})

	var _ icebergio.ReadFileIO = iofs
	opened, err := iofs.Open("s3://warehouse/sales/orders.avro")
	if err != nil {
		t.Fatalf("open through FileServiceIO: %v", err)
	}
	defer opened.Close()
	info, err := opened.Stat()
	if err != nil {
		t.Fatalf("stat opened file: %v", err)
	}
	if info.Name() != "orders.avro" || info.Size() != int64(len("manifest-data")) {
		t.Fatalf("unexpected file info name=%q size=%d", info.Name(), info.Size())
	}
	if pos, err := opened.Seek(9, io.SeekStart); err != nil || pos != 9 {
		t.Fatalf("seek failed pos=%d err=%v", pos, err)
	}
	buf := make([]byte, 4)
	if n, err := opened.Read(buf); err != nil || n != 4 || string(buf) != "data" {
		t.Fatalf("read after seek n=%d data=%q err=%v", n, buf, err)
	}
	at := make([]byte, 8)
	if n, err := opened.ReadAt(at, 0); err != nil || n != 8 || string(at) != "manifest" {
		t.Fatalf("readat n=%d data=%q err=%v", n, at, err)
	}

	all, err := iofs.ReadFile("s3://warehouse/sales/orders.avro")
	if err != nil {
		t.Fatalf("read file through FileServiceIO: %v", err)
	}
	if string(all) != "manifest-data" {
		t.Fatalf("unexpected readfile payload: %q", all)
	}
}

func TestFileServiceIOWriteCreateAndRemove(t *testing.T) {
	ctx := context.Background()
	mem, err := fileservice.NewMemoryFS("iceberg-go-write", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	iofs := NewFileServiceIO(ctx, mopio.ScopedProvider{FileService: mem}, func(location string) mopio.ObjectScope {
		return objectScopeForAdapterTest(location, strings.TrimPrefix(location, "s3://warehouse/"))
	})
	var _ icebergio.WriteFileIO = iofs

	if err := iofs.WriteFile("s3://warehouse/sales/from-writefile.bin", []byte("writefile")); err != nil {
		t.Fatalf("writefile through FileServiceIO: %v", err)
	}
	data, err := readAdapterMemoryFile(ctx, mem, "sales/from-writefile.bin")
	if err != nil {
		t.Fatalf("read writefile result: %v", err)
	}
	if string(data) != "writefile" {
		t.Fatalf("unexpected writefile payload: %q", data)
	}

	writer, err := iofs.Create("s3://warehouse/sales/from-create.bin")
	if err != nil {
		t.Fatalf("create through FileServiceIO: %v", err)
	}
	if _, err := writer.Write([]byte("create-")); err != nil {
		t.Fatalf("write create prefix: %v", err)
	}
	if _, err := writer.ReadFrom(strings.NewReader("reader")); err != nil {
		t.Fatalf("readfrom create suffix: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	data, err = readAdapterMemoryFile(ctx, mem, "sales/from-create.bin")
	if err != nil {
		t.Fatalf("read create result: %v", err)
	}
	if string(data) != "create-reader" {
		t.Fatalf("unexpected create payload: %q", data)
	}

	if err := iofs.Remove("s3://warehouse/sales/from-create.bin"); err != nil {
		t.Fatalf("remove through FileServiceIO: %v", err)
	}
	if _, err := readAdapterMemoryFile(ctx, mem, "sales/from-create.bin"); err == nil {
		t.Fatalf("expected removed file to be absent")
	}
}

func TestFileServiceIOMatchesMOFileServiceSemantics(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	mem, err := fileservice.NewMemoryFS("iceberg-go-cross-check", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	provider := mopio.VendedCredentialProvider{
		Credentials: []api.StorageCredential{{
			Prefix:    "s3://warehouse/sales",
			Config:    map[string]string{"s3.access-key-id": "cross-check-ak"},
			ExpiresAt: now.Add(10 * time.Minute),
		}},
		Now: func() time.Time { return now },
		BuildFileService: func(ctx context.Context, scope mopio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			return mem, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
	}
	iofs := NewFileServiceIO(ctx, provider, func(location string) mopio.ObjectScope {
		return objectScopeForAdapterTest(location, location)
	})

	externalPath := "s3://warehouse/sales/cross/check.bin"
	moPath := "cross/check.bin"
	payload := []byte("0123456789abcdef")
	if err := iofs.WriteFile(externalPath, payload); err != nil {
		t.Fatalf("adapter writefile: %v", err)
	}
	direct, err := readAdapterMemoryFile(ctx, mem, moPath)
	if err != nil {
		t.Fatalf("direct MO FileService read after adapter write: %v", err)
	}
	if string(direct) != string(payload) {
		t.Fatalf("adapter write did not match MO FileService payload: got %q want %q", direct, payload)
	}

	opened, err := iofs.Open(externalPath)
	if err != nil {
		t.Fatalf("adapter open: %v", err)
	}
	defer opened.Close()
	info, err := opened.Stat()
	if err != nil {
		t.Fatalf("adapter stat: %v", err)
	}
	if info.Name() != "check.bin" || info.Size() != int64(len(payload)) {
		t.Fatalf("adapter stat mismatch name=%q size=%d", info.Name(), info.Size())
	}
	if pos, err := opened.Seek(4, io.SeekStart); err != nil || pos != 4 {
		t.Fatalf("adapter seek pos=%d err=%v", pos, err)
	}
	seekBuf := make([]byte, 5)
	if n, err := opened.Read(seekBuf); err != nil || n != len(seekBuf) {
		t.Fatalf("adapter read after seek n=%d err=%v", n, err)
	}
	directRange := readAdapterMemoryRange(t, ctx, mem, moPath, 4, int64(len(seekBuf)))
	if string(seekBuf) != string(directRange) {
		t.Fatalf("adapter seek/read mismatch: got %q want direct MO range %q", seekBuf, directRange)
	}
	readAtBuf := make([]byte, 6)
	if n, err := opened.ReadAt(readAtBuf, 7); err != nil || n != len(readAtBuf) {
		t.Fatalf("adapter readat n=%d err=%v", n, err)
	}
	directRange = readAdapterMemoryRange(t, ctx, mem, moPath, 7, int64(len(readAtBuf)))
	if string(readAtBuf) != string(directRange) {
		t.Fatalf("adapter readat mismatch: got %q want direct MO range %q", readAtBuf, directRange)
	}

	directPath := "cross/direct.bin"
	directExternalPath := "s3://warehouse/sales/cross/direct.bin"
	writeAdapterMemoryFile(t, ctx, mem, directPath, []byte("direct-file-service"))
	throughAdapter, err := iofs.ReadFile(directExternalPath)
	if err != nil {
		t.Fatalf("adapter readfile after direct MO write: %v", err)
	}
	direct, err = readAdapterMemoryFile(ctx, mem, directPath)
	if err != nil {
		t.Fatalf("direct MO FileService read: %v", err)
	}
	if string(throughAdapter) != string(direct) {
		t.Fatalf("adapter readfile mismatch: got %q want direct MO payload %q", throughAdapter, direct)
	}

	createExternalPath := "s3://warehouse/sales/cross/create.bin"
	createMOPath := "cross/create.bin"
	writer, err := iofs.Create(createExternalPath)
	if err != nil {
		t.Fatalf("adapter create: %v", err)
	}
	if _, err := writer.Write([]byte("created-")); err != nil {
		t.Fatalf("adapter writer write: %v", err)
	}
	if _, err := writer.ReadFrom(strings.NewReader("through-adapter")); err != nil {
		t.Fatalf("adapter writer readfrom: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("adapter writer close: %v", err)
	}
	direct, err = readAdapterMemoryFile(ctx, mem, createMOPath)
	if err != nil {
		t.Fatalf("direct MO FileService read after adapter create: %v", err)
	}
	if string(direct) != "created-through-adapter" {
		t.Fatalf("adapter create did not match MO FileService payload: %q", direct)
	}

	if err := iofs.Remove(createExternalPath); err != nil {
		t.Fatalf("adapter remove: %v", err)
	}
	if _, err := readAdapterMemoryFile(ctx, mem, createMOPath); err == nil {
		t.Fatalf("expected adapter remove to delete the MO FileService object")
	}

	rawPath := "s3://warehouse/sales/cross/missing.bin?X-Amz-Signature=raw-secret"
	redacted := mopio.RedactObjectPath(rawPath)
	_, err = NewFileServiceIO(ctx, nil, nil).Open(rawPath)
	if err == nil {
		t.Fatalf("expected missing provider error")
	}
	var pathErr *fs.PathError
	if !errorAsPath(err, &pathErr) {
		t.Fatalf("expected path error, got %T", err)
	}
	if pathErr.Path != redacted || strings.Contains(err.Error(), "raw-secret") || strings.Contains(err.Error(), "s3://warehouse/") {
		t.Fatalf("adapter path redaction mismatch: path=%q want=%q err=%v", pathErr.Path, redacted, err)
	}
}

func TestFileServiceIOUsesVendedCredentialProvider(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	mem, err := fileservice.NewMemoryFS("iceberg-go-vended", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeAdapterMemoryFile(t, ctx, mem, "orders/data.bin", []byte("scoped"))

	var credentialSeen string
	provider := mopio.VendedCredentialProvider{
		Credentials: []api.StorageCredential{{
			Prefix:    "s3://warehouse/sales",
			Config:    map[string]string{"s3.access-key-id": "sales-ak"},
			ExpiresAt: now.Add(10 * time.Minute),
		}},
		Now: func() time.Time { return now },
		BuildFileService: func(ctx context.Context, scope mopio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			credentialSeen = credential.Config["s3.access-key-id"]
			return mem, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
	}
	iofs := NewFileServiceIO(ctx, provider, func(location string) mopio.ObjectScope {
		return objectScopeForAdapterTest(location, location)
	})

	data, err := iofs.ReadFile("s3://warehouse/sales/orders/data.bin")
	if err != nil {
		t.Fatalf("read through vended provider: %v", err)
	}
	if string(data) != "scoped" || credentialSeen != "sales-ak" {
		t.Fatalf("unexpected vended read data=%q credential=%q", data, credentialSeen)
	}
}

func TestFileServiceIOErrorPathIsRedacted(t *testing.T) {
	ctx := context.Background()
	iofs := NewFileServiceIO(ctx, nil, nil)
	_, err := iofs.Open("s3://warehouse/sales/data.parquet?X-Amz-Signature=raw-secret")
	if err == nil {
		t.Fatalf("expected missing provider error")
	}
	var pathErr *fs.PathError
	if !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected iceberg object IO error, got %v", err)
	}
	if strings.Contains(err.Error(), "s3://warehouse/") || strings.Contains(err.Error(), "raw-secret") {
		t.Fatalf("expected redacted path error, got %v", err)
	}
	if !strings.Contains(err.Error(), "<redacted:path:") {
		t.Fatalf("expected path hash in redacted error, got %v", err)
	}
	if !strings.Contains(err.Error(), "open ") {
		t.Fatalf("expected PathError-style operation, got %v", err)
	}
	if !errorAsPath(err, &pathErr) {
		t.Fatalf("expected *fs.PathError, got %T", err)
	}
}

func objectScopeForAdapterTest(originalLocation, storageLocation string) mopio.ObjectScope {
	return mopio.ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: storageLocation,
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
}

func writeAdapterMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string, data []byte) {
	t.Helper()
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   append([]byte(nil), data...),
		}},
	}); err != nil {
		t.Fatalf("write memory file %s: %v", path, err)
	}
}

func readAdapterMemoryFile(ctx context.Context, fs fileservice.ETLFileService, path string) ([]byte, error) {
	vec := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		return nil, err
	}
	return append([]byte(nil), vec.Entries[0].Data...), nil
}

func readAdapterMemoryRange(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string, offset, size int64) []byte {
	t.Helper()
	vec := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: offset,
			Size:   size,
		}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		t.Fatalf("read memory range %s offset=%d size=%d: %v", path, offset, size, err)
	}
	return append([]byte(nil), vec.Entries[0].Data...)
}

func errorAsPath(err error, target **fs.PathError) bool {
	switch e := err.(type) {
	case *fs.PathError:
		*target = e
		return true
	default:
		return false
	}
}
