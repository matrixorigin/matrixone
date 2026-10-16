// Copyright 2022 Matrix Origin
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
	"io"
	"testing"

	"iter"

	"github.com/stretchr/testify/assert"
)

func TestGetForBackup(t *testing.T) {
	ctx := WithSQLFileAccess(t.Context(), "sys", "moadmin")
	dir := t.TempDir()
	fs, err := GetForBackup(ctx, dir, "DISK")
	assert.Nil(t, err)
	localFS, ok := fs.(*LocalFS)
	assert.True(t, ok)
	assert.Equal(t, dir, localFS.rootPath)
	assert.False(t, localFS.noChecksum)

	// DISK-V2 backend yields a raw (checksum-free) LocalFS
	fs2, err := GetForBackup(ctx, t.TempDir(), "DISK-V2")
	assert.Nil(t, err)
	localFS2, ok := fs2.(*LocalFS)
	assert.True(t, ok)
	assert.True(t, localFS2.noChecksum)
}

func TestLocalBackendOf(t *testing.T) {
	ctx := context.Background()
	disk, err := NewLocalFS(ctx, "s", t.TempDir(), DisabledCacheConfig, nil)
	assert.Nil(t, err)
	assert.Equal(t, "DISK", LocalBackendOf(disk))

	diskV2, err := NewLocalFS2(ctx, "s", t.TempDir(), DisabledCacheConfig, nil)
	assert.Nil(t, err)
	assert.Equal(t, "DISK-V2", LocalBackendOf(diskV2))

	// non-LocalFS (e.g. memory/S3) is raw -> DISK-V2
	mem, err := NewMemoryFS("mem", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	assert.Equal(t, "DISK-V2", LocalBackendOf(mem))
}

func TestGetForBackupS3Opts(t *testing.T) {
	ctx := WithSQLFileAccess(t.Context(), "sys", "moadmin")
	dir := t.TempDir()
	spec := JoinPath("s3-opts,endpoint=disk,bucket="+dir+",prefix=backup-prefix,name=backup", "object")
	fs, err := GetForBackup(ctx, spec, "DISK")
	assert.Nil(t, err)
	s3fs, ok := fs.(*S3FS)
	assert.True(t, ok)
	assert.Equal(t, "backup", s3fs.name)
	assert.Equal(t, "backup-prefix", s3fs.keyPrefix)
}

func TestBackupObjectStorageUsesDedicatedConnectionPool(t *testing.T) {
	var args ObjectStorageArguments
	setBackupConnectionPool(&args)

	assert.Equal(t, maxConnsPerHost, args.MaxConnsPerHost)
	assert.NotEqual(t, httpRoundTripper, newHTTPClient(args).Transport)

	args.MaxConnsPerHost = 7
	setBackupConnectionPool(&args)
	assert.Equal(t, 7, args.MaxConnsPerHost)
}

func TestSQLFileResourcesMirrorBackendSelection(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty service arguments still local", path: ",endpoint=disk:file", want: true},
		{name: "s3 disk", path: "s3,disk,region,bucket,key,secret,prefix:file", want: true},
		{name: "s3 opts disk", path: "s3-opts,endpoint=disk,bucket=/tmp/file:file", want: true},
		{name: "s3 opts caller hdfs is normalized away", path: "s3-opts,endpoint=disk,is-hdfs=true,bucket=/tmp/file:file", want: true},
		{name: "hdfs disk endpoint is hdfs provider", path: "hdfs,endpoint=disk,user=caller:file", want: false},
		{name: "minio disk endpoint is minio provider", path: "s3-opts,endpoint=disk,is-minio=true,bucket=/tmp/file,key=k,secret=s:file", want: false},
		{name: "remote s3", path: "s3-opts,endpoint=https://s3.example,bucket=b,key=k,secret=s:file", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := ParsePath(tt.path)
			assert.NoError(t, err)
			local, err := SQLFilePathRequiresAdmin(path)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, local)
		})
	}
}

func TestGetForETLDynamicS3OptsDiskRoundTrip(t *testing.T) {
	ctx := WithSQLFileAccess(t.Context(), "sys", "moadmin")
	bucket := t.TempDir()
	spec := JoinPath("s3-opts,endpoint=disk,bucket="+bucket+",prefix=roundtrip", "object")

	fs, path, err := GetForETL(ctx, nil, spec)
	assert.NoError(t, err)
	assert.Equal(t, "object", path)
	defer fs.Close(ctx)

	payload := []byte("owned dynamic fixture")
	assert.NoError(t, fs.Write(ctx, IOVector{
		FilePath: path,
		Entries:  []IOEntry{{Data: payload, Size: int64(len(payload))}},
	}))

	var reader io.ReadCloser
	assert.NoError(t, fs.Read(ctx, &IOVector{
		FilePath: path,
		Entries:  []IOEntry{{Size: -1, ReadCloserForRead: &reader}},
	}))
	got, readErr := io.ReadAll(reader)
	assert.NoError(t, readErr)
	assert.NoError(t, reader.Close())
	assert.Equal(t, payload, got)

	entry, err := fs.StatFile(ctx, path)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(payload)), entry.Size)
}

type dummyFileService struct{ name string }

func (d dummyFileService) Delete(ctx context.Context, filePaths ...string) error { return nil }
func (d dummyFileService) Name() string                                          { return d.name }
func (d dummyFileService) Read(ctx context.Context, vector *IOVector) error      { return nil }
func (d dummyFileService) ReadCache(ctx context.Context, vector *IOVector) error { return nil }
func (d dummyFileService) Write(ctx context.Context, vector IOVector) error      { return nil }
func (d dummyFileService) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		yield(&DirEntry{Name: "a"}, nil)
	}
}
func (d dummyFileService) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	return &DirEntry{Name: filePath}, nil
}
func (d dummyFileService) PrefetchFile(ctx context.Context, filePath string) error { return nil }
func (d dummyFileService) Cost() *CostAttr                                         { return nil }
func (d dummyFileService) Close(ctx context.Context)                               {}

func TestGetFromMappings(t *testing.T) {
	fs1 := dummyFileService{name: "first"}
	fs2 := dummyFileService{name: "second"}
	mapping, err := NewFileServices("first", fs1, fs2)
	assert.NoError(t, err)

	var res FileService
	res, err = Get[FileService](mapping, "second")
	assert.NoError(t, err)
	assert.Equal(t, "second", res.Name())

	_, err = Get[FileService](mapping, "missing")
	assert.Error(t, err)

	res, err = Get[FileService](fs1, "first")
	assert.NoError(t, err)
	assert.Equal(t, "first", res.Name())

	_, err = Get[FileService](fs1, "other")
	assert.Error(t, err)
}

func TestETLProviderAdmissionRejectsMalformedArguments(t *testing.T) {
	for _, spec := range []string{
		"s3,disk:r", "unknown,endpoint=disk:r", "s3-opts,endpoint=disk,is-minio=maybe:r",
		"s3-opts,endpoint=disk,is-hdfs=maybe:r", "s3-opts,concurrency=bad:r",
		"s3-opts,no-default-credentials=bad:r", "s3-opts,max-conns-per-host=bad:r",
	} {
		t.Run(spec, func(t *testing.T) {
			p, err := ParseETLService(spec)
			assert.NoError(t, err)
			_, err = SQLFilePathRequiresAdmin(p)
			assert.Error(t, err)
			_, _, err = GetForETLWithParsedPath(WithSQLFileAccess(t.Context(), "sys", "moadmin"), nil, spec, p)
			assert.Error(t, err)
		})
	}
}

func TestETLProviderAdmissionLeavesGlobToExpansion(t *testing.T) {
	for _, spec := range []string{"/owned/test_[bgnslz]*.parquet", "/owned/test_?.parquet"} {
		p, err := ParseETLService(spec)
		assert.NoError(t, err)
		assert.Equal(t, spec, p.File)
		local, err := SQLFilePathRequiresAdmin(p)
		assert.NoError(t, err)
		assert.True(t, local)
		_, err = ParsePath(spec)
		assert.Error(t, err, "concrete file grammar is unchanged")
	}
}
