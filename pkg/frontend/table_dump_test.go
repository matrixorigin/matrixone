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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/stage"
)

type testTableDumpObjectCopier struct {
	fileservice.FileService
	data   []byte
	copied bool
}

type testConcurrentTableDumpObjectCopier struct {
	fileservice.FileService
	data        []byte
	release     chan struct{}
	active      atomic.Int32
	maximum     atomic.Int32
	statRelease chan struct{}
	statActive  atomic.Int32
	statMaximum atomic.Int32
}

type testAmbiguousWriteFileService struct {
	fileservice.FileService
}

func (f *testAmbiguousWriteFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := f.FileService.Write(ctx, vector); err != nil {
		return err
	}
	return errors.New("write response lost after destination was created")
}

func (c *testConcurrentTableDumpObjectCopier) CopyObject(
	ctx context.Context,
	_ fileservice.FileService,
	_ string,
	dstPath string,
) (bool, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maximum := c.maximum.Load()
		if active <= maximum || c.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.release:
	}
	err := c.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(c.data)), Data: c.data}},
	})
	return true, err
}

func (c *testConcurrentTableDumpObjectCopier) StatFile(
	ctx context.Context,
	filePath string,
) (*fileservice.DirEntry, error) {
	if c.statRelease == nil {
		return c.FileService.StatFile(ctx, filePath)
	}
	active := c.statActive.Add(1)
	defer c.statActive.Add(-1)
	for {
		maximum := c.statMaximum.Load()
		if active <= maximum || c.statMaximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.statRelease:
	}
	return c.FileService.StatFile(ctx, filePath)
}

func (c *testTableDumpObjectCopier) CopyObject(
	ctx context.Context,
	_ fileservice.FileService,
	_ string,
	dstPath string,
) (bool, error) {
	c.copied = true
	err := c.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(c.data)), Data: c.data}},
	})
	return true, err
}

func TestOpenLocalTableDump(t *testing.T) {
	_, err := openLocalTableDump("relative/path")
	require.Error(t, err)
	_, err = openLocalTableDump("s3://bucket/path")
	require.Error(t, err)

	fs, err := openLocalTableDump("file://" + t.TempDir())
	require.NoError(t, err)
	fs.Close(context.Background())
}

func TestOpenStageTableDump(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	root := t.TempDir()
	ses.proc.GetStageCache().Set("fixture", stage.StageDef{
		Id: 1, Name: "fixture", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: root},
	})

	fs, closeFS, err := openTableDumpFS(context.Background(), ses, "stage://fixture/tpch100g/lineitem")
	require.NoError(t, err)
	defer closeFS()
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: tableDumpReadyName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}},
	}))
	_, err = fs.StatFile(context.Background(), tableDumpReadyName)
	require.NoError(t, err)
}

func TestTableDumpManifestAndCopy(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)

	content := []byte("native object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))
	size, hash, err := copyTableDumpFile(ctx, src, fixture, "obj", "objects/obj")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)

	manifest := &tableDumpManifest{Version: tableDumpFormatVersion, Relations: []tableDumpRelation{{
		Role: "main", Objects: []tableDumpObject{{
			Name: "obj", Size: size, SHA256: hash, FixturePath: "objects/obj",
		}},
	}}}
	require.NoError(t, writeTableDumpJSON(ctx, fixture, tableDumpManifestName, manifest))
	read, err := readTableDumpManifest(ctx, fixture)
	require.NoError(t, err)
	require.Equal(t, manifest.Relations, read.Relations)
	err = installTableDumpObject(ctx, fixture, dst, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.NoError(t, verifyTableDumpObject(ctx, dst, "obj", size, hash))

	serverSideDst, err := fileservice.NewLocalETLFS("server-side-dst", t.TempDir())
	require.NoError(t, err)
	serverSideCopier := &testTableDumpObjectCopier{FileService: serverSideDst, data: content}
	err = installTableDumpObject(ctx, fixture, serverSideCopier, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.NoError(t, verifyTableDumpObject(ctx, serverSideCopier, "obj", size, hash))

	badDst, err := fileservice.NewLocalETLFS("bad-dst", t.TempDir())
	require.NoError(t, err)
	badCopier := &testTableDumpObjectCopier{FileService: badDst, data: []byte("corrupt object byte")}
	err = installTableDumpObject(ctx, fixture, badCopier, read.Relations[0].Objects[0])
	require.Error(t, err)
	_, err = badDst.StatFile(ctx, "obj")
	require.NoError(t, err)
}

func TestCopyTableDumpFilePrefersObjectCopier(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))
	copier := &testTableDumpObjectCopier{FileService: dst, data: content}

	size, hash, err := copyTableDumpFile(ctx, src, copier, "obj", "objects/obj")
	require.NoError(t, err)
	require.True(t, copier.copied)
	require.Equal(t, int64(len(content)), size)
	require.Empty(t, hash)
	require.NoError(t, verifyTableDumpObject(ctx, copier, "objects/obj", size, hash))
}

func TestCopyTableDumpFileTracksAmbiguousWriteOwnership(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))

	_, _, err = copyTableDumpFile(
		ctx, src, &testAmbiguousWriteFileService{FileService: dst}, "obj", "objects/obj",
	)
	require.Error(t, err)
	_, err = dst.StatFile(ctx, "objects/obj")
	require.NoError(t, err)
}

func TestCopyTableDumpObjectsUsesBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		require.NoError(t, src.Write(ctx, fileservice.IOVector{
			FilePath: name,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: path.Join("objects", name)}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	type copyResult struct {
		written []string
		err     error
	}
	resultCh := make(chan copyResult, 1)
	go func() {
		written, err := copyTableDumpObjects(ctx, src, copier, objects)
		resultCh <- copyResult{written: written, err: err}
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.release)
	result := <-resultCh
	require.NoError(t, result.err)
	require.Len(t, result.written, len(objects))
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.maximum.Load())
	for i := range objects {
		require.Equal(t, int64(len(content)), objects[i].Size)
		require.Empty(t, objects[i].SHA256)
	}
}

func TestCopyTableDumpObjectsValidatesWithBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		require.NoError(t, src.Write(ctx, fileservice.IOVector{
			FilePath: name,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: path.Join("objects", name)}
	}
	copyRelease := make(chan struct{})
	close(copyRelease)
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     copyRelease,
		statRelease: make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(copier.statRelease)
		}
	}()
	type copyResult struct {
		written []string
		err     error
	}
	resultCh := make(chan copyResult, 1)
	go func() {
		written, err := copyTableDumpObjects(ctx, src, copier, objects)
		resultCh <- copyResult{written: written, err: err}
	}()
	require.Eventually(t, func() bool {
		return copier.statMaximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.statRelease)
	released = true
	result := <-resultCh
	require.NoError(t, result.err)
	require.Len(t, result.written, len(objects))
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.statMaximum.Load())
	for i := range objects {
		require.Equal(t, int64(len(content)), objects[i].Size)
	}
}

func TestInstallTableDumpObjectsUsesBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		fixturePath := path.Join("objects", name)
		require.NoError(t, fixture.Write(ctx, fileservice.IOVector{
			FilePath: fixturePath,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: fixturePath, Size: int64(len(content))}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(copier.release)
		}
	}()
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects)
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.release)
	released = true
	require.NoError(t, <-resultCh)
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.maximum.Load())
	for i := range objects {
		require.NoError(t, verifyTableDumpObject(ctx, copier, objects[i].Name, objects[i].Size, ""))
	}
}

func TestInstallTableDumpObjectsCancelsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		fixturePath := path.Join("objects", name)
		require.NoError(t, fixture.Write(ctx, fileservice.IOVector{
			FilePath: fixturePath,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: fixturePath, Size: int64(len(content))}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects)
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-resultCh, context.Canceled)
}

func TestTableSchemaHashIgnoresIdentity(t *testing.T) {
	left := &plan.TableDef{TblId: 1, DbId: 2, DbName: "a", Name: "t", Createsql: "create table a.t(a int)", Cols: []*plan.ColDef{{Name: "a"}}}
	right := &plan.TableDef{TblId: 9, DbId: 8, DbName: "b", Name: "t2", Createsql: "create table b.t2(a int)", Cols: []*plan.ColDef{{Name: "a"}}}
	leftHash, err := tableSchemaHash(left)
	require.NoError(t, err)
	rightHash, err := tableSchemaHash(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	right.Createsql = "create table b.t2(b int)"
	rightHash, err = tableSchemaHash(right)
	require.NoError(t, err)
	require.NotEqual(t, leftHash, rightHash)
}

func TestTableSchemaHashExpandsCreateLike(t *testing.T) {
	column := func(typ types.T) *plan.ColDef {
		return &plan.ColDef{
			Name: "a",
			Typ:  plan.Type{Id: int32(typ)},
			Default: &plan.Default{
				NullAbility: false,
			},
		}
	}
	left := &plan.TableDef{Name: "src", Createsql: "create table src(a bigint)", Cols: []*plan.ColDef{column(types.T_int64)}}
	right := &plan.TableDef{Name: "dst", Createsql: "create table dst like src", Cols: []*plan.ColDef{column(types.T_int64)}}
	leftHash, err := tableSchemaHash(left)
	require.NoError(t, err)
	rightHash, err := tableSchemaHash(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	right.Cols[0] = column(types.T_int32)
	rightHash, err = tableSchemaHash(right)
	require.NoError(t, err)
	require.NotEqual(t, leftHash, rightHash)
}
