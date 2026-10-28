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

package indexfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Env is what reading or writing an index file needs: the storage engine, the caller's
// process and transaction (possibly pinned to a snapshot), and the CN's index-file cache
// (nil when the CN has none).
type Env struct {
	Engine engine.Engine
	Proc   *process.Process
	Txn    client.TxnOperator
	Cache  *fileservice.IndexFileCache
}

// File is an open index file. The caller closes it.
type File struct {
	f    *os.File
	size int64
}

// File returns the open file. It stays readable until Close, even if the cache evicts it.
func (f *File) File() *os.File { return f.f }

// Size returns the file size.
func (f *File) Size() int64 { return f.size }

// FDPath returns a path that opens this file through its open descriptor (/dev/fd/N), for
// libraries that take a path. It stays valid until Close, even after the cache evicts the
// file, and must not be used after Close, when the descriptor number can be reused. On
// Linux opening it creates an independent file description; on macOS it duplicates the
// descriptor and shares its offset, so readers of the File use ReadAt.
func (f *File) FDPath() string { return fmt.Sprintf("/dev/fd/%d", f.f.Fd()) }

// Close closes the file.
func (f *File) Close() error { return f.f.Close() }

// Open returns the file ref names, as visible to env's transaction: from the index-file
// cache when present, otherwise rebuilt from its chunk rows, verified and cached.
func Open(ctx context.Context, env *Env, ref Ref) (*File, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}
	rel, err := openRelation(ctx, env, &ref)
	if err != nil {
		return nil, err
	}
	if env.Cache == nil {
		return rebuildTemp(ctx, env, rel, &ref)
	}

	name := entryName(rel.GetTableID(ctx), &ref)
	if f, err := openCached(ctx, env.Cache, name, &ref); f != nil || err != nil {
		return f, err
	}

	unlock, err := rebuildLocks.lock(ctx, env.Cache.Path()+"\x00"+name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if f, err := openCached(ctx, env.Cache, name, &ref); f != nil || err != nil {
		return f, err
	}
	return rebuildCached(ctx, env, rel, &ref, name)
}

// verified holds the cache entries whose full checksum was checked since the process
// started; later opens check the length only. It is cleared when it reaches
// maxVerified entries, which only costs a full check on the next open of each entry.
var verified = verifiedSet{m: make(map[string]struct{})}

const maxVerified = 1 << 16

type verifiedSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func (v *verifiedSet) has(key string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.m[key]
	return ok
}

func (v *verifiedSet) add(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.m) >= maxVerified {
		clear(v.m)
	}
	v.m[key] = struct{}{}
}

func (v *verifiedSet) remove(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.m, key)
}

// openCached opens entry name. A missing, invalid or unreadable entry is a miss
// (nil, nil); an invalid entry is deleted. Only cancellation is an error.
func openCached(ctx context.Context, cache *fileservice.IndexFileCache, name string, ref *Ref) (*File, error) {
	files, err := cache.Open(ctx, name, dataFile, manifestFile)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			logutil.Warn("index file cache open failed", zap.String("entry", name), zap.Error(err))
		}
		return nil, nil
	}
	data, manifest := files[0], files[1]
	defer manifest.Close()
	key := cache.Path() + "\x00" + name
	if err := checkCached(data, manifest, ref, key); err != nil {
		_ = data.Close()
		verified.remove(key)
		logutil.Warn("index file cache entry invalid, rebuilding", zap.String("entry", name), zap.Error(err))
		if delErr := cache.Delete(ctx, name); delErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
		}
		return nil, nil
	}
	return &File{f: data, size: ref.Size}, nil
}

func checkCached(data, manifest *os.File, ref *Ref, key string) error {
	b, err := io.ReadAll(io.LimitReader(manifest, manifestLen+1))
	if err != nil {
		return err
	}
	size, crc, err := decodeManifest(b)
	if err != nil {
		return err
	}
	stat, err := data.Stat()
	if err != nil {
		return err
	}
	if size != ref.Size || stat.Size() != ref.Size {
		return moerr.NewInternalErrorNoCtxf("index file %s: cached size %d/%d, want %d", ref.IndexID, size, stat.Size(), ref.Size)
	}
	if verified.has(key) {
		return nil
	}
	got, err := fileCRC(data, size)
	if err != nil {
		return err
	}
	if got != crc {
		return moerr.NewInternalErrorNoCtxf("index file %s: cached file checksum mismatch", ref.IndexID)
	}
	verified.add(key)
	return nil
}

// rebuildCached rebuilds the file into a cache temp file and publishes it with its
// manifest. If the cache cannot take it, the caller still gets the rebuilt file.
func rebuildCached(ctx context.Context, env *Env, rel engine.Relation, ref *Ref, name string) (*File, error) {
	tmp, remove, err := env.Cache.CreateTempFile()
	if err != nil {
		return rebuildTemp(ctx, env, rel, ref)
	}
	defer remove()
	crc, err := rebuildInto(ctx, env, rel, ref, tmp)
	if err != nil {
		_ = tmp.Close()
		if isStagingError(err) {
			return rebuildTemp(ctx, env, rel, ref)
		}
		return nil, err
	}
	written, err := env.Cache.Put(ctx, name, []fileservice.IndexCacheFile{
		{Name: dataFile, Path: tmp.Name()},
		{Name: manifestFile, Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(encodeManifest(ref.Size, crc))), nil
		}},
	})
	if err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if written {
		verified.add(env.Cache.Path() + "\x00" + name)
	}
	if f, err := openCached(ctx, env.Cache, name, ref); f != nil || err != nil {
		_ = tmp.Close()
		return f, err
	}
	// Not cached (cache write failed or the entry was evicted at once): serve the
	// rebuilt file, which stays readable through tmp after remove.
	return &File{f: tmp, size: ref.Size}, nil
}

// rebuildTemp rebuilds the file into a private temp file removed on close.
func rebuildTemp(ctx context.Context, env *Env, rel engine.Relation, ref *Ref) (*File, error) {
	tmp, err := os.CreateTemp("", "indexfile-*")
	if err != nil {
		return nil, err
	}
	_ = os.Remove(tmp.Name())
	if _, err := rebuildInto(ctx, env, rel, ref, tmp); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	return &File{f: tmp, size: ref.Size}, nil
}

// rebuildInto writes the file's chunk rows into f, verifies it and returns its CRC32-C.
func rebuildInto(ctx context.Context, env *Env, rel engine.Relation, ref *Ref, f *os.File) (uint32, error) {
	asm, err := newAssembler(f, ref)
	if err != nil {
		return 0, err
	}
	if env.Cache != nil {
		policy := fileservice.GetFileServicePolicy(ctx) | env.Cache.RebuildPolicy()
		ctx = fileservice.WithFileServicePolicy(ctx, policy)
	}
	if err := scanChunks(ctx, env, rel, ref, asm.add); err != nil {
		return 0, err
	}
	if err := asm.finish(); err != nil {
		return 0, err
	}
	verify := ref.Verify
	if verify == nil {
		verify = VerifyMD5
	}
	if err := verify(f, ref.Size, ref.Checksum); err != nil {
		return 0, err
	}
	crc, err := fileCRC(f, ref.Size)
	if err != nil {
		return 0, &stagingError{err}
	}
	return crc, nil
}

var rebuildLocks = keyedLocks{m: make(map[string]*keyedLock)}

type keyedLocks struct {
	mu sync.Mutex
	m  map[string]*keyedLock
}

type keyedLock struct {
	sem  chan struct{}
	refs int
}

// lock serializes callers by key until ctx is done. Entries are removed when unused.
func (k *keyedLocks) lock(ctx context.Context, key string) (unlock func(), err error) {
	k.mu.Lock()
	l := k.m[key]
	if l == nil {
		l = &keyedLock{sem: make(chan struct{}, 1)}
		k.m[key] = l
	}
	l.refs++
	k.mu.Unlock()

	release := func() {
		k.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
	select {
	case l.sem <- struct{}{}:
		return func() {
			<-l.sem
			release()
		}, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}
