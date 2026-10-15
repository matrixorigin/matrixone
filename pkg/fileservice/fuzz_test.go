// Copyright 2023 Matrix Origin
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
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

func fuzzFS(
	newFS func(*perfcounter.CounterSet) FileService,
	do func(
		op func(t *testing.T, input uint64),
	),
) {

	ctx := context.Background()
	counterSet := new(perfcounter.CounterSet)

	fs := newFS(counterSet)
	defer fs.Close(ctx)

	type FileInfo struct {
		Path    string
		Content []byte
	}
	files := new(sync.Map)

	policies := []Policy{
		0,
	}

	create := func(t *testing.T, i uint64) {
		name := fmt.Sprintf("%d", i)
		size := i%1024 + 1
		content := bytes.Repeat([]byte("abcd"), int(size))
		err := fs.Write(ctx, IOVector{
			FilePath: name,
			Entries: []IOEntry{
				{
					Offset:      0,
					Size:        int64(len(content)),
					Data:        content,
					ToCacheData: CacheOriginalData,
				},
			},
			Policy: policies[int(i)%len(policies)],
		})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
				return
			}
			t.Fatal(err)
		}
		files.Store(name, FileInfo{
			Path:    name,
			Content: content,
		})
	}

	read := func(t *testing.T, i uint64) {
		var info FileInfo
		ok := false
		files.Range(func(_, v any) bool {
			info = v.(FileInfo)
			ok = true
			return false
		})
		if !ok {
			return
		}

		offset := i % uint64(len(info.Content))

		vec := &IOVector{
			FilePath: info.Path,
			Entries: []IOEntry{
				{
					Offset:      int64(offset),
					Size:        -1,
					ToCacheData: CacheOriginalData,
				},
			},
			Policy: policies[int(i)%len(policies)],
		}
		defer vec.Release()
		if err := fs.Read(ctx, vec); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
				// ignore
				return
			}
			t.Fatal(err)
		}

		// ToCacheData makes CachedData authoritative; raw Data may be released.
		cached := vec.Entries[0].CachedData
		if cached == nil {
			t.Fatalf("read %s at offset %d: missing CachedData", info.Path, offset)
		}
		if !bytes.Equal(cached.Bytes(), info.Content[offset:]) {
			t.Fatalf("read %s at offset %d: got %q, want %q", info.Path, offset, cached.Bytes(), info.Content[offset:])
		}
	}

	var ops []func(*testing.T, uint64)
	for i := 0; i < 1; i++ {
		ops = append(ops, create)
	}
	for i := 0; i < 10; i++ {
		ops = append(ops, read)
	}

	do(func(t *testing.T, input uint64) {
		ops[input%uint64(len(ops))](t, input)
	})

}

func newDiskS3ForTesting(
	dir string,
	cacheDir string,
	counterSet *perfcounter.CounterSet,
) (FileService, error) {
	fs, err := NewFileService(
		context.Background(),
		Config{
			Name:    "fuzz",
			Backend: "S3",
			S3: ObjectStorageArguments{
				Endpoint: "disk",
				Bucket:   dir,
			},
			Cache: CacheConfig{
				MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
				DiskPath:       ptrTo(cacheDir),
				DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
				CheckOverlaps:  false,
			},
		},
		[]*perfcounter.CounterSet{
			counterSet,
		},
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func FuzzDiskS3(f *testing.F) {
	fuzzFS(
		func(counterSet *perfcounter.CounterSet) FileService {
			fs, err := newDiskS3ForTesting(f.TempDir(), f.TempDir(), counterSet)
			if err != nil {
				f.Fatal(err)
			}
			return fs
		},
		func(
			op func(t *testing.T, input uint64),
		) {
			f.Fuzz(func(t *testing.T, input uint64) {
				op(t, input)
			})
		},
	)
}

func TestFuzzingDiskS3(t *testing.T) {
	var diskCache *DiskCache
	fuzzFS(
		func(counterSet *perfcounter.CounterSet) FileService {
			fs, err := newDiskS3ForTesting(t.TempDir(), t.TempDir(), counterSet)
			if err != nil {
				t.Fatal(err)
			}
			diskCache = fs.(*S3FS).diskCache
			return fs
		},
		func(
			op func(t *testing.T, input uint64),
		) {
			const (
				numWorkers    = 8
				opsPerWorker  = 128
				operationSeed = uint64(1)
			)

			// A full async queue must not turn a successful storage write into
			// a failed read. Skip the seed's disk fill, then read it cold before
			// concurrent operations: ToCacheData permits the raw Data to be nil.
			func() {
				for i := 0; i < cap(diskCache.async.slots); i++ {
					diskCache.async.slots <- struct{}{}
				}
				defer func() {
					for i := 0; i < cap(diskCache.async.slots); i++ {
						<-diskCache.async.slots
					}
				}()
				op(t, 0)
			}()
			op(t, 1)

			start := make(chan struct{})
			var wg sync.WaitGroup
			for worker := 0; worker < numWorkers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					t.Run(fmt.Sprintf("worker-%d", worker), func(t *testing.T) {
						<-start
						base := operationSeed + uint64(worker*opsPerWorker)
						for i := 0; i < opsPerWorker; i++ {
							op(t, base+uint64(i))
						}
					})
				}(worker)
			}
			close(start)
			wg.Wait()
		},
	)
}
