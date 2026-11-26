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
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

func probeSetMemory(fs *S3FS, n int64) { fs.server.memoryLimit = n }

func probeMemoryBudget(fs *S3FS) int64 { return fs.server.memoryBudget() }

func probeMemoryHeld(fs *S3FS) int64 { return heldMemory(fs.diskCache) }

func probeMemoryFits(fs *S3FS, n int64) bool { return fs.server.memoryFits(n, foregroundRequest) }

func probeArgsMemory(args ObjectStorageArguments) toml.ByteSize { return args.ObjectStorageMemory }

func probeRaw(fs *S3FS) ObjectStorage { return fs.server.raw }

func probeSetRaw(fs *S3FS, raw ObjectStorage) { fs.server.raw = raw }

func probeCounts(fs *S3FS) (running, waiting [2]int) { return fs.server.counts() }

func probeStat(ctx context.Context, fs *S3FS, name string) error {
	_, err := fs.server.Do(ctx, statRequest{key: fs.pathToKey(name)})
	return err
}

// probeFillRoom holds all the free room of fs, background first, and returns the function
// freeing each.
func probeFillRoom(t *testing.T, fs *S3FS) []func() {
	var frees []func()
	for _, c := range []requestClass{backgroundRequest, foregroundRequest} {
		for {
			free, ok := admitted(fs.server, c)
			if !ok {
				break
			}
			frees = append(frees, sync.OnceFunc(free))
		}
	}
	return frees
}

// probeParts is object storage that writes in parts, reading and dropping what it is
// sent; other calls go to the object storage it wraps.
type probeParts struct {
	ObjectStorage
}

func newProbeParts(raw ObjectStorage) ObjectStorage { return probeParts{raw} }

func (probeParts) Write(_ context.Context, _ string, r io.Reader, _ *int64, _ *time.Time) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (probeParts) createMultipart(context.Context, string, *time.Time) (string, error) {
	return "upload", nil
}

func (probeParts) uploadPart(_ context.Context, _, _ string, _ int32, body io.ReaderAt, size int64) (string, error) {
	_, err := io.Copy(io.Discard, io.NewSectionReader(body, 0, size))
	return "etag", err
}

func (probeParts) completeMultipart(context.Context, string, string, []multipartPart) error {
	return nil
}

func (probeParts) abortMultipart(context.Context, string, string) error { return nil }
