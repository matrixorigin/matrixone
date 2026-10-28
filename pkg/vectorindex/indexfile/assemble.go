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
	"errors"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// assembler writes the chunks of one file, in any order, into f. Every chunk must be
// present exactly once with its exact length.
type assembler struct {
	f     *os.File
	ref   *Ref
	n     int64
	seen  []bool
	count int64
}

// stagingError is a failed I/O on the local file a rebuild or write assembles.
type stagingError struct{ err error }

func (e *stagingError) Error() string { return e.err.Error() }
func (e *stagingError) Unwrap() error { return e.err }

func isStagingError(err error) bool {
	var se *stagingError
	return errors.As(err, &se)
}

// beforeStagingWrite, when set by a test, runs before each write to a staging file.
var beforeStagingWrite func(f *os.File) error

func writeStagingAt(f *os.File, b []byte, off int64) (int, error) {
	if beforeStagingWrite != nil {
		if err := beforeStagingWrite(f); err != nil {
			return 0, &stagingError{err}
		}
	}
	n, err := f.WriteAt(b, off)
	if err != nil {
		return n, &stagingError{err}
	}
	return n, nil
}

func newAssembler(f *os.File, ref *Ref) (*assembler, error) {
	if err := f.Truncate(ref.Size); err != nil {
		return nil, &stagingError{err}
	}
	n := ref.numChunks()
	return &assembler{f: f, ref: ref, n: n, seen: make([]bool, n)}, nil
}

// add writes chunk chunkID. Chunks outside the file's range are ignored.
func (a *assembler) add(chunkID int64, data []byte) error {
	i := chunkID - a.ref.FirstChunk
	if i < 0 || i >= a.n {
		return nil
	}
	if a.seen[i] {
		return moerr.NewInternalErrorNoCtxf("index file %s: duplicate chunk %d", a.ref.IndexID, chunkID)
	}
	want := int64(ChunkSize)
	if i == a.n-1 {
		want = a.ref.Size - i*ChunkSize
	}
	if int64(len(data)) != want {
		return moerr.NewInternalErrorNoCtxf("index file %s: chunk %d has %d bytes, want %d",
			a.ref.IndexID, chunkID, len(data), want)
	}
	if _, err := writeStagingAt(a.f, data, i*ChunkSize); err != nil {
		return err
	}
	a.seen[i] = true
	a.count++
	return nil
}

func (a *assembler) finish() error {
	if a.count == a.n {
		return nil
	}
	for i, ok := range a.seen {
		if !ok {
			return moerr.NewInternalErrorNoCtxf("index file %s: missing chunk %d of %d chunks from %d",
				a.ref.IndexID, a.ref.FirstChunk+int64(i), a.n, a.ref.FirstChunk)
		}
	}
	return nil
}
