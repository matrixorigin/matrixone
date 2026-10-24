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

func newAssembler(f *os.File, ref *Ref) (*assembler, error) {
	if err := f.Truncate(ref.Size); err != nil {
		return nil, err
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
	if _, err := a.f.WriteAt(data, i*ChunkSize); err != nil {
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
