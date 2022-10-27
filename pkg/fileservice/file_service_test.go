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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io"
	mrand "math/rand"
	"path"
	"sort"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func testFileService(
	t *testing.T,
	newFS func(name string) FileService,
) {

	fsName := time.Now().Format("fs-2006-01-02-15-04-05")

	t.Run("basic", func(t *testing.T) {
		ctx := context.Background()
		fs := newFS(fsName)

		assert.Equal(t, fsName, fs.Name())

		err := fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("1234"),
				},
				{
					Offset: 4,
					Size:   4,
					Data:   []byte("5678"),
				},
				{
					Offset:         8,
					Size:           3,
					ReaderForWrite: bytes.NewReader([]byte("9ab")),
				},
			},
		})
		assert.Nil(t, err)

		buf1 := new(bytes.Buffer)
		var r io.ReadCloser
		buf2 := make([]byte, 4)
		vec := IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				0: {
					Offset: 2,
					Size:   2,
				},
				1: {
					Offset: 2,
					Size:   4,
					Data:   buf2,
				},
				2: {
					Offset: 7,
					Size:   1,
				},
				3: {
					Offset: 0,
					Size:   1,
				},
				4: {
					Offset:            0,
					Size:              7,
					ReadCloserForRead: &r,
				},
				5: {
					Offset:        4,
					Size:          2,
					WriterForRead: buf1,
				},
				6: {
					Offset: 0,
					Size:   -1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("34"), vec.Entries[0].Data)
		assert.Equal(t, []byte("3456"), vec.Entries[1].Data)
		assert.Equal(t, []byte("3456"), buf2)
		assert.Equal(t, []byte("8"), vec.Entries[2].Data)
		assert.Equal(t, []byte("1"), vec.Entries[3].Data)
		content, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Nil(t, r.Close())
		assert.Equal(t, []byte("1234567"), content)
		assert.Equal(t, []byte("56"), buf1.Bytes())
		assert.Equal(t, []byte("123456789ab"), vec.Entries[6].Data)

		// read from non-zero offset
		vec = IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 7,
					Size:   1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("8"), vec.Entries[0].Data)

		// sub path
		err = fs.Write(ctx, IOVector{
			FilePath: "sub/sub2/sub3",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   1,
					Data:   []byte("1"),
				},
			},
		})
		assert.Nil(t, err)

	})

	t.Run("random", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		for i := 0; i < 8; i++ {
			filePath := fmt.Sprintf("%d", mrand.Int63())

			// random content
			content := make([]byte, 512)
			_, err := rand.Read(content)
			assert.Nil(t, err)
			parts := randomSplit(content, 32)

			// write
			writeVector := IOVector{
				FilePath: filePath,
			}
			offset := int64(0)
			for _, part := range parts {
				writeVector.Entries = append(writeVector.Entries, IOEntry{
					Offset: offset,
					Size:   int64(len(part)),
					Data:   part,
				})
				offset += int64(len(part))
			}
			err = fs.Write(ctx, writeVector)
			assert.Nil(t, err)

			// read, align to write vector
			readVector := &IOVector{
				FilePath: filePath,
			}
			for _, entry := range writeVector.Entries {
				readVector.Entries = append(readVector.Entries, IOEntry{
					Offset: entry.Offset,
					Size:   entry.Size,
				})
			}
			err = fs.Read(ctx, readVector)
			assert.Nil(t, err)
			for i, entry := range readVector.Entries {
				assert.Equal(t, parts[i], entry.Data, "part %d, got %+v", i, entry)
			}

			// read, random entry
			parts2 := randomSplit(content, 16)
			readVector.Entries = readVector.Entries[:0]
			offset = int64(0)
			for _, part := range parts2 {
				readVector.Entries = append(readVector.Entries, IOEntry{
					Offset: offset,
					Size:   int64(len(part)),
				})
				offset += int64(len(part))
			}
			err = fs.Read(ctx, readVector)
			assert.Nil(t, err)
			for i, entry := range readVector.Entries {
				assert.Equal(t, parts2[i], entry.Data, "path: %s, entry: %+v, content %v", filePath, entry, content)
			}

			// list
			entries, err := fs.List(ctx, "/")
			assert.Nil(t, err)
			for _, entry := range entries {
				if entry.Name != filePath {
					continue
				}
				assert.Equal(t, filePath, entry.Name)
				assert.Equal(t, false, entry.IsDir)
				assert.Equal(t, int64(len(content)), entry.Size)
			}

		}
	})

	t.Run("tree", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		for _, dir := range []string{
			"",
			"foo",
			"bar",
			"qux/quux",
		} {
			for i := int64(0); i < 8; i++ {
				err := fs.Write(ctx, IOVector{
					FilePath: path.Join(dir, fmt.Sprintf("%d", i)),
					Entries: []IOEntry{
						{
							Size: i,
							Data: []byte(strings.Repeat(fmt.Sprintf("%d", i), int(i))),
						},
					},
				})
				assert.Nil(t, err)
			}
		}

		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 11)
		sort.Slice(entries, func(i, j int) bool {
			a := entries[i]
			b := entries[j]
			if a.IsDir && !b.IsDir {
				return true
			} else if !a.IsDir && b.IsDir {
				return false
			}
			return a.Name < b.Name
		})
		assert.Equal(t, entries[0].IsDir, true)
		assert.Equal(t, entries[0].Name, "bar")
		assert.Equal(t, entries[1].IsDir, true)
		assert.Equal(t, entries[1].Name, "foo")
		assert.Equal(t, entries[2].IsDir, true)
		assert.Equal(t, entries[2].Name, "qux")
		assert.Equal(t, entries[3].IsDir, false)
		assert.Equal(t, entries[3].Name, "0")
		assert.Equal(t, entries[3].Size, int64(0))
		assert.Equal(t, entries[10].IsDir, false)
		assert.Equal(t, entries[10].Name, "7")
		if _, ok := fs.(ETLFileService); ok {
			assert.Equal(t, entries[10].Size, int64(7))
		}

		entries, err = fs.List(ctx, "abc")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 0)

		entries, err = fs.List(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		entries, err = fs.List(ctx, "qux/quux")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		for _, entry := range entries {
			err := fs.Delete(ctx, path.Join("qux/quux", entry.Name))
			assert.Nil(t, err)
		}
		entries, err = fs.List(ctx, "qux/quux")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 0)

	})

	t.Run("errors", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		err := fs.Read(ctx, &IOVector{
			FilePath: "foo",
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyVector))

		err = fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 2,
					Data: []byte("ab"),
				},
			},
		})
		assert.Nil(t, err)
		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))

		err = fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   3,
				},
			},
		})
		assert.True(t, moerr.IsMoErrCode(moerr.ConvertGoError(err), moerr.ErrUnexpectedEOF))

		err = fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 1,
					Size:   0,
				},
			},
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))

		err = fs.Write(ctx, IOVector{
			FilePath: "bar",
			Entries: []IOEntry{
				{
					Size: 1,
				},
			},
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch))

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					ReaderForWrite: iotest.ErrReader(io.ErrNoProgress),
				},
			},
		})
		// fs leaking io error, but I don't know what this test really tests.
		// assert.True(t, err == io.ErrNoProgress)
		// assert.True(t, moerr.IsMoErrCode(moerr.ConvertGoError(err), moerr.ErrInternal))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))

		vector := IOVector{
			FilePath: joinPath(fsName, "a:b:c"),
			Entries: []IOEntry{
				{Size: 1, Data: []byte("a")},
			},
		}
		err = fs.Write(ctx, vector)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidPath))
		err = fs.Read(ctx, &vector)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidPath))
		_, err = fs.List(ctx, vector.FilePath)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidPath))
		err = fs.Delete(ctx, vector.FilePath)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidPath))
	})

	t.Run("object", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		buf := new(bytes.Buffer)
		err := gob.NewEncoder(buf).Encode(map[int]int{
			42: 42,
		})
		assert.Nil(t, err)
		data := buf.Bytes()
		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					Data: data,
				},
			},
		})
		assert.Nil(t, err)

		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					ToObject: func(r io.Reader) (any, int64, error) {
						var m map[int]int
						if err := gob.NewDecoder(r).Decode(&m); err != nil {
							return nil, 0, err
						}
						return m, 1, nil
					},
				},
			},
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)

		m, ok := vec.Entries[0].Object.(map[int]int)
		assert.True(t, ok)
		assert.Equal(t, 1, len(m))
		assert.Equal(t, 42, m[42])
		assert.Equal(t, int64(1), vec.Entries[0].ObjectSize)

	})

	t.Run("ignore", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		data := []byte("foo")
		err := fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					Data: data,
				},
			},
		})
		assert.Nil(t, err)

		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size:   int64(len(data)),
					ignore: true,
				},
				{
					Size: int64(len(data)),
				},
			},
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)

		assert.Nil(t, vec.Entries[0].Data)
		assert.Equal(t, []byte("foo"), vec.Entries[1].Data)

	})

	t.Run("named path", func(t *testing.T) {
		ctx := context.Background()
		fs := newFS(fsName)

		// write
		err := fs.Write(ctx, IOVector{
			FilePath: joinPath(fs.Name(), "foo"),
			Entries: []IOEntry{
				{
					Size: 4,
					Data: []byte("1234"),
				},
			},
		})
		assert.Nil(t, err)

		// read
		vec := IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// read with lower named path
		vec = IOVector{
			FilePath: joinPath(strings.ToLower(fs.Name()), "foo"),
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// read with upper named path
		vec = IOVector{
			FilePath: joinPath(strings.ToUpper(fs.Name()), "foo"),
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// bad name
		vec.FilePath = joinPath(fs.Name()+"abc", "foo")
		err = fs.Read(ctx, &vec)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
		err = fs.Write(ctx, vec)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
		err = fs.Delete(ctx, vec.FilePath)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
	})

}

func randomSplit(data []byte, maxLen int) (ret [][]byte) {
	for {
		if len(data) == 0 {
			return
		}
		if len(data) < maxLen {
			ret = append(ret, data)
			return
		}
		cut := 1 + mrand.Intn(maxLen)
		ret = append(ret, data[:cut])
		data = data[cut:]
	}
}

func fixedSplit(data []byte, l int) (ret [][]byte) {
	for {
		if len(data) == 0 {
			return
		}
		if len(data) < l {
			ret = append(ret, data)
			return
		}
		ret = append(ret, data[:l])
		data = data[l:]
	}
}
