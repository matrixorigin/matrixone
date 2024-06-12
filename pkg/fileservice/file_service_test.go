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
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func testFileService(
	t *testing.T,
	policy Policy,
	newFS func(name string) FileService,
) {

	fsName := time.Now().Format("fs-2006-01-02-15-04-05")

	t.Run("basic", func(t *testing.T) {
		ctx := context.Background()
		fs := newFS(fsName)

		assert.True(t, strings.Contains(fs.Name(), fsName))

		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.Equal(t, 0, len(entries))

		err = fs.Write(ctx, IOVector{
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
			Policy: policy,
		})
		assert.Nil(t, err)

		entries, err = fs.List(ctx, "")
		assert.Nil(t, err)
		assert.Equal(t, 1, len(entries))

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
			Policy: policy,
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

		// stat
		entry, err := fs.StatFile(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "foo", entry.Name)
		assert.Equal(t, false, entry.IsDir)
		assert.Equal(t, int64(11), entry.Size)

		// read from non-zero offset
		vec = IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 7,
					Size:   1,
				},
			},
			Policy: policy,
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
			Policy: policy,
		})
		assert.Nil(t, err)

	})

	t.Run("WriterForRead", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		err := fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("1234"),
				},
			},
			Policy: policy,
		})
		assert.Nil(t, err)

		buf := new(bytes.Buffer)
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:        0,
					Size:          4,
					WriterForRead: buf,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), buf.Bytes())

		buf = new(bytes.Buffer)
		vec = &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:        0,
					Size:          -1,
					WriterForRead: buf,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), buf.Bytes())

	})

	t.Run("ReadCloserForRead", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()
		err := fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("1234"),
				},
			},
			Policy: policy,
		})
		assert.Nil(t, err)

		var r io.ReadCloser

		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:            0,
					Size:              4,
					ReadCloserForRead: &r,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		data, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), data)
		err = r.Close()
		assert.Nil(t, err)

		vec = &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:            0,
					Size:              3,
					ReadCloserForRead: &r,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		data, err = io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, []byte("123"), data)
		err = r.Close()
		assert.Nil(t, err)

		vec = &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:            1,
					Size:              3,
					ReadCloserForRead: &r,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		data, err = io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, []byte("234"), data)
		err = r.Close()
		assert.Nil(t, err)

	})

	t.Run("random", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		for i := 0; i < 8; i++ {
			filePath := fmt.Sprintf("%d", mrand.Int63())

			// random content
			content := make([]byte, _BlockContentSize*4)
			_, err := rand.Read(content)
			assert.Nil(t, err)
			parts := randomSplit(content, 32)

			// write
			writeVector := IOVector{
				FilePath: filePath,
				Policy:   policy,
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
				Policy:   policy,
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
			readVector.Release()

			// read, random entry
			parts = randomSplit(content, 16)
			readVector.Entries = readVector.Entries[:0]
			offset = int64(0)
			for _, part := range parts {
				readVector.Entries = append(readVector.Entries, IOEntry{
					Offset: offset,
					Size:   int64(len(part)),
				})
				offset += int64(len(part))
			}
			err = fs.Read(ctx, readVector)
			assert.Nil(t, err)
			for i, entry := range readVector.Entries {
				assert.Equal(t, parts[i], entry.Data, "path: %s, entry: %+v, content %v", filePath, entry, content)
			}
			readVector.Release()

			// read, random entry with ReadCloserForRead
			parts = randomSplit(content, len(content)/10)
			readVector.Entries = readVector.Entries[:0]
			offset = int64(0)
			readers := make([]io.ReadCloser, len(parts))
			for i, part := range parts {
				readVector.Entries = append(readVector.Entries, IOEntry{
					Offset:            offset,
					Size:              int64(len(part)),
					ReadCloserForRead: &readers[i],
				})
				offset += int64(len(part))
			}
			err = fs.Read(ctx, readVector)
			assert.Nil(t, err)
			wg := new(sync.WaitGroup)
			errCh := make(chan error, 1)
			numDone := int64(0)
			for i, entry := range readVector.Entries {
				wg.Add(1)
				i := i
				entry := entry
				go func() {
					defer wg.Done()
					reader := readers[i]
					data, err := io.ReadAll(reader)
					assert.Nil(t, err)
					reader.Close()
					if !bytes.Equal(parts[i], data) {
						select {
						case errCh <- moerr.NewInternalError(context.Background(),
							"not equal: path: %s, entry: %+v, content %v",
							filePath, entry, content,
						):
						default:
						}
					}
					atomic.AddInt64(&numDone, 1)
				}()
			}
			wg.Wait()
			if int(numDone) != len(parts) {
				t.Fatal()
			}
			select {
			case err := <-errCh:
				t.Fatal(err)
			default:
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
					Policy: policy,
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

		// with / suffix
		entries, err = fs.List(ctx, "qux/quux/")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		// with / prefix
		entries, err = fs.List(ctx, "/qux/quux/")
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		// with fs name
		entries, err = fs.List(ctx, JoinPath(fs.Name(), "qux/quux/"))
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		// with fs name and / prefix and suffix
		entries, err = fs.List(ctx, JoinPath(fs.Name(), "/qux/quux/"))
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 8)
		assert.Equal(t, entries[0].IsDir, false)
		assert.Equal(t, entries[0].Name, "0")
		assert.Equal(t, entries[7].IsDir, false)
		assert.Equal(t, entries[7].Name, "7")

		for _, entry := range entries {
			err := fs.Delete(ctx, path.Join("qux/quux", entry.Name))
			assert.Nil(t, err)
			// delete again
			err = fs.Delete(ctx, path.Join("qux/quux", entry.Name))
			assert.Nil(t, err)
		}
		entries, err = fs.List(ctx, "qux/quux")
		assert.Nil(t, err)
		assert.Equal(t, 0, len(entries))

	})

	t.Run("errors", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()

		err := fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Policy:   policy,
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyVector))

		err = fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
			Policy: policy,
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
			Policy: policy,
		})
		assert.Nil(t, err)
		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Policy:   policy,
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
			Policy: policy,
		})
		assert.True(t, moerr.IsMoErrCode(moerr.ConvertGoError(ctx, err), moerr.ErrUnexpectedEOF))

		err = fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 1,
					Size:   0,
				},
			},
			Policy: policy,
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))

		err = fs.Write(ctx, IOVector{
			FilePath: "bar",
			Entries: []IOEntry{
				{
					Size: 1,
				},
			},
			Policy: policy,
		})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch))

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					ReaderForWrite: iotest.ErrReader(io.ErrNoProgress),
				},
			},
			Policy: policy,
		})
		// fs leaking io error, but I don't know what this test really tests.
		// assert.True(t, err == io.ErrNoProgress)
		// assert.True(t, moerr.IsMoErrCode(moerr.ConvertGoError(err), moerr.ErrInternal))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))

		vector := IOVector{
			FilePath: JoinPath(fsName, "a#b#c"),
			Entries: []IOEntry{
				{Size: 1, Data: []byte("a")},
			},
			Policy: policy,
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

	t.Run("cache data", func(t *testing.T) {
		fs := newFS(fsName)
		ctx := context.Background()
		var counterSet perfcounter.CounterSet
		ctx = perfcounter.WithCounterSet(ctx, &counterSet)

		m := api.Int64Map{
			M: map[int64]int64{
				42: 42,
			},
		}
		data, err := m.Marshal()
		assert.Nil(t, err)
		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					Data: data,
				},
			},
			Policy: policy,
		})
		assert.Nil(t, err)

		// read with ToCacheData
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					ToCacheData: func(r io.Reader, data []byte, allocator CacheDataAllocator) (memorycache.CacheData, error) {
						bs, err := io.ReadAll(r)
						assert.Nil(t, err)
						if len(data) > 0 {
							assert.Equal(t, bs, data)
						}
						cacheData := allocator.Alloc(len(bs))
						copy(cacheData.Bytes(), bs)
						return cacheData, nil
					},
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)

		cachedData := vec.Entries[0].CachedData
		assert.NotNil(t, cachedData)
		assert.Equal(t, data, cachedData.Bytes())

		err = m.Unmarshal(vec.Entries[0].CachedData.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(m.M))
		assert.Equal(t, int64(42), m.M[42])

		vec.Release()

		// ReadCache
		vec = &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
				},
			},
			Policy: policy,
		}
		err = fs.ReadCache(ctx, vec)
		assert.Nil(t, err)
		if vec.Entries[0].CachedData != nil {
			assert.Equal(t, data, vec.Entries[0].CachedData.Bytes())
		}
		vec.Release()
		fs.Close()
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
			Policy: policy,
		})
		assert.Nil(t, err)

		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					done: true,
				},
				{
					Size: int64(len(data)),
				},
			},
			Policy: policy,
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
			FilePath: JoinPath(fs.Name(), "foo"),
			Entries: []IOEntry{
				{
					Size: 4,
					Data: []byte("1234"),
				},
			},
			Policy: policy,
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
			Policy: policy,
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// read with lower named path
		vec = IOVector{
			FilePath: JoinPath(strings.ToLower(fs.Name()), "foo"),
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// read with upper named path
		vec = IOVector{
			FilePath: JoinPath(strings.ToUpper(fs.Name()), "foo"),
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)
		assert.Equal(t, []byte("1234"), vec.Entries[0].Data)

		// bad name
		vec.FilePath = JoinPath(fs.Name()+"abc", "foo")
		err = fs.Read(ctx, &vec)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
		err = fs.Write(ctx, vec)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
		err = fs.Delete(ctx, vec.FilePath)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoService) || moerr.IsMoErrCode(err, moerr.ErrWrongService))
	})

	t.Run("issue6110", func(t *testing.T) {
		ctx := context.Background()
		fs := newFS(fsName)
		err := fs.Write(ctx, IOVector{
			FilePath: "path/to/file/foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   4,
					Data:   []byte("1234"),
				},
			},
			Policy: policy,
		})
		assert.Nil(t, err)
		entries, err := fs.List(ctx, JoinPath(fs.Name(), "/path"))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, "to", entries[0].Name)
	})

	t.Run("streaming write", func(t *testing.T) {
		ctx := context.Background()
		fs := newFS(fsName)

		reader, writer := io.Pipe()
		n := 65536
		defer reader.Close()
		defer writer.Close()

		go func() {
			csvWriter := csv.NewWriter(writer)
			for i := 0; i < n; i++ {
				err := csvWriter.Write([]string{"foo", strconv.Itoa(i)})
				if err != nil {
					writer.CloseWithError(err)
					return
				}
			}
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				writer.CloseWithError(err)
				return
			}
			writer.Close()
		}()

		filePath := "foo"
		vec := IOVector{
			FilePath: filePath,
			Entries: []IOEntry{
				{
					ReaderForWrite: reader,
					Size:           -1, // must set to -1
				},
			},
			Policy: policy,
		}

		// write
		err := fs.Write(ctx, vec)
		assert.Nil(t, err)

		// read
		vec = IOVector{
			FilePath: filePath,
			Entries: []IOEntry{
				{
					Size: -1,
				},
			},
			Policy: policy,
		}
		err = fs.Read(ctx, &vec)
		assert.Nil(t, err)

		// validate
		buf := new(bytes.Buffer)
		csvWriter := csv.NewWriter(buf)
		for i := 0; i < n; i++ {
			err := csvWriter.Write([]string{"foo", strconv.Itoa(i)})
			assert.Nil(t, err)
		}
		csvWriter.Flush()
		err = csvWriter.Error()
		assert.Nil(t, err)
		assert.Equal(t, buf.Bytes(), vec.Entries[0].Data)

		// write to existed
		vec = IOVector{
			FilePath: filePath,
			Entries: []IOEntry{
				{
					ReaderForWrite: bytes.NewReader([]byte("abc")),
					Size:           -1,
				},
			},
			Policy: policy,
		}
		err = fs.Write(ctx, vec)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))

		// cancel write
		reader, writer = io.Pipe()
		defer reader.Close()
		defer writer.Close()
		vec = IOVector{
			FilePath: "bar",
			Entries: []IOEntry{
				{
					ReaderForWrite: reader,
					Size:           -1,
				},
			},
			Policy: policy,
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		errCh := make(chan error)
		go func() {
			err := fs.Write(ctx, vec)
			errCh <- err
		}()
		select {
		case err := <-errCh:
			assert.True(t, errors.Is(err, context.Canceled))
		case <-time.After(time.Second * 10):
			t.Fatal("should cancel")
		}

	})

	t.Run("context cancel", func(t *testing.T) {
		fs := newFS(fsName)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fs.Write(ctx, IOVector{
			Policy: policy,
		})
		assert.ErrorIs(t, err, context.Canceled)

		err = fs.Read(ctx, &IOVector{
			Policy: policy,
		})
		assert.ErrorIs(t, err, context.Canceled)

		_, err = fs.List(ctx, "")
		assert.ErrorIs(t, err, context.Canceled)

		err = fs.Delete(ctx, "")
		assert.ErrorIs(t, err, context.Canceled)
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
