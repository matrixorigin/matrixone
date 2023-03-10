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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

//TODO LRU

//TODO full data. If we know the size and it's small, we can get the whole object in one request and save it to a file named full_data

type DiskCache struct {
	capacity   int64
	path       string
	stats      *CacheStats
	fileExists sync.Map
}

func NewDiskCache(path string, capacity int64) (*DiskCache, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	return &DiskCache{
		capacity: capacity,
		path:     path,
		stats:    new(CacheStats),
	}, nil
}

var _ Cache = new(DiskCache)

func (d *DiskCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {
	_, span := trace.Start(ctx, "DiskCache.Read")
	defer span.End()

	numHit := 0
	defer func() {
		if d.stats != nil {
			atomic.AddInt64(&d.stats.NumRead, int64(len(vector.Entries)))
			atomic.AddInt64(&d.stats.NumHit, int64(numHit))
		}
	}()

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}

		updateCounters(ctx, func(c *Counter) {
			atomic.AddInt64(&c.DiskCacheRead, 1)
		})

		linkPath := d.entryLinkPath(vector, entry)
		file, err := os.Open(linkPath)
		if err != nil {
			// ignore error
			continue
		}
		defer file.Close()

		if _, err := file.Seek(entry.Offset, 0); err != nil {
			// ignore error
			continue
		}
		data := make([]byte, entry.Size)
		if _, err := io.ReadFull(file, data); err != nil {
			// ignore error
			continue
		}

		entry.Data = data
		if entry.WriterForRead != nil {
			if _, err := entry.WriterForRead.Write(data); err != nil {
				return err
			}
		}
		if entry.ReadCloserForRead != nil {
			*entry.ReadCloserForRead = io.NopCloser(bytes.NewReader(data))
		}
		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		entry.done = true

		vector.Entries[i] = entry
		numHit++
		d.cacheHit()
		updateCounters(ctx, func(c *Counter) {
			atomic.AddInt64(&c.DiskCacheHit, 1)
		})
	}

	return nil
}

func (d *DiskCache) cacheHit() {
	FSProfileHandler.AddSample()
}

func (d *DiskCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) (
	err error,
) {

	for _, entry := range vector.Entries {
		if len(entry.Data) == 0 {
			// no data
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}

		linkPath := d.entryLinkPath(vector, entry)
		if _, ok := d.fileExists.Load(linkPath); ok {
			// already exists
			continue
		}
		_, err := os.Stat(linkPath)
		if err == nil {
			// file exists
			d.fileExists.Store(linkPath, true)
			continue
		}

		// write data
		dataPath := d.entryDataPath(vector, entry)
		err = os.MkdirAll(filepath.Dir(dataPath), 0755)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		n, err := file.WriteAt(entry.Data, entry.Offset)
		if err != nil {
			return err
		}
		if int64(n) != entry.Size {
			return io.ErrShortWrite
		}

		// link
		if err := os.Link(dataPath, linkPath); err != nil {
			if os.IsExist(err) {
				// ok
			} else {
				return err
			}
		}

		d.fileExists.Store(linkPath, true)
	}

	return nil
}

func (d *DiskCache) Flush() {
	//TODO
}

func (d *DiskCache) CacheStats() *CacheStats {
	return d.stats
}

func (d *DiskCache) entryLinkPath(vector *IOVector, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	return filepath.Join(
		d.path,
		toOSPath(vector.FilePath),
		fmt.Sprintf("%d-%d", entry.Offset, entry.Size),
	)
}

func (d *DiskCache) entryDataPath(vector *IOVector, entry IOEntry) string {
	return filepath.Join(
		d.path,
		toOSPath(vector.FilePath),
		"data",
	)
}
