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
	"io"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func (i *IOEntry) setCachedData(ctx context.Context) error {
	LogEvent(ctx, str_set_cache_data_begin)
	t0 := time.Now()
	defer func() {
		LogEvent(ctx, str_set_cache_data_end)
		metric.FSReadDurationSetCachedData.Observe(time.Since(t0).Seconds())
	}()
	if i.ToCacheData == nil {
		return nil
	}
	if len(i.Data) == 0 {
		return nil
	}
	if i.allocator == nil {
		i.allocator = GetDefaultCacheDataAllocator()
	}
	LogEvent(ctx, str_to_cache_data_begin)
	cacheData, err := i.ToCacheData(bytes.NewReader(i.Data), i.Data, i.allocator)
	LogEvent(ctx, str_to_cache_data_end)
	if err != nil {
		return err
	}
	if cacheData == nil {
		panic("ToCacheData returns nil cache data")
	}
	i.CachedData = cacheData
	return nil
}

func (i *IOEntry) ReadFromOSFile(ctx context.Context, file *os.File) (err error) {
	finally := i.prepareData()
	defer finally(&err)
	r := io.LimitReader(file, i.Size)
	n, err := io.ReadFull(r, i.Data)
	if err != nil {
		return err
	}
	if n != int(i.Size) {
		return io.ErrUnexpectedEOF
	}

	if i.WriterForRead != nil {
		if _, err := i.WriterForRead.Write(i.Data); err != nil {
			return err
		}
	}
	if i.ReadCloserForRead != nil {
		*i.ReadCloserForRead = io.NopCloser(bytes.NewReader(i.Data))
	}
	if err := i.setCachedData(ctx); err != nil {
		return err
	}

	i.done = true

	return nil
}

func CacheOriginalData(r io.Reader, data []byte, allocator CacheDataAllocator) (cacheData memorycache.CacheData, err error) {
	if len(data) == 0 {
		data, err = io.ReadAll(r)
		if err != nil {
			return
		}
	}
	cacheData = allocator.Alloc(len(data))
	copy(cacheData.Bytes(), data)
	return
}

func (i *IOEntry) prepareData() (finally func(err *error)) {
	if cap(i.Data) < int(i.Size) {
		slice, dec, err := getIOAllocator().Allocate(uint64(i.Size), malloc.NoHints)
		if err != nil {
			panic(err)
		}
		metric.FSMallocLiveObjectsIOEntryData.Inc()
		i.Data = slice
		if i.releaseData != nil {
			i.releaseData()
		}
		i.releaseData = func() {
			dec.Deallocate(malloc.NoHints)
			metric.FSMallocLiveObjectsIOEntryData.Dec()
		}
		finally = func(err *error) {
			if err != nil && *err != nil {
				dec.Deallocate(malloc.NoHints)
				metric.FSMallocLiveObjectsIOEntryData.Dec()
			}
		}

	} else {
		i.Data = i.Data[:i.Size]
		finally = noopFinally
	}

	return
}

func noopFinally(*error) {}
