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
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
)

func setCachedData[P interface {
	*A
	CacheDataAllocator
}, A any](i *IOEntry, allocator P) error {
	if i.ToCacheData == nil {
		return nil
	}
	if len(i.Data) == 0 {
		return nil
	}
	var bs memorycache.CacheData
	var err error
	if allocator == nil {
		bs, err = i.ToCacheData(bytes.NewReader(i.Data), i.Data, DefaultCacheDataAllocator)
	} else {
		bs, err = i.ToCacheData(bytes.NewReader(i.Data), i.Data, allocator)
	}
	if err != nil {
		return err
	}
	i.CachedData = bs
	return nil
}

func readFromOSFile[P interface {
	*A
	CacheDataAllocator
}, A any](i *IOEntry, file *os.File, allocator P) error {
	r := io.LimitReader(file, i.Size)

	if cap(i.Data) < int(i.Size) {
		i.Data = make([]byte, i.Size)
	} else {
		i.Data = i.Data[:i.Size]
	}

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
	if err := setCachedData(i, allocator); err != nil {
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
