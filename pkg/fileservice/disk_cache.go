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
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

type DiskCache struct {
	capacity        int64
	evictInterval   time.Duration
	evictTarget     float64
	path            string
	fileExists      sync.Map
	perfCounterSets []*perfcounter.CounterSet

	settingContent struct {
		sync.Mutex
		m map[string]chan struct{}
	}

	evictState struct {
		sync.Mutex
		timer        *time.Timer
		newlyWritten int64
	}
}

func NewDiskCache(
	path string,
	capacity int64,
	evictInterval time.Duration,
	evictTarget float64,
	perfCounterSets []*perfcounter.CounterSet,
) (*DiskCache, error) {

	if evictInterval == 0 {
		panic("zero evict interval")
	}
	if evictTarget == 0 {
		panic("zero evict target")
	}
	if evictTarget > 1 {
		panic("evict target greater than 1")
	}

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	ret := &DiskCache{
		capacity:        capacity,
		evictInterval:   evictInterval,
		evictTarget:     evictTarget,
		path:            path,
		perfCounterSets: perfCounterSets,
	}
	ret.settingContent.m = make(map[string]chan struct{})
	ret.triggerEvict(context.TODO(), 0)
	return ret, nil
}

var _ IOVectorCache = new(DiskCache)

func (d *DiskCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {
	if vector.NoCache {
		return nil
	}

	var numHit, numRead, numOpen, numError int64
	defer func() {
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Disk.Read.Add(numRead)
			c.FileService.Cache.Disk.Hit.Add(numHit)
			c.FileService.Cache.Disk.Error.Add(numError)
			c.FileService.Cache.Disk.OpenFile.Add(numOpen)
		}, d.perfCounterSets...)
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	contentOK := false
	contentPath := d.fullDataFilePath(path.File)
	contentFile, err := os.Open(contentPath)
	if err == nil {
		numOpen++
		contentOK = true
		defer contentFile.Close()
	} else {
		err = nil // ignore
	}

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}

		numRead++

		var file *os.File
		if contentOK {
			// use content file
			_, err = contentFile.Seek(entry.Offset, io.SeekStart)
			if err == nil {
				file = contentFile
			}
		} else {
			// open entry file
			entryFile, err := os.Open(d.entryDataFilePath(path.File, entry))
			if err == nil {
				file = entryFile
				defer entryFile.Close()
				numOpen++
			}
		}
		if file == nil {
			// no file available
			continue
		}

		if err := entry.ReadFromOSFile(file); err != nil {
			// ignore error
			numError++
			continue
		}

		vector.Entries[i] = entry
		numHit++
		d.cacheHit()
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
	if vector.NoCache {
		return nil
	}

	var numOpen, numStat, numError, numWrite int64
	defer func() {
		perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
			set.FileService.Cache.Disk.OpenFile.Add(numOpen)
			set.FileService.Cache.Disk.StatFile.Add(numStat)
			set.FileService.Cache.Disk.WriteFile.Add(numWrite)
			set.FileService.Cache.Disk.Error.Add(numError)
		})
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	contentPath := d.fullDataFilePath(path.File)
	if _, ok := d.fileExists.Load(contentPath); ok {
		// full data ok, return
		return nil
	} else {
		// full data file exists, return
		_, err := os.Stat(contentPath)
		if err == nil {
			numStat++
			d.fileExists.Store(contentPath, true)
			return nil
		}
	}

	for _, entry := range vector.Entries {
		if len(entry.Data) == 0 {
			// no data
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}

		entryFilePath := d.entryDataFilePath(path.File, entry)
		if _, ok := d.fileExists.Load(entryFilePath); ok {
			// already exists
			continue
		}
		_, err := os.Stat(entryFilePath)
		if err == nil {
			// file exists
			d.fileExists.Store(entryFilePath, true)
			numStat++
			continue
		}

		// write data
		dir := filepath.Dir(entryFilePath)
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			numError++
			continue // ignore error
		}
		f, err := os.CreateTemp(dir, "*")
		if err != nil {
			numError++
			continue // ignore error
		}
		numOpen++
		n, err := f.Write(entry.Data)
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			numError++
			continue // ignore error
		}
		if err := f.Close(); err != nil {
			numError++
			continue // ignore error
		}
		if err := os.Rename(f.Name(), entryFilePath); err != nil {
			numError++
			continue // ignore error
		}
		numWrite++

		d.triggerEvict(ctx, int64(n))

		d.fileExists.Store(entryFilePath, true)
	}

	return nil
}

func (d *DiskCache) Flush() {
	//TODO
}

func (d *DiskCache) triggerEvict(ctx context.Context, bytesWritten int64) {
	d.evictState.Lock()
	defer d.evictState.Unlock()

	d.evictState.newlyWritten += bytesWritten

	if d.evictState.timer != nil {
		// has pending eviction

		newlyWrittenThreshold := int64(math.Ceil(float64(d.capacity) * (1 - d.evictTarget)))
		if newlyWrittenThreshold > 0 && d.evictState.newlyWritten >= newlyWrittenThreshold {
			if d.evictState.timer.Stop() {
				// evict immediately
				logutil.Info("disk cache: newly written bytes may excceeds eviction target, start immediately",
					zap.Any("newly-written", d.evictState.newlyWritten),
					zap.Any("newly-written-threshold", newlyWrittenThreshold),
				)
				// timer stopped, start eviction
				// before the following eviction is end, further calls of triggerEvict will not go this path, since timer.Stop will return false
				perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
					set.FileService.Cache.Disk.EvictImmediately.Add(1)
				}, d.perfCounterSets...)
				go func() {
					defer func() {
						d.evictState.Lock()
						d.evictState.timer = nil
						d.evictState.newlyWritten = 0
						d.evictState.Unlock()
					}()
					d.evict(ctx)
				}()
			}
			return
		}

		return
	}

	perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
		set.FileService.Cache.Disk.EvictPending.Add(1)
	}, d.perfCounterSets...)
	d.evictState.timer = time.AfterFunc(
		d.evictInterval,
		func() {
			defer func() {
				d.evictState.Lock()
				d.evictState.timer = nil
				d.evictState.newlyWritten = 0
				d.evictState.Unlock()
			}()
			d.evict(ctx)
		},
	)
}

func (d *DiskCache) evict(ctx context.Context) {
	paths := make(map[string]int64)
	var sumSize int64

	_ = filepath.WalkDir(d.path, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil //ignore
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), cacheFileSuffix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil // ignore
		}
		size := info.Size()
		if _, ok := paths[path]; !ok {
			paths[path] = size
			sumSize += size
		}
		return nil
	})

	var numDeleted int64
	var bytesDeleted int64
	defer func() {
		if numDeleted > 0 {
			perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
				set.FileService.Cache.Disk.Evict.Add(numDeleted)
			}, d.perfCounterSets...)
			logutil.Info("disk cache: eviction finished",
				zap.Any("files", numDeleted),
				zap.Any("bytes", bytesDeleted),
			)
		}
	}()
	target := int64(float64(d.capacity) * d.evictTarget)
	for path, size := range paths {
		if sumSize <= target {
			break
		}
		if err := os.Remove(path); err != nil {
			continue // ignore
		}
		d.fileExists.Delete(path)
		sumSize -= size
		numDeleted++
		bytesDeleted += size
	}
}

var _ FileContentCache = new(DiskCache)

func (d *DiskCache) GetFileContent(ctx context.Context, filePath string, offset int64) (r io.ReadCloser, err error) {
	contentPath := d.fullDataFilePath(filePath)
	f, err := os.Open(contentPath)
	if err != nil {
		return nil, err
	}
	perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
		set.FileService.Cache.Disk.OpenFile.Add(1)
	})
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
		set.FileService.Cache.Disk.GetFileContent.Add(1)
	})
	return f, nil
}

func (d *DiskCache) SetFileContent(
	ctx context.Context,
	filePath string,
	readFunc func(context.Context, *IOVector) error,
) (err error) {

	contentPath := d.fullDataFilePath(filePath)

	_, err = os.Stat(contentPath)
	if err == nil {
		// file exists
		perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
			set.FileService.Cache.Disk.StatFile.Add(1)
		})
		return nil
	}

	// concurrency control
	d.settingContent.Lock()
	waitChan, wait := d.settingContent.m[contentPath]
	if wait {
		d.settingContent.Unlock()
		<-waitChan
		return nil
	}
	waitChan = make(chan struct{})
	d.settingContent.m[contentPath] = waitChan
	d.settingContent.Unlock()
	defer func() {
		close(waitChan)
	}()

	w, done, closeW, err := d.newFileContentWriter(filePath)
	if err != nil {
		return err
	}
	defer closeW()
	vec := IOVector{
		FilePath: filePath,
		Entries: []IOEntry{
			{
				Offset:        0,
				Size:          -1,
				WriterForRead: w,
			},
		},
	}
	if err := readFunc(ctx, &vec); err != nil {
		return err
	}
	if err := done(ctx); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
		set.FileService.Cache.Disk.SetFileContent.Add(1)
	})

	return nil
}

func (d *DiskCache) newFileContentWriter(filePath string) (w io.Writer, done func(context.Context) error, closeFunc func() error, err error) {
	contentPath := d.fullDataFilePath(filePath)
	dir := filepath.Dir(contentPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, nil, nil, err
	}
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		return nil, nil, nil, err
	}
	var doneOnce sync.Once
	return f, func(ctx context.Context) (err error) {
		doneOnce.Do(func() {
			var info fs.FileInfo
			info, err = f.Stat()
			if err != nil {
				return
			}
			name := f.Name()
			if err = f.Close(); err != nil {
				return
			}
			if err = os.Rename(name, contentPath); err != nil {
				return
			}
			d.triggerEvict(ctx, info.Size())
		})
		return
	}, f.Close, nil
}

const cacheFileSuffix = ".mofscache"

func (d *DiskCache) entryDataFilePath(path string, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	return filepath.Join(
		d.path,
		toOSPath(path),
		fmt.Sprintf("%d-%d%s", entry.Offset, entry.Size, cacheFileSuffix),
	)
}

func (d *DiskCache) fullDataFilePath(path string) string {
	return filepath.Join(
		d.path,
		toOSPath(path),
		"full-data"+cacheFileSuffix,
	)
}
