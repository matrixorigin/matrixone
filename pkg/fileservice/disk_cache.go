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
	"math/rand"
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
	path            string
	fileExists      sync.Map
	perfCounterSets []*perfcounter.CounterSet

	settingContent struct {
		sync.Mutex
		m map[string]chan struct{}
	}

	evictState struct {
		sync.Mutex
		pending bool
	}
}

func NewDiskCache(
	path string,
	capacity int64,
	perfCounterSets []*perfcounter.CounterSet,
) (*DiskCache, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	ret := &DiskCache{
		capacity:        capacity,
		path:            path,
		perfCounterSets: perfCounterSets,
	}
	ret.settingContent.m = make(map[string]chan struct{})
	ret.triggerEvict(context.TODO())
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

	var numHit, numRead, numOpen int64
	defer func() {
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Disk.Read.Add(numRead)
			c.FileService.Cache.Disk.Hit.Add(numHit)
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
			file = contentFile
		} else {
			// open link file
			linkPath := d.entryLinkPath(path.File, entry)
			linkFile, err := os.Open(linkPath)
			if err == nil {
				file = linkFile
				defer linkFile.Close()
				numOpen++
			}
		}
		if file == nil {
			// no file available
			continue
		}

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

	var numOpen, numStat, numError int64
	defer func() {
		perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
			set.FileService.Cache.Disk.OpenFile.Add(numOpen)
			set.FileService.Cache.Disk.StatFile.Add(numStat)
			set.FileService.Cache.Disk.Error.Add(numError)
		})
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	contentOK := false
	contentPath := d.fullDataFilePath(path.File)
	if _, ok := d.fileExists.Load(contentPath); ok {
		contentOK = true
	} else {
		_, err := os.Stat(contentPath)
		if err == nil {
			numStat++
			contentOK = true
			d.fileExists.Store(contentPath, true)
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

		linkPath := d.entryLinkPath(path.File, entry)
		if _, ok := d.fileExists.Load(linkPath); ok {
			// already exists
			continue
		}
		_, err := os.Stat(linkPath)
		if err == nil {
			// file exists
			d.fileExists.Store(linkPath, true)
			numStat++
			continue
		}

		var linkTarget string

		if contentOK {
			linkTarget = contentPath

		} else {
			// write data
			dataPath := d.dataFilePath(path.File)
			err = os.MkdirAll(filepath.Dir(dataPath), 0755)
			if err != nil {
				numError++
				continue // ignore error
			}
			file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				numError++
				continue // ignore error
			}
			numOpen++
			n, err := file.WriteAt(entry.Data, entry.Offset)
			if err != nil {
				numError++
				continue // ignore error
			}
			if err := file.Close(); err != nil {
				numError++
				continue // ignore error
			}
			if int64(n) != entry.Size {
				numError++
				continue // ignore error
			}
			d.triggerEvict(ctx)
			linkTarget = dataPath
		}

		// link
		if err := os.Symlink(filepath.Base(linkTarget), linkPath); err != nil {
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

func (d *DiskCache) triggerEvict(ctx context.Context) {
	d.evictState.Lock()
	defer d.evictState.Unlock()
	if d.evictState.pending {
		return
	}
	d.evictState.pending = true
	time.AfterFunc(time.Minute*7, func() {
		defer func() {
			d.evictState.Lock()
			d.evictState.pending = false
			d.evictState.Unlock()
		}()
		d.evict(ctx)
	})
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
		if !strings.HasSuffix(entry.Name(), ".mofscache") {
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
			logutil.Info("disk info eviction",
				zap.Any("files", numDeleted),
				zap.Any("bytes", bytesDeleted),
			)
		}
	}()
	for path, size := range paths {
		if sumSize <= d.capacity {
			break
		}
		if err := os.Remove(path); err != nil {
			continue // ignore
		}
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
	tmpPath := contentPath + fmt.Sprintf(".%d.tmp", rand.Int63())
	err = os.MkdirAll(filepath.Dir(tmpPath), 0755)
	if err != nil {
		return nil, nil, nil, err
	}
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, nil, nil, err
	}
	var doneOnce sync.Once
	return f, func(ctx context.Context) (err error) {
		doneOnce.Do(func() {
			if err = f.Close(); err != nil {
				return
			}
			if err = os.Rename(tmpPath, contentPath); err != nil {
				return
			}
			d.triggerEvict(ctx)
		})
		return
	}, f.Close, nil
}

func (d *DiskCache) entryLinkPath(path string, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	return filepath.Join(
		d.path,
		toOSPath(path),
		fmt.Sprintf("%d-%d", entry.Offset, entry.Size),
	)
}

func (d *DiskCache) dataFilePath(path string) string {
	return filepath.Join(
		d.path,
		toOSPath(path),
		"data.mofscache",
	)
}

func (d *DiskCache) fullDataFilePath(path string) string {
	return filepath.Join(
		d.path,
		toOSPath(path),
		"full-data.mofscache",
	)
}
