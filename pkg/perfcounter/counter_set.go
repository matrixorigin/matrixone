// Copyright 2023 Matrix Origin
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

package perfcounter

import (
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

type CounterSet struct {
	FileService FileServiceCounterSet
}

type FileServiceCounterSet struct {
	S3 struct {
		List        stats.Counter // listObjects:List all Objects  [Put type request]
		Head        stats.Counter // statObject:View all meta information contained in the object [Get type request]
		Put         stats.Counter // putObject:Upload an Object   [Put type request]
		Get         stats.Counter // getObject:Download an Object   [Get type request]
		Delete      stats.Counter // deleteObject:Delete a single Object [Put type request]
		DeleteMulti stats.Counter // deleteObjects:Delete multiple Objects [Put type request]
	}

	Cache struct {
		Read   stats.Counter // CacheRead
		Hit    stats.Counter // CacheHit
		Memory struct {
			Read stats.Counter // CacheMemoryRead
			Hit  stats.Counter // CacheMemoryHit
		}
		Disk struct {
			Read stats.Counter // CacheDiskRead
			Hit  stats.Counter // CacheDiskHit
		}
		Remote struct {
			Read stats.Counter // CacheRemoteRead
			Hit  stats.Counter // CacheRemoteHit
		}
	}

	// ReadSize: actual bytes read from storage layer (excluding rowid tombstone)
	ReadSize stats.Counter
	// S3ReadSize: actual bytes read from S3 (excluding rowid tombstone)
	S3ReadSize stats.Counter
	// DiskReadSize: actual bytes read from disk cache (excluding rowid tombstone)
	DiskReadSize stats.Counter
}

func (c *CounterSet) Reset() {
	// FileService.S3
	c.FileService.S3.List.Reset()
	c.FileService.S3.Head.Reset()
	c.FileService.S3.Put.Reset()
	c.FileService.S3.Get.Reset()
	c.FileService.S3.Delete.Reset()
	c.FileService.S3.DeleteMulti.Reset()

	// FileService.Cache
	c.FileService.Cache.Read.Reset()
	c.FileService.Cache.Hit.Reset()
	c.FileService.Cache.Memory.Read.Reset()
	c.FileService.Cache.Memory.Hit.Reset()
	c.FileService.Cache.Disk.Read.Reset()
	c.FileService.Cache.Disk.Hit.Reset()
	c.FileService.Cache.Remote.Read.Reset()
	c.FileService.Cache.Remote.Hit.Reset()

	// FileService top-level
	c.FileService.ReadSize.Reset()
	c.FileService.S3ReadSize.Reset()
	c.FileService.DiskReadSize.Reset()
}
