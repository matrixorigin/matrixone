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
	"sync/atomic"
)

type CounterSet struct {
	FileService FileServiceCounterSet
}

type FileServiceCounterSet struct {
	S3 struct {
		List        atomic.Int64 // listObjects:List all Objects  [Put type request]
		Head        atomic.Int64 // statObject:View all meta information contained in the object [Get type request]
		Put         atomic.Int64 // putObject:Upload an Object   [Put type request]
		Get         atomic.Int64 // getObject:Download an Object   [Get type request]
		Delete      atomic.Int64 // deleteObject:Delete a single Object [Put type request]
		DeleteMulti atomic.Int64 // deleteObjects:Delete multiple Objects [Put type request]
	}

	Cache struct {
		Read   atomic.Int64 // CacheRead
		Hit    atomic.Int64 // CacheHit
		Memory struct {
			Read atomic.Int64 // CacheMemoryRead
			Hit  atomic.Int64 // CacheMemoryHit
		}
		Disk struct {
			Read atomic.Int64 // CacheDiskRead
			Hit  atomic.Int64 // CacheDiskHit
		}
		Remote struct {
			Read atomic.Int64 // CacheRemoteRead
			Hit  atomic.Int64 // CacheRemoteHit
		}
	}

	// ReadSize: actual bytes read from storage layer (excluding rowid tombstone)
	ReadSize atomic.Int64
	// S3ReadSize: actual bytes read from S3 (excluding rowid tombstone)
	S3ReadSize atomic.Int64
	// DiskReadSize: actual bytes read from disk cache (excluding rowid tombstone)
	DiskReadSize atomic.Int64
}

func (c *CounterSet) Reset() {
	// FileService.S3
	c.FileService.S3.List.Store(0)
	c.FileService.S3.Head.Store(0)
	c.FileService.S3.Put.Store(0)
	c.FileService.S3.Get.Store(0)
	c.FileService.S3.Delete.Store(0)
	c.FileService.S3.DeleteMulti.Store(0)

	// FileService.Cache
	c.FileService.Cache.Read.Store(0)
	c.FileService.Cache.Hit.Store(0)
	c.FileService.Cache.Memory.Read.Store(0)
	c.FileService.Cache.Memory.Hit.Store(0)
	c.FileService.Cache.Disk.Read.Store(0)
	c.FileService.Cache.Disk.Hit.Store(0)
	c.FileService.Cache.Remote.Read.Store(0)
	c.FileService.Cache.Remote.Hit.Store(0)

	// FileService top-level
	c.FileService.ReadSize.Store(0)
	c.FileService.S3ReadSize.Store(0)
	c.FileService.DiskReadSize.Store(0)
}
