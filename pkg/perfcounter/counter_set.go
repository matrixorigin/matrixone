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
	FileService          FileServiceCounterSet
	ProtocolOutputWaitNS atomic.Int64 // time blocked in physical client socket writes
}

// ColumnConstructionCounterSet counts column-cache constructors, not scan rows
// or cache misses. Bytes includes successful decoded column bytes only.
type ColumnConstructionCounterSet struct {
	Calls  atomic.Int64
	Bytes  atomic.Int64
	Errors atomic.Int64
}

type FileServiceCounterSet struct {
	// ReaderCacheWriteBypassBlocks counts reads crossing the reader's own
	// block threshold, independently of caller-supplied FileService policy.
	ReaderCacheWriteBypassBlocks atomic.Int64
	ColumnConstruction           struct {
		Uncompressed ColumnConstructionCounterSet
		LZ4          ColumnConstructionCounterSet
		Chunked      ColumnConstructionCounterSet
		Other        ColumnConstructionCounterSet
	}
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
			Read                 atomic.Int64 // CacheMemoryRead
			Hit                  atomic.Int64 // CacheMemoryHit
			UpdatePolicySkips    atomic.Int64 // IOVectors skipped before entry inspection
			UpdateEntries        atomic.Int64 // non-hit entries with decoded cache data
			UpdateAdmitted       atomic.Int64 // newly inserted entries
			UpdateAlreadyPresent atomic.Int64 // successful Set with an existing key
			UpdatePressureSkips  atomic.Int64 // rejected before/after rehome or by Set
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
	// S3WriteSize: actual bytes accepted by object storage.
	S3WriteSize atomic.Int64
	// DiskReadSize: actual bytes read from disk cache (excluding rowid tombstone)
	DiskReadSize atomic.Int64
}

func (c *CounterSet) Reset() {
	c.FileService.ReaderCacheWriteBypassBlocks.Store(0)
	for _, codec := range []*ColumnConstructionCounterSet{
		&c.FileService.ColumnConstruction.Uncompressed,
		&c.FileService.ColumnConstruction.LZ4,
		&c.FileService.ColumnConstruction.Chunked,
		&c.FileService.ColumnConstruction.Other,
	} {
		codec.Calls.Store(0)
		codec.Bytes.Store(0)
		codec.Errors.Store(0)
	}
	c.FileService.Cache.Memory.UpdatePolicySkips.Store(0)
	c.FileService.Cache.Memory.UpdateEntries.Store(0)
	c.FileService.Cache.Memory.UpdateAdmitted.Store(0)
	c.FileService.Cache.Memory.UpdateAlreadyPresent.Store(0)
	c.FileService.Cache.Memory.UpdatePressureSkips.Store(0)
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
	c.FileService.S3WriteSize.Store(0)
	c.FileService.DiskReadSize.Store(0)
	c.ProtocolOutputWaitNS.Store(0)
}
