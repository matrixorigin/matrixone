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
	S3 struct {
		List        atomic.Int64
		Head        atomic.Int64
		Put         atomic.Int64
		Get         atomic.Int64
		Delete      atomic.Int64
		DeleteMulti atomic.Int64
	}

	Cache struct {
		Read     atomic.Int64
		Hit      atomic.Int64
		MemRead  atomic.Int64
		MemHit   atomic.Int64
		DiskRead atomic.Int64
		DiskHit  atomic.Int64
	}

	FileServices map[string]*CounterSet

	DistTAE struct {
		Logtail struct {
			Entries               atomic.Int64
			InsertEntries         atomic.Int64
			MetadataInsertEntries atomic.Int64
			DeleteEntries         atomic.Int64
			MetadataDeleteEntries atomic.Int64

			InsertRows   atomic.Int64
			ActiveRows   atomic.Int64
			InsertBlocks atomic.Int64
		}
	}
}
