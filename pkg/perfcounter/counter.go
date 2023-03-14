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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

type Counter struct {
	S3 struct {
		List        StatsCounter
		Head        StatsCounter
		Put         StatsCounter
		Get         StatsCounter
		Delete      StatsCounter
		DeleteMulti StatsCounter
	}

	Cache struct {
		Read     StatsCounter
		Hit      StatsCounter
		MemRead  StatsCounter
		MemHit   StatsCounter
		DiskRead StatsCounter
		DiskHit  StatsCounter
	}

	DistTAE struct {
		MPool mpool.MPoolStats

		Logtail struct {
			Entries               StatsCounter
			InsertEntries         StatsCounter
			MetadataInsertEntries StatsCounter
			DeleteEntries         StatsCounter
			MetadataDeleteEntries StatsCounter

			InsertRows   StatsCounter
			ActiveRows   StatsCounter
			InsertBlocks StatsCounter
		}
	}
}
