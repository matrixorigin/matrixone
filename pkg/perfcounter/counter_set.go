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
	S3 struct {
		List        stats.Counter
		Head        stats.Counter
		Put         stats.Counter
		Get         stats.Counter
		Delete      stats.Counter
		DeleteMulti stats.Counter
	}

	Cache struct {
		Read   stats.Counter
		Hit    stats.Counter
		Memory struct {
			Read stats.Counter
			Hit  stats.Counter
		}
		Disk struct {
			Read           stats.Counter
			Hit            stats.Counter
			GetFileContent stats.Counter
			SetFileContent stats.Counter
			OpenFile       stats.Counter
			StatFile       stats.Counter
		}
	}

	FileWithChecksum struct {
		Read            stats.Counter
		Write           stats.Counter
		UnderlyingRead  stats.Counter
		UnderlyingWrite stats.Counter
	}

	FileServices map[string]*CounterSet

	DistTAE struct {
		Logtail struct {
			Entries               stats.Counter
			InsertEntries         stats.Counter
			MetadataInsertEntries stats.Counter
			DeleteEntries         stats.Counter
			MetadataDeleteEntries stats.Counter

			InsertRows   stats.Counter
			ActiveRows   stats.Counter
			InsertBlocks stats.Counter
		}
	}
}
