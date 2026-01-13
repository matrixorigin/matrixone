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

package logtailreplay

import "github.com/matrixorigin/matrixone/pkg/container/types"

func (p *PartitionState) GetFlushTS() types.TS {
	if p.rows.Len() == 0 {
		return types.MaxTs()
	}

	dataObjectIter := p.dataObjectTSIndex.Iter()
	defer dataObjectIter.Release()

	var dataFlushTS types.TS
	for dataObjectIter.Next() {
		entry := dataObjectIter.Item()
		if !entry.IsAppendable {
			continue
		}
		if !entry.IsDelete {
			continue
		}
		dataFlushTS = entry.Time
		break
	}

	var tombstoneFlushTS types.TS

	tombstoneObjectIter := p.tombstoneObjectDTSIndex.Iter()
	defer tombstoneObjectIter.Release()
	for tombstoneObjectIter.Next() {
		entry := tombstoneObjectIter.Item()
		if !entry.GetAppendable() {
			continue
		}
		if entry.DeleteTime.IsEmpty() {
			continue
		}
		tombstoneFlushTS = entry.DeleteTime
		break
	}

	flushedTS := dataFlushTS
	if tombstoneFlushTS.LT(&dataFlushTS) {
		flushedTS = tombstoneFlushTS
	}

	return flushedTS
}
