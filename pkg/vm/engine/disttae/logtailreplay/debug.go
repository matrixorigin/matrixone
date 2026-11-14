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

package logtailreplay

import "github.com/matrixorigin/matrixone/pkg/pb/timestamp"

type DebugPartitionStateSummary struct {
	Start              timestamp.Timestamp `json:"start"`
	End                timestamp.Timestamp `json:"end"`
	Rows               int                 `json:"rows"`
	Checkpoints        int                 `json:"checkpoints"`
	DataObjects        int                 `json:"data_objects"`
	TombstoneObjects   int                 `json:"tombstone_objects"`
	Prefetch           bool                `json:"prefetch"`
	NoData             bool                `json:"no_data"`
	LastFlushTimestamp timestamp.Timestamp `json:"last_flush_timestamp"`
}

func (p *PartitionState) DebugSummary() DebugPartitionStateSummary {
	if p == nil {
		return DebugPartitionStateSummary{}
	}
	return DebugPartitionStateSummary{
		Start:              p.start.ToTimestamp(),
		End:                p.end.ToTimestamp(),
		Rows:               p.rows.Len(),
		Checkpoints:        len(p.checkpoints),
		DataObjects:        p.dataObjectsNameIndex.Len(),
		TombstoneObjects:   p.tombstoneObjectsNameIndex.Len(),
		Prefetch:           p.prefetch,
		NoData:             p.noData,
		LastFlushTimestamp: p.lastFlushTimestamp.ToTimestamp(),
	}
}

func (p *Partition) CheckpointConsumed() bool {
	if p == nil {
		return false
	}
	return p.checkpointConsumed.Load()
}
