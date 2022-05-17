// Copyright 2021 Matrix Origin
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

package catalog

type Replayer struct {
	dataFactory DataFactory
	catalog     *Catalog
	ts          uint64
}

func NewReplayer(dataFactory DataFactory, catalog *Catalog) *Replayer {
	return &Replayer{
		dataFactory: dataFactory,
		catalog:     catalog,
		ts:          1,
	}
}

func (replayer *Replayer) ReplayerHandle(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
	if typ != ETCatalogCheckpoint {
		return
	}
	e := NewEmptyCheckpointEntry()
	if err = e.Unmarshal(payload); err != nil {
		return
	}
	checkpoint := new(Checkpoint)
	checkpoint.CommitId = commitId
	checkpoint.MaxTS = e.MaxTS
	checkpoint.LSN = e.MaxIndex.LSN
	ts := uint64(0)
	for _, cmd := range e.Entries {
		if ts, err = replayer.catalog.ReplayCmd(cmd, replayer.dataFactory); err != nil {
			return
		}
		if replayer.ts < ts {
			replayer.ts = ts
		}
	}
	if len(replayer.catalog.checkpoints) == 0 {
		replayer.catalog.checkpoints = append(replayer.catalog.checkpoints, checkpoint)
	} else {
		replayer.catalog.checkpoints[0] = checkpoint
	}
	return

}
