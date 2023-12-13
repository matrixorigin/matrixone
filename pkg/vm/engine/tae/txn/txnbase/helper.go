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

package txnbase

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
)

const (
	IDSize = 8 + types.UuidSize + types.BlockidSize + 4 + 2 + 1
)

func MarshalID(id *common.ID) []byte {
	var err error
	var w bytes.Buffer
	_, err = w.Write(common.EncodeID(id))
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func UnmarshalID(buf []byte) *common.ID {
	var err error
	r := bytes.NewBuffer(buf)
	id := common.ID{}
	_, err = r.Read(common.EncodeID(&id))
	if err != nil {
		panic(err)
	}
	return &id
}

// for debug
type StagesDuration struct {
	mu   sync.Mutex
	name string
	// stage name --> duration (ms)
	stages map[string]int64
	// total stages duration ms
	total int64
	// the threshold to print log (ms)
	logThreshold int64
}

func NewStagesDurationWithLogThreshold(ms int64, name string) *StagesDuration {
	sd := new(StagesDuration)
	sd.logThreshold = ms
	sd.name = name
	sd.stages = make(map[string]int64, 0)
	return sd
}

func (sd *StagesDuration) Log() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if sd.total >= sd.logThreshold {
		logutil.Info(fmt.Sprintf(
			"[stages durations]: %s elapsed %d ms; stages: %v", sd.name, sd.total, sd.stages))
	}
}

func (sd *StagesDuration) Record(elapsed int64, stage string) {
	sd.mu.Lock()
	sd.mu.Unlock()

	sd.total += elapsed
	sd.stages[stage] += elapsed
}

func (sd *StagesDuration) ClearOnlyElapsed() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.total = 0
	for k := range sd.stages {
		sd.stages[k] = 0
	}
}

var onPrepareWALStages = NewStagesDurationWithLogThreshold(1000, "onPrepareWAL")
