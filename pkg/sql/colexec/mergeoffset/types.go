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

package mergeoffset

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	seen uint64

	// aliveMergeReceiver is a count for no-close receiver
	aliveMergeReceiver int
	// receiverListener is a structure to listen all the merge receiver.
	receiverListener []reflect.SelectCase
}

type Argument struct {
	// Offset records the offset number of mergeOffset operator
	Offset uint64
	// ctr contains the attributes needn't do serialization work
	ctr *container
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.ctr != nil {
		arg.ctr.cleanReceiver(proc.Mp())
	}
}

func (ctr *container) cleanReceiver(mp *mpool.MPool) {
	listeners := ctr.receiverListener
	alive := len(listeners)
	for alive != 0 {
		chosen, value, ok := reflect.Select(listeners)
		if !ok {
			listeners = append(listeners[:chosen], listeners[chosen+1:]...)
			alive--
			continue
		}
		pointer := value.UnsafePointer()
		bat := (*batch.Batch)(pointer)
		if bat == nil {
			alive--
			listeners = append(listeners[:chosen], listeners[chosen+1:]...)
			continue
		}
		bat.Clean(mp)
	}
}
