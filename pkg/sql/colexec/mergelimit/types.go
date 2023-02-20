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

package mergelimit

import (
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
	// Limit records the limit number of this operator
	Limit uint64
	// ctr stores the attributes needn't do Serialization work
	ctr *container
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}
