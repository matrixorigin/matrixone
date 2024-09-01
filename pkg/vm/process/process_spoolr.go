// Copyright 2024 Matrix Origin
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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

type PipelineActionType uint8

const (
	GetFromIndex PipelineActionType = iota
	GetDirectly
)

var _ = GetDirectly

type PipelineSignal struct {
	typ PipelineActionType

	// for case: GetFromIndex
	index  int
	source pSpool.PipelineCommunication

	// for case: GetDirectly
	directly *batch.Batch
}

// Action will get the input batch from one place according to which type this signal is.
//
// the result batch of this function is an READ-ONLY one.
func (signal *PipelineSignal) Action() *batch.Batch {
	if signal.typ == GetFromIndex {
		return signal.source.ReceiveBatch(signal.index)
	}
	return signal.directly
}
