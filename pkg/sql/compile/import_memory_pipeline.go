// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Every scope contributes a max producer, so len(Producers) bounds the scope
// count. ParallelRun only duplicates each scope by its effective Mcpu. All
// compiler channel constructors use 1, 10, shuffleChannelBufferSize, Mcpu or
// len(scopes); hence this checked product covers the final, duplicated graph.
// The manifest already describes the complete local DAG.
func memoryPipelineChannelCapacity(manifest MemoryManifest) (uint64, bool) {
	parallel := uint64(max(1, manifest.Parallel))
	for _, stage := range manifest.Stages {
		parallel = max(parallel, uint64(stage.Count))
	}
	producers := uint64(len(manifest.Producers))
	if producers != 0 && parallel > uint64(math.MaxInt)/producers {
		return 0, false
	}
	capacity := max(uint64(shuffleChannelBufferSize), producers*parallel)
	_, ok := process.PipelineEdgeMetadataBound(capacity)
	return capacity, ok
}

// Check metadata before admission. Fixed metadata cannot be made safe by
// reducing the byte unit.
func memoryPipelineMetadataFits(manifest MemoryManifest, capacity uint64) bool {
	edge, ok := process.PipelineEdgeMetadataBound(capacity)
	if !ok {
		return false
	}
	var total int64
	for _, stage := range manifest.Stages {
		if stage.Role != MemoryStageProducer || int(stage.Producer) >= len(manifest.Producers) {
			continue
		}
		producer := manifest.Producers[stage.Producer]
		if producer.Receivers == 0 {
			continue
		}
		var spool int64
		if producer.Kind == "spool" {
			var valid bool
			spool, valid = pSpool.MemoryMetadataBound(producer.Receivers)
			if !valid {
				return false
			}
		}
		receivers, valid := memoryProduct(edge, int64(producer.Receivers))
		if !valid {
			return false
		}
		metadata, valid := memorySum(spool, receivers)
		if !valid {
			return false
		}
		metadata, valid = memoryProduct(metadata, int64(stage.Count))
		if !valid {
			return false
		}
		total, valid = memorySum(total, metadata)
		if !valid {
			return false
		}
	}
	return true
}

// Validate embedding/custom channel layouts too. Unknown topology stays legacy
// before admission; a later runtime allocation cannot repair the bound.
func (c *Compile) memoryPipelineChannelsFit(manifest MemoryManifest) bool {
	parallel := max(1, manifest.Parallel)
	for _, stage := range manifest.Stages {
		parallel = max(parallel, int(stage.Count))
	}
	capacity, ok := memoryPipelineChannelCapacity(manifest)
	if !ok || !memoryPipelineMetadataFits(manifest, capacity) {
		return false
	}
	validReg := func(reg *process.WaitRegister) bool {
		return reg != nil && reg.Ch2 != nil && uint64(cap(reg.Ch2)) <= capacity
	}
	var walk func(*Scope) bool
	walk = func(scope *Scope) bool {
		if scope == nil {
			return true
		}
		if max(1, scope.NodeInfo.Mcpu) > parallel {
			return false
		}
		if scope.Proc != nil {
			for _, reg := range scope.Proc.Reg.MergeReceivers {
				if !validReg(reg) {
					return false
				}
			}
		}
		err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch value := op.(type) {
			case *connector.Connector:
				if !validReg(value.Reg) {
					ok = false
				}
			case *dispatch.Dispatch:
				for _, reg := range value.LocalRegs {
					if !validReg(reg) {
						ok = false
					}
				}
			}
			return nil
		})
		if err != nil || !ok {
			return false
		}
		for _, child := range scope.PreScopes {
			if !walk(child) {
				return false
			}
		}
		return true
	}
	for _, scope := range c.scopes {
		if !walk(scope) {
			return false
		}
	}
	return true
}
