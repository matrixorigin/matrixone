// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	constMaxMemCap = 12 * common.Const1GBytes // max original memory for an object
)

func resourceAvailable(
	estMem int64,
	rsc rscthrottler.RSCThrottler,
) bool {

	mem := rsc.Available()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	return estMem <= 2*mem/3
}

func IterStats(objs []*objectio.ObjectStats) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, obj := range objs {
			if !yield(obj) {
				return
			}
		}
	}
}
