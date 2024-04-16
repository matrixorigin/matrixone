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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type opSerial struct {
	ps []*types.Packer
}

func newOpSerial() *opSerial {
	return &opSerial{
		ps: make([]*types.Packer, 0),
	}
}

func (op *opSerial) tryExpand(length int, mp *mpool.MPool) {
	if len(op.ps) < length {
		// close the current packer array
		for _, p := range op.ps {
			p.FreeMem()
		}

		// create a new packer array with the new length
		op.ps = types.NewPackerArray(length, mp)
	}

	for _, p := range op.ps {
		p.Reset()
	}
}

func (op *opSerial) Close() error {
	if op.ps == nil {
		return nil
	}

	for _, p := range op.ps {
		p.FreeMem()
	}
	return nil
}
