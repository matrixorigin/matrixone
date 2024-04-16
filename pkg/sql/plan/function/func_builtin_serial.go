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
	"sync"
)

type opSerial struct {
	sync.Mutex
	isInit bool
	ps     []*types.Packer
}

func newOpSerial() *opSerial {
	return &opSerial{
		isInit: false,
		ps:     make([]*types.Packer, 0),
	}
}

func (op *opSerial) init(length int, mp *mpool.MPool) {
	op.Lock()
	defer op.Unlock()

	if op.isInit {
		return
	}
	op.ps = types.NewPackerArray(length, mp)
	op.isInit = true
}

func (op *opSerial) Close() error {
	for _, p := range op.ps {
		p.FreeMem()
	}
	return nil
}
