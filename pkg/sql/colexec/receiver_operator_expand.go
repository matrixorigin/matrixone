// Copyright 2023 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func (r *ReceiverOperator) selectFrom1Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom2Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom3Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom4Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom5Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom6Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom7Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom8Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom9Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom10Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom11Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom12Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom13Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	case bat, ok = <-r.chs[12]:
		idx = 13
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom14Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	case bat, ok = <-r.chs[12]:
		idx = 13
	case bat, ok = <-r.chs[13]:
		idx = 14
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom15Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	case bat, ok = <-r.chs[12]:
		idx = 13
	case bat, ok = <-r.chs[13]:
		idx = 14
	case bat, ok = <-r.chs[14]:
		idx = 15
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom16Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	case bat, ok = <-r.chs[12]:
		idx = 13
	case bat, ok = <-r.chs[13]:
		idx = 14
	case bat, ok = <-r.chs[14]:
		idx = 15
	case bat, ok = <-r.chs[15]:
		idx = 16
	}
	return idx, bat, ok
}

func (r *ReceiverOperator) selectFrom17Reg() (int, *batch.Batch, bool) {
	idx := 0
	ok := true
	var bat *batch.Batch
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case bat, ok = <-r.chs[0]:
		idx = 1
	case bat, ok = <-r.chs[1]:
		idx = 2
	case bat, ok = <-r.chs[2]:
		idx = 3
	case bat, ok = <-r.chs[3]:
		idx = 4
	case bat, ok = <-r.chs[4]:
		idx = 5
	case bat, ok = <-r.chs[5]:
		idx = 6
	case bat, ok = <-r.chs[6]:
		idx = 7
	case bat, ok = <-r.chs[7]:
		idx = 8
	case bat, ok = <-r.chs[8]:
		idx = 9
	case bat, ok = <-r.chs[9]:
		idx = 10
	case bat, ok = <-r.chs[10]:
		idx = 11
	case bat, ok = <-r.chs[11]:
		idx = 12
	case bat, ok = <-r.chs[12]:
		idx = 13
	case bat, ok = <-r.chs[13]:
		idx = 14
	case bat, ok = <-r.chs[14]:
		idx = 15
	case bat, ok = <-r.chs[15]:
		idx = 16
	case bat, ok = <-r.chs[16]:
		idx = 17
	}
	return idx, bat, ok
}
