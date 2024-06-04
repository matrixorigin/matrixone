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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (r *ReceiverOperator) selectFrom1Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom2Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom3Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom4Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom5Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom6Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom7Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom8Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom9Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom10Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom11Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom12Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom13Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom14Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom15Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom16Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	case msg, ok = <-r.chs[15]:
		idx = 16
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom32Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	case msg, ok = <-r.chs[15]:
		idx = 16
	case msg, ok = <-r.chs[16]:
		idx = 17
	case msg, ok = <-r.chs[17]:
		idx = 18
	case msg, ok = <-r.chs[18]:
		idx = 19
	case msg, ok = <-r.chs[19]:
		idx = 20
	case msg, ok = <-r.chs[20]:
		idx = 21
	case msg, ok = <-r.chs[21]:
		idx = 22
	case msg, ok = <-r.chs[22]:
		idx = 23
	case msg, ok = <-r.chs[23]:
		idx = 24
	case msg, ok = <-r.chs[24]:
		idx = 25
	case msg, ok = <-r.chs[25]:
		idx = 26
	case msg, ok = <-r.chs[26]:
		idx = 27
	case msg, ok = <-r.chs[27]:
		idx = 28
	case msg, ok = <-r.chs[28]:
		idx = 29
	case msg, ok = <-r.chs[29]:
		idx = 30
	case msg, ok = <-r.chs[30]:
		idx = 31
	case msg, ok = <-r.chs[31]:
		idx = 32
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom48Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	case msg, ok = <-r.chs[15]:
		idx = 16
	case msg, ok = <-r.chs[16]:
		idx = 17
	case msg, ok = <-r.chs[17]:
		idx = 18
	case msg, ok = <-r.chs[18]:
		idx = 19
	case msg, ok = <-r.chs[19]:
		idx = 20
	case msg, ok = <-r.chs[20]:
		idx = 21
	case msg, ok = <-r.chs[21]:
		idx = 22
	case msg, ok = <-r.chs[22]:
		idx = 23
	case msg, ok = <-r.chs[23]:
		idx = 24
	case msg, ok = <-r.chs[24]:
		idx = 25
	case msg, ok = <-r.chs[25]:
		idx = 26
	case msg, ok = <-r.chs[26]:
		idx = 27
	case msg, ok = <-r.chs[27]:
		idx = 28
	case msg, ok = <-r.chs[28]:
		idx = 29
	case msg, ok = <-r.chs[29]:
		idx = 30
	case msg, ok = <-r.chs[30]:
		idx = 31
	case msg, ok = <-r.chs[31]:
		idx = 32
	case msg, ok = <-r.chs[32]:
		idx = 33
	case msg, ok = <-r.chs[33]:
		idx = 34
	case msg, ok = <-r.chs[34]:
		idx = 35
	case msg, ok = <-r.chs[35]:
		idx = 36
	case msg, ok = <-r.chs[36]:
		idx = 37
	case msg, ok = <-r.chs[37]:
		idx = 38
	case msg, ok = <-r.chs[38]:
		idx = 39
	case msg, ok = <-r.chs[39]:
		idx = 40
	case msg, ok = <-r.chs[40]:
		idx = 41
	case msg, ok = <-r.chs[41]:
		idx = 42
	case msg, ok = <-r.chs[42]:
		idx = 43
	case msg, ok = <-r.chs[43]:
		idx = 44
	case msg, ok = <-r.chs[44]:
		idx = 45
	case msg, ok = <-r.chs[45]:
		idx = 46
	case msg, ok = <-r.chs[46]:
		idx = 47
	case msg, ok = <-r.chs[47]:
		idx = 48
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom64Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	case msg, ok = <-r.chs[15]:
		idx = 16
	case msg, ok = <-r.chs[16]:
		idx = 17
	case msg, ok = <-r.chs[17]:
		idx = 18
	case msg, ok = <-r.chs[18]:
		idx = 19
	case msg, ok = <-r.chs[19]:
		idx = 20
	case msg, ok = <-r.chs[20]:
		idx = 21
	case msg, ok = <-r.chs[21]:
		idx = 22
	case msg, ok = <-r.chs[22]:
		idx = 23
	case msg, ok = <-r.chs[23]:
		idx = 24
	case msg, ok = <-r.chs[24]:
		idx = 25
	case msg, ok = <-r.chs[25]:
		idx = 26
	case msg, ok = <-r.chs[26]:
		idx = 27
	case msg, ok = <-r.chs[27]:
		idx = 28
	case msg, ok = <-r.chs[28]:
		idx = 29
	case msg, ok = <-r.chs[29]:
		idx = 30
	case msg, ok = <-r.chs[30]:
		idx = 31
	case msg, ok = <-r.chs[31]:
		idx = 32
	case msg, ok = <-r.chs[32]:
		idx = 33
	case msg, ok = <-r.chs[33]:
		idx = 34
	case msg, ok = <-r.chs[34]:
		idx = 35
	case msg, ok = <-r.chs[35]:
		idx = 36
	case msg, ok = <-r.chs[36]:
		idx = 37
	case msg, ok = <-r.chs[37]:
		idx = 38
	case msg, ok = <-r.chs[38]:
		idx = 39
	case msg, ok = <-r.chs[39]:
		idx = 40
	case msg, ok = <-r.chs[40]:
		idx = 41
	case msg, ok = <-r.chs[41]:
		idx = 42
	case msg, ok = <-r.chs[42]:
		idx = 43
	case msg, ok = <-r.chs[43]:
		idx = 44
	case msg, ok = <-r.chs[44]:
		idx = 45
	case msg, ok = <-r.chs[45]:
		idx = 46
	case msg, ok = <-r.chs[46]:
		idx = 47
	case msg, ok = <-r.chs[47]:
		idx = 48
	case msg, ok = <-r.chs[48]:
		idx = 49
	case msg, ok = <-r.chs[49]:
		idx = 50
	case msg, ok = <-r.chs[50]:
		idx = 51
	case msg, ok = <-r.chs[51]:
		idx = 52
	case msg, ok = <-r.chs[52]:
		idx = 53
	case msg, ok = <-r.chs[53]:
		idx = 54
	case msg, ok = <-r.chs[54]:
		idx = 55
	case msg, ok = <-r.chs[55]:
		idx = 56
	case msg, ok = <-r.chs[56]:
		idx = 57
	case msg, ok = <-r.chs[57]:
		idx = 58
	case msg, ok = <-r.chs[58]:
		idx = 59
	case msg, ok = <-r.chs[59]:
		idx = 60
	case msg, ok = <-r.chs[60]:
		idx = 61
	case msg, ok = <-r.chs[61]:
		idx = 62
	case msg, ok = <-r.chs[62]:
		idx = 63
	case msg, ok = <-r.chs[63]:
		idx = 64
	}
	return idx, msg, ok
}

func (r *ReceiverOperator) selectFrom80Reg() (int, *process.RegisterMessage, bool) {
	idx := 0
	ok := true
	var msg *process.RegisterMessage
	select {
	case <-r.proc.Ctx.Done():
		return 0, nil, true
	case msg, ok = <-r.chs[0]:
		idx = 1
	case msg, ok = <-r.chs[1]:
		idx = 2
	case msg, ok = <-r.chs[2]:
		idx = 3
	case msg, ok = <-r.chs[3]:
		idx = 4
	case msg, ok = <-r.chs[4]:
		idx = 5
	case msg, ok = <-r.chs[5]:
		idx = 6
	case msg, ok = <-r.chs[6]:
		idx = 7
	case msg, ok = <-r.chs[7]:
		idx = 8
	case msg, ok = <-r.chs[8]:
		idx = 9
	case msg, ok = <-r.chs[9]:
		idx = 10
	case msg, ok = <-r.chs[10]:
		idx = 11
	case msg, ok = <-r.chs[11]:
		idx = 12
	case msg, ok = <-r.chs[12]:
		idx = 13
	case msg, ok = <-r.chs[13]:
		idx = 14
	case msg, ok = <-r.chs[14]:
		idx = 15
	case msg, ok = <-r.chs[15]:
		idx = 16
	case msg, ok = <-r.chs[16]:
		idx = 17
	case msg, ok = <-r.chs[17]:
		idx = 18
	case msg, ok = <-r.chs[18]:
		idx = 19
	case msg, ok = <-r.chs[19]:
		idx = 20
	case msg, ok = <-r.chs[20]:
		idx = 21
	case msg, ok = <-r.chs[21]:
		idx = 22
	case msg, ok = <-r.chs[22]:
		idx = 23
	case msg, ok = <-r.chs[23]:
		idx = 24
	case msg, ok = <-r.chs[24]:
		idx = 25
	case msg, ok = <-r.chs[25]:
		idx = 26
	case msg, ok = <-r.chs[26]:
		idx = 27
	case msg, ok = <-r.chs[27]:
		idx = 28
	case msg, ok = <-r.chs[28]:
		idx = 29
	case msg, ok = <-r.chs[29]:
		idx = 30
	case msg, ok = <-r.chs[30]:
		idx = 31
	case msg, ok = <-r.chs[31]:
		idx = 32
	case msg, ok = <-r.chs[32]:
		idx = 33
	case msg, ok = <-r.chs[33]:
		idx = 34
	case msg, ok = <-r.chs[34]:
		idx = 35
	case msg, ok = <-r.chs[35]:
		idx = 36
	case msg, ok = <-r.chs[36]:
		idx = 37
	case msg, ok = <-r.chs[37]:
		idx = 38
	case msg, ok = <-r.chs[38]:
		idx = 39
	case msg, ok = <-r.chs[39]:
		idx = 40
	case msg, ok = <-r.chs[40]:
		idx = 41
	case msg, ok = <-r.chs[41]:
		idx = 42
	case msg, ok = <-r.chs[42]:
		idx = 43
	case msg, ok = <-r.chs[43]:
		idx = 44
	case msg, ok = <-r.chs[44]:
		idx = 45
	case msg, ok = <-r.chs[45]:
		idx = 46
	case msg, ok = <-r.chs[46]:
		idx = 47
	case msg, ok = <-r.chs[47]:
		idx = 48
	case msg, ok = <-r.chs[48]:
		idx = 49
	case msg, ok = <-r.chs[49]:
		idx = 50
	case msg, ok = <-r.chs[50]:
		idx = 51
	case msg, ok = <-r.chs[51]:
		idx = 52
	case msg, ok = <-r.chs[52]:
		idx = 53
	case msg, ok = <-r.chs[53]:
		idx = 54
	case msg, ok = <-r.chs[54]:
		idx = 55
	case msg, ok = <-r.chs[55]:
		idx = 56
	case msg, ok = <-r.chs[56]:
		idx = 57
	case msg, ok = <-r.chs[57]:
		idx = 58
	case msg, ok = <-r.chs[58]:
		idx = 59
	case msg, ok = <-r.chs[59]:
		idx = 60
	case msg, ok = <-r.chs[60]:
		idx = 61
	case msg, ok = <-r.chs[61]:
		idx = 62
	case msg, ok = <-r.chs[62]:
		idx = 63
	case msg, ok = <-r.chs[63]:
		idx = 64
	case msg, ok = <-r.chs[64]:
		idx = 65
	case msg, ok = <-r.chs[65]:
		idx = 66
	case msg, ok = <-r.chs[66]:
		idx = 67
	case msg, ok = <-r.chs[67]:
		idx = 68
	case msg, ok = <-r.chs[68]:
		idx = 69
	case msg, ok = <-r.chs[69]:
		idx = 70
	case msg, ok = <-r.chs[70]:
		idx = 71
	case msg, ok = <-r.chs[71]:
		idx = 72
	case msg, ok = <-r.chs[72]:
		idx = 73
	case msg, ok = <-r.chs[73]:
		idx = 74
	case msg, ok = <-r.chs[74]:
		idx = 75
	case msg, ok = <-r.chs[75]:
		idx = 76
	case msg, ok = <-r.chs[76]:
		idx = 77
	case msg, ok = <-r.chs[77]:
		idx = 78
	case msg, ok = <-r.chs[78]:
		idx = 79
	case msg, ok = <-r.chs[79]:
		idx = 80
	}
	return idx, msg, ok
}
