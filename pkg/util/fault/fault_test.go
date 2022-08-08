// Copyright 2022 Matrix Origin
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

package fault

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCount(t *testing.T) {
	var ok bool
	var cnt int64

	Enable()
	AddFaultPoint("a", ":5::", "return", 0, "")
	AddFaultPoint("aa", ":::", "getcount", 0, "a")
	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(3), cnt)

	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(4), cnt)

	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(5), cnt)

	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)
	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(6), cnt)

	RemoveFaultPoint("a")
	RemoveFaultPoint("aa")

	AddFaultPoint("a", "3:8:2:", "return", 0, "")
	AddFaultPoint("aa", ":::", "getcount", 0, "a")
	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)
	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(1), cnt)

	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)

	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)
	cnt, _ = TriggerFault("aa")
	require.Equal(t, int64(3), cnt)

	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)
	cnt, _ = TriggerFault("aa")
	require.Equal(t, int64(4), cnt)

	// 5
	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)

	// 6
	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)

	// 7
	_, ok = TriggerFault("a")
	require.Equal(t, true, ok)

	//8
	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)

	//9
	_, ok = TriggerFault("a")
	require.Equal(t, false, ok)

	cnt, ok = TriggerFault("aa")
	require.Equal(t, true, ok)
	require.Equal(t, int64(9), cnt)
	Disable()
	Disable()
}

func wait(t *testing.T) {
	_, ok := TriggerFault("w")
	require.Equal(t, true, ok)
}

func TestWait(t *testing.T) {
	var ok bool
	var cnt int64

	Enable()

	AddFaultPoint("w", ":::", "wait", 0, "")
	AddFaultPoint("n1", ":::", "notify", 0, "w")
	AddFaultPoint("nall", ":::", "notifyall", 0, "w")
	AddFaultPoint("gc", ":::", "getcount", 0, "w")
	AddFaultPoint("gw", ":::", "getwaiters", 0, "w")
	AddFaultPoint("s", ":::", "sleep", 1, "w")

	for i := 0; i < 10; i++ {
		go wait(t)
	}

	TriggerFault("s")

	cnt, ok = TriggerFault("gc")
	require.Equal(t, true, ok)
	require.Equal(t, int64(10), cnt)

	cnt, ok = TriggerFault("gw")
	require.Equal(t, true, ok)
	require.Equal(t, int64(10), cnt)

	TriggerFault("n1")
	TriggerFault("s")

	cnt, ok = TriggerFault("gc")
	require.Equal(t, true, ok)
	require.Equal(t, int64(10), cnt)

	cnt, ok = TriggerFault("gw")
	require.Equal(t, true, ok)
	require.Equal(t, int64(9), cnt)

	TriggerFault("nall")
	TriggerFault("s")

	cnt, ok = TriggerFault("gc")
	require.Equal(t, true, ok)
	require.Equal(t, int64(10), cnt)

	cnt, ok = TriggerFault("gw")
	require.Equal(t, true, ok)
	require.Equal(t, int64(0), cnt)

	Disable()
}
