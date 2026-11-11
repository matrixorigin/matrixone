// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mpool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestNativeAllocationFailureRollsBackAdmission(t *testing.T) {
	for _, failure := range []string{"oom", "unrelated native failure"} {
		t.Run(failure, func(t *testing.T) {
			mp, err := NewMPool("native-failure", 1<<20, NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() { DeleteMPool(mp) })
			mp.EnableDetailRecording()
			// Keep one live allocation: rollback must not retire its ownership.
			live, err := mp.Alloc(64, true)
			require.NoError(t, err)
			t.Cleanup(func() { mp.Free(live) })
			globalBytes := GlobalStats().NumCurrBytes.Load()
			globalBalance := GlobalStats().NumAlloc.Load() - GlobalStats().NumFree.Load()
			allocated := mp.AllocatedBytes()
			poolBytes := mp.CurrNB()
			poolBalance := mp.stats.NumAlloc.Load() - mp.stats.NumFree.Load()
			details := mp.details.reportJson()
			fault.EnableDomain(fault.DomainTest)
			t.Cleanup(func() { fault.DisableDomain(fault.DomainTest) })
			require.NoError(t, fault.AddFaultPointInDomain(context.Background(), fault.DomainTest,
				"simple_c_allocator_calloc", ":::", "echo", 128, failure, false))
			for range 3 {
				buf, err := mp.Alloc(128, true)
				require.Nil(t, buf)
				if failure == "oom" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
				} else {
					require.ErrorContains(t, err, failure)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
				}
				require.Equal(t, allocated, mp.AllocatedBytes())
				require.Equal(t, details, mp.details.reportJson(), "failed calloc has no detail owner")
				require.Equal(t, poolBytes, mp.CurrNB())
				require.Equal(t, globalBytes, GlobalStats().NumCurrBytes.Load())
				require.Equal(t, poolBalance, mp.stats.NumAlloc.Load()-mp.stats.NumFree.Load())
				require.Equal(t, globalBalance, GlobalStats().NumAlloc.Load()-GlobalStats().NumFree.Load())
			}
			fault.DisableDomain(fault.DomainTest)
			// A fresh attempt can consume the complete remaining cap.
			buf, err := mp.Alloc(int(mp.Cap()-poolBytes), true)
			require.NoError(t, err)
			t.Cleanup(func() { mp.Free(buf) })
			require.Equal(t, mp.Cap(), mp.CurrNB())
			live[0] = 42
			require.Equal(t, byte(42), live[0])
		})
	}
}
