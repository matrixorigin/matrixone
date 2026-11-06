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

package compile

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type m0CountedView struct{ released atomic.Int32 }

func (v *m0CountedView) Release() { v.released.Add(1) }

func TestM0StatementViewPreparedRelease(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	// A prepared Compile is cached and bypasses clear()/pool reclamation. Its
	// execution view still has to end on success, failure, or cancelled Run.
	c.isPrepare = true
	first := new(m0CountedView)
	c.statementView = first
	c.Release()
	require.Nil(t, c.statementView)
	require.Equal(t, int32(1), first.released.Load())
	c.Release()
	require.Equal(t, int32(1), first.released.Load())
	second := new(m0CountedView)
	c.statementView = second
	c.Release()
	require.Equal(t, int32(1), second.released.Load())
	require.Equal(t, int32(1), first.released.Load())
}
