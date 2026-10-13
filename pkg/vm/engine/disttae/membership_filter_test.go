// Copyright 2026 Matrix Origin
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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type recordingFilterAdmission struct {
	acquireCalls int
	releaseCalls int
	acquired     int64
	released     int64
}

func (a *recordingFilterAdmission) Acquire(bytes int64) (int64, bool) {
	a.acquireCalls++
	a.acquired += bytes
	return bytes, true
}

func (a *recordingFilterAdmission) Release(bytes int64) int64 {
	a.releaseCalls++
	a.released += bytes
	return bytes
}

func TestPrepareMembershipFilterOwnsOneAdmissionLease(t *testing.T) {
	payload := append([]byte{docfilter.TagSorted64}, make([]byte, 8)...)
	admission := new(recordingFilterAdmission)

	hint, filter, owned, err := prepareMembershipFilter(
		engine.FilterHint{MembershipFilterBytes: payload},
		admission,
	)
	require.NoError(t, err)
	require.True(t, owned)
	require.NotNil(t, filter)
	require.Same(t, filter, hint.BF)
	require.Empty(t, hint.MembershipFilterBytes)
	require.Equal(t, 1, admission.acquireCalls)
	require.Equal(t, int64(8), admission.acquired)

	readerShare := filter.Share()
	filter.Free()
	require.Zero(t, admission.releaseCalls)
	require.True(t, readerShare.Valid())

	readerShare.Free()
	require.Equal(t, 1, admission.releaseCalls)
	require.Equal(t, int64(8), admission.released)
}
