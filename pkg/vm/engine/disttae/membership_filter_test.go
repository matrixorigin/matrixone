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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

type trackingMembershipFilter struct {
	shares    *[]*trackingMembershipFilter
	freeCalls int
}

func newTrackingMembershipFilter() *trackingMembershipFilter {
	shares := make([]*trackingMembershipFilter, 0, 2)
	return &trackingMembershipFilter{shares: &shares}
}

func (*trackingMembershipFilter) Test([]byte) bool { return true }

func (*trackingMembershipFilter) TestVector(
	*vector.Vector,
	func(bool, bool, int),
) []uint8 {
	return nil
}

func (f *trackingMembershipFilter) Valid() bool { return f.freeCalls == 0 }
func (*trackingMembershipFilter) Exact() bool   { return true }

func (f *trackingMembershipFilter) Free() {
	f.freeCalls++
}

func (f *trackingMembershipFilter) Share() docfilter.MembershipFilter {
	share := &trackingMembershipFilter{shares: f.shares}
	*f.shares = append(*f.shares, share)
	return share
}

type trackingMembershipReader struct {
	*mockReader
	closeCalls int
}

func (r *trackingMembershipReader) Close() error {
	r.closeCalls++
	return r.mockReader.Close()
}

func TestBuildReadersWithMembershipFilterClosesPartialConstruction(t *testing.T) {
	errNthReader := errors.New("nth reader failed")

	for _, test := range []struct {
		name       string
		failSource bool
	}{
		{name: "source fails before consuming current share", failSource: true},
		{name: "reader fails after consuming current share"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mainFilter := newTrackingMembershipFilter()
			var firstReader *trackingMembershipReader
			var failedSource *stubSnapshotDataSource

			readers, err := buildReadersWithMembershipFilter(
				nil,
				2,
				engine.FilterHint{BF: mainFilter},
				mainFilter,
				func(reader int) (engine.DataSource, error) {
					if test.failSource && reader == 1 {
						failedSource = new(stubSnapshotDataSource)
						return failedSource, errNthReader
					}
					return nil, nil
				},
				func(_ engine.DataSource, hint engine.FilterHint) (engine.Reader, error) {
					filter := hint.BF.(*trackingMembershipFilter)
					if !test.failSource && len(*mainFilter.shares) == 2 {
						// Match readutil.NewReader's contract: the current share is
						// consumed even when construction fails.
						filter.Free()
						return nil, errNthReader
					}
					firstReader = &trackingMembershipReader{
						mockReader: &mockReader{filter: filter},
					}
					return firstReader, nil
				},
			)
			require.ErrorIs(t, err, errNthReader)
			require.Nil(t, readers)
			require.NotNil(t, firstReader)
			require.Equal(t, 1, firstReader.closeCalls)
			require.Len(t, *mainFilter.shares, 2)
			require.Equal(t, 1, (*mainFilter.shares)[0].freeCalls)
			require.Equal(t, 1, (*mainFilter.shares)[1].freeCalls)
			require.Zero(t, mainFilter.freeCalls, "the outer builder still owns the main filter")
			if test.failSource {
				require.True(t, failedSource.closed)
			}
		})
	}
}
