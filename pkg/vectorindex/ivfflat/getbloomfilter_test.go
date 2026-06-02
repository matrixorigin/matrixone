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

package ivfflat

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

// setupKeyFilter builds an int64 key vector of n rows, broadcasts it as the
// build-side runtime bloom filter, and wires the matching RuntimeFilterSpec so
// getBloomFilter's WaitBloomFilter receives it.
func setupKeyFilter(t *testing.T, n int) *sqlexec.SqlProcess {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	mb := message.NewMessageBoard()
	proc.SetMessageBoard(mb)
	sqlproc := sqlexec.NewSqlProcess(proc)

	keyvec := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, vector.AppendFixed(keyvec, int64(i+1), false, m))
	}
	data, err := keyvec.MarshalBinary()
	require.NoError(t, err)

	tag := int32(42)
	message.SendMessage(message.RuntimeFilterMessage{
		Tag:  tag,
		Typ:  message.RuntimeFilter_BLOOMFILTER,
		Data: data,
	}, mb)
	sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
		{Tag: tag, UseBloomFilter: true},
	}
	return sqlproc
}

// runGetBloomFilter calls getBloomFilter with a timeout guard (WaitBloomFilter
// blocks on the message board).
func runGetBloomFilter(t *testing.T, sqlproc *sqlexec.SqlProcess) {
	idx := &IvfflatSearchIndex[float32]{}
	done := make(chan error, 1)
	go func() { done <- idx.getBloomFilter(sqlproc) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("getBloomFilter timed out")
	}
}

// Larger-than-threshold key set -> builds an exact doc_id pushdown filter.
func TestGetBloomFilter_DocFilter(t *testing.T) {
	sqlproc := setupKeyFilter(t, exactPkFilterThreshold+50)
	runGetBloomFilter(t, sqlproc)

	require.NotEmpty(t, sqlproc.IvfBloomFilter)
	require.Empty(t, sqlproc.ExactPkFilter)

	// The payload reconstructs into a usable membership filter.
	f, err := docfilter.New(sqlproc.IvfBloomFilter)
	require.NoError(t, err)
	require.True(t, f.Valid())
	f.Free()
}

// Small key set -> emits an exact "pk IN (...)" SQL filter instead.
func TestGetBloomFilter_ExactPk(t *testing.T) {
	sqlproc := setupKeyFilter(t, 5)
	runGetBloomFilter(t, sqlproc)

	require.NotEmpty(t, sqlproc.ExactPkFilter)
	require.Empty(t, sqlproc.IvfBloomFilter)
}
