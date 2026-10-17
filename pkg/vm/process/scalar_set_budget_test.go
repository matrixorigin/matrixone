// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestScalarSetExecutionAdmission(t *testing.T) {
	b := MustNewExecutionResourceBudget(1<<20, 1<<20)
	g, err := b.OpenGeneration(1)
	require.NoError(t, err)
	defer g.Close()
	first, err := g.AcquireScalarSetExecution()
	require.NoError(t, err)
	defer first.Close()
	second, err := g.AcquireScalarSetExecution()
	require.NoError(t, err)
	defer second.Close()
	_, err = g.AcquireScalarSetExecution()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrScalarSetResourceBudgetExceeded))
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
	require.NoError(t, first.ChargeCandidatePairs(ScalarSetCandidatePairLimit-1))
	require.NoError(t, first.ChargeCandidatePairs(1))
	require.True(t, moerr.IsMoErrCode(first.ChargeCandidatePairs(1), moerr.ErrScalarSetResourceBudgetExceeded))
	require.True(t, moerr.IsMoErrCode(first.ChargeCandidatePairs(math.MaxUint64), moerr.ErrScalarSetResourceBudgetExceeded))
	require.Equal(t, ScalarSetCandidatePairLimit, first.CandidatePairs())
	require.Zero(t, second.CandidatePairs())
	first.Close()
	first.Close()
	require.ErrorIs(t, first.ChargeCandidatePairs(0), ErrExecutionResourceClosed)
	next, err := g.AcquireScalarSetExecution()
	require.NoError(t, err)
	defer next.Close()
	require.Zero(t, next.CandidatePairs())
	g.Close()
	_, err = g.AcquireScalarSetExecution()
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
}

func TestScalarSetSharedCandidateCounter(t *testing.T) {
	b := MustNewExecutionResourceBudget(1<<20, 1<<20)
	g, err := b.OpenGeneration(1)
	require.NoError(t, err)
	defer g.Close()
	l, err := g.AcquireScalarSetExecution()
	require.NoError(t, err)
	defer l.Close()
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- l.ChargeCandidatePairs(ScalarSetCandidatePairLimit / workers) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, ScalarSetCandidatePairLimit, l.CandidatePairs())
	require.True(t, moerr.IsMoErrCode(l.ChargeCandidatePairs(1), moerr.ErrScalarSetResourceBudgetExceeded))
}
