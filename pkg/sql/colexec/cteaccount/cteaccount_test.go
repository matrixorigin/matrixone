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

package cteaccount

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestResolveQuotaBytesFallbacks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	cases := []struct {
		name    string
		resolve func(string, bool, bool) (interface{}, error)
		want    int64
	}{
		{
			name:    "nil_resolver_falls_back_to_default",
			resolve: nil,
			want:    DefaultMemQuotaBytes,
		},
		{
			name: "resolver_error_falls_back_to_default",
			resolve: func(string, bool, bool) (interface{}, error) {
				return nil, errors.New("var not found")
			},
			want: DefaultMemQuotaBytes,
		},
		{
			name: "unexpected_type_falls_back_to_default",
			resolve: func(string, bool, bool) (interface{}, error) {
				return "not-an-int", nil
			},
			want: DefaultMemQuotaBytes,
		},
		{
			name: "negative_falls_back_to_default",
			resolve: func(string, bool, bool) (interface{}, error) {
				return int64(-1), nil
			},
			want: DefaultMemQuotaBytes,
		},
		{
			name: "zero_means_disabled",
			resolve: func(string, bool, bool) (interface{}, error) {
				return int64(0), nil
			},
			want: 0,
		},
		{
			name: "positive_value_passes_through",
			resolve: func(string, bool, bool) (interface{}, error) {
				return int64(4096), nil
			},
			want: 4096,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc.SetResolveVariableFunc(tc.resolve)
			require.Equal(t, tc.want, ResolveQuotaBytes(proc))
		})
	}
}

func TestAccountQuotaDisabled(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	require.Equal(t, int64(0), a.QuotaBytes())
	require.True(t, a.Resolved())
	require.Equal(t, int64(bat.Size()), a.TotalBytes())
}

func TestAccountFallbackWhenResolverMissing(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(nil)

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	require.Equal(t, DefaultMemQuotaBytes, a.QuotaBytes())
	require.True(t, a.Resolved())
	require.Equal(t, int64(bat.Size()), a.TotalBytes())
}

func TestAccountQuotaExceededLeavesTotalUntouched(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(name string, isSystem, isGlobal bool) (interface{}, error) {
		if name == QuotaVarName {
			return int64(1), nil
		}
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	err := a.Account(proc, bat, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded), "want ErrCteMemoryQuotaExceeded, got %v", err)
	require.Equal(t, int64(0), a.TotalBytes(), "totalBytes must not advance when quota is exceeded")
}

func TestAccountDeltaShrinkReducesTotal(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	big := colexec.MakeMockBatchs(proc.Mp())
	defer big.Clean(proc.Mp())
	small := colexec.MakeMockBatchs(proc.Mp())
	defer small.Clean(proc.Mp())
	small.CleanOnlyData()
	require.Less(t, int64(small.Size()), int64(big.Size()), "small must be smaller than big")

	var a Accountant
	require.NoError(t, a.Account(proc, big, nil))
	bigSize := a.TotalBytes()
	require.Equal(t, int64(big.Size()), bigSize)

	require.NoError(t, a.Account(proc, small, big))
	require.Equal(t, int64(small.Size()), a.TotalBytes())
	require.Less(t, a.TotalBytes(), bigSize)
}

func TestAccountTreatsSentinelPrevAsZero(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	sentinel := colexec.MakeMockBatchs(proc.Mp())
	defer sentinel.Clean(proc.Mp())
	sentinel.SetLast()
	next := colexec.MakeMockBatchs(proc.Mp())
	defer next.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, next, sentinel))
	require.Equal(t, int64(next.Size()), a.TotalBytes(),
		"sentinel prev must not subtract from delta — Reset baseline excludes it")
}

func TestAccountSkipsSentinelNext(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	sentinel := colexec.MakeMockBatchs(proc.Mp())
	defer sentinel.Clean(proc.Mp())
	sentinel.SetLast()

	var a Accountant
	require.NoError(t, a.Account(proc, sentinel, nil))
	require.Equal(t, int64(0), a.TotalBytes(),
		"sentinel next must not be charged — Reset baseline excludes it, so charging would drift")
	require.False(t, a.Resolved(), "early-return must not consume the lazy resolution slot")
}

func TestAccountAtQuotaBoundarySucceeds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	exact := int64(bat.Size())
	proc.SetResolveVariableFunc(func(name string, isSystem, isGlobal bool) (interface{}, error) {
		if name == QuotaVarName {
			return exact, nil
		}
		return int64(0), nil
	})

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil),
		"projected==quota must succeed: enforcement uses '>' so the boundary is inclusive")
	require.Equal(t, exact, a.TotalBytes())
	require.Equal(t, exact, a.QuotaBytes())
}

func TestSetBaselineResetsResolution(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	require.True(t, a.Resolved())

	a.SetBaseline(123)
	require.Equal(t, int64(123), a.TotalBytes())
	require.False(t, a.Resolved(), "SetBaseline must force re-resolution on next Account call")
}

func TestReleaseSubtractsFromTotal(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	before := a.TotalBytes()
	require.Greater(t, before, int64(0))

	a.Release(bat)
	require.Equal(t, int64(0), a.TotalBytes(),
		"Release must subtract the full batch size, bringing total back to zero")
}

func TestReleaseNilOrSentinelIsNoop(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	before := a.TotalBytes()
	require.Greater(t, before, int64(0))

	// Release nil
	a.Release(nil)
	require.Equal(t, before, a.TotalBytes())

	// Release sentinel
	sentinel := colexec.MakeMockBatchs(proc.Mp())
	defer sentinel.Clean(proc.Mp())
	sentinel.SetLast()
	a.Release(sentinel)
	require.Equal(t, before, a.TotalBytes())
}

func TestReleaseUnderflowClamp(t *testing.T) {
	// If Release is called twice (or on an unaccounted batch), totalBytes must
	// not go negative — it undermines the quota enforcement.
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())

	var a Accountant
	require.NoError(t, a.Account(proc, bat, nil))
	require.Greater(t, a.TotalBytes(), int64(0))

	// First Release: should zero out
	a.Release(bat)
	require.Equal(t, int64(0), a.TotalBytes())

	// Second Release: must clamp to 0, not go negative
	a.Release(bat)
	require.Equal(t, int64(0), a.TotalBytes(),
		"Release must clamp totalBytes to zero, not go negative")
}
