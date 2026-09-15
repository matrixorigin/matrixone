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
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveMaxDigestLength(t *testing.T) {
	require.Equal(t, DefaultMaxDigestLength, ResolveMaxDigestLength(nil))
	require.Equal(t, DefaultMaxDigestLength, ResolveMaxDigestLength(&Process{}))

	for _, test := range []struct {
		name    string
		value   any
		err     error
		want    int
		wantSet bool
	}{
		{name: "int64 zero", value: int64(0), want: 0, wantSet: true},
		{name: "uint64 zero", value: uint64(0), want: 0, wantSet: true},
		{name: "int custom", value: 37, want: 37, wantSet: true},
		{name: "maximum", value: int64(MaximumMaxDigestLength), want: MaximumMaxDigestLength, wantSet: true},
		{name: "negative", value: int64(-1), want: DefaultMaxDigestLength},
		{name: "int64 above maximum", value: int64(MaximumMaxDigestLength + 1), want: DefaultMaxDigestLength},
		{name: "uint64 overflow", value: ^uint64(0), want: DefaultMaxDigestLength},
		{name: "wrong type", value: "37", want: DefaultMaxDigestLength},
		{name: "resolver error", err: errors.New("unavailable"), want: DefaultMaxDigestLength},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := &Process{Base: &BaseProcess{}}
			proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
				require.Equal(t, "max_digest_length", name)
				require.True(t, system)
				require.True(t, global)
				return test.value, test.err
			})
			require.Equal(t, test.want, ResolveMaxDigestLength(proc))
			require.Equal(t, test.wantSet, proc.Base.SessionInfo.MaxDigestLengthSet)
			if test.wantSet {
				require.Equal(t, int64(test.want), proc.Base.SessionInfo.MaxDigestLength)
			} else {
				require.Zero(t, proc.Base.SessionInfo.MaxDigestLength)
			}
		})
	}
}

func TestResolveMaxDigestLengthWithErrorRejectsInvalidResolverResults(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		resolverErr error
		want        int
		wantErr     string
	}{
		{name: "nil uses default", want: DefaultMaxDigestLength},
		{name: "zero", value: int64(0), want: 0},
		{name: "maximum", value: int64(MaximumMaxDigestLength), want: MaximumMaxDigestLength},
		{name: "resolver error", resolverErr: errors.New("unavailable"), wantErr: "resolve max_digest_length: unavailable"},
		{name: "negative", value: int64(-1), wantErr: "invalid max_digest_length value"},
		{name: "too large", value: int64(MaximumMaxDigestLength + 1), wantErr: "invalid max_digest_length value"},
		{name: "uint64 overflow", value: ^uint64(0), wantErr: "invalid max_digest_length value"},
		{name: "wrong type", value: "1024", wantErr: "invalid max_digest_length value"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := &Process{Base: &BaseProcess{}}
			proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
				require.Equal(t, "max_digest_length", name)
				require.True(t, system)
				require.True(t, global)
				return test.value, test.resolverErr
			})

			got, err := ResolveMaxDigestLengthWithError(proc)
			if test.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, test.wantErr)
				require.False(t, proc.Base.SessionInfo.MaxDigestLengthSet,
					"a failed resolution must not publish a misleading snapshot")
				require.Zero(t, proc.Base.SessionInfo.MaxDigestLength)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.True(t, proc.Base.SessionInfo.MaxDigestLengthSet)
			require.Equal(t, int64(test.want), proc.Base.SessionInfo.MaxDigestLength)
		})
	}

	malformedSnapshot := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{
		MaxDigestLength:    MaximumMaxDigestLength + 1,
		MaxDigestLengthSet: true,
	}}}
	_, err := ResolveMaxDigestLengthWithError(malformedSnapshot)
	require.ErrorContains(t, err, "invalid max_digest_length value")
	require.True(t, malformedSnapshot.Base.SessionInfo.MaxDigestLengthSet)
	require.Equal(t, int64(MaximumMaxDigestLength+1), malformedSnapshot.Base.SessionInfo.MaxDigestLength)
}

func TestResolveMaxDigestLengthForProcessInfoValidatesCapturedSnapshot(t *testing.T) {
	proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{
		MaxDigestLength:    MaximumMaxDigestLength + 1,
		MaxDigestLengthSet: true,
	}}}
	_, err := resolveMaxDigestLengthForProcessInfo(proc)
	require.ErrorContains(t, err, "invalid max_digest_length value")
	require.True(t, proc.Base.SessionInfo.MaxDigestLengthSet)
	require.Equal(t, int64(MaximumMaxDigestLength+1), proc.Base.SessionInfo.MaxDigestLength)
}

func TestResolveMaxDigestLengthForProcessInfoKeepsOrdinaryResolverFallback(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "max_digest_length" {
			return nil, errors.New("setting not implemented")
		}
		return nil, errors.New("unexpected variable")
	})

	got, err := resolveMaxDigestLengthForProcessInfo(proc)
	require.NoError(t, err)
	require.Equal(t, DefaultMaxDigestLength, got)
	require.False(t, proc.Base.SessionInfo.MaxDigestLengthSet,
		"a compatibility fallback must not mask a later strict resolution")
	require.Zero(t, proc.Base.SessionInfo.MaxDigestLength)
	_, err = ResolveMaxDigestLengthWithError(proc)
	require.ErrorContains(t, err, "resolve max_digest_length: setting not implemented")
}

func TestMaxDigestLengthSnapshotConcurrentChildren(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	var calls atomic.Int32
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		calls.Add(1)
		return int64(23), nil
	})

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	results := make(chan int, workers)
	for range workers {
		go func() {
			defer wg.Done()
			results <- ResolveMaxDigestLength(proc.NewNoContextChildProc(0))
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.Equal(t, 23, result)
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestMaxDigestLengthSnapshotGenerationAndChildren(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	var calls atomic.Int32
	configured := int64(0)
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		calls.Add(1)
		return configured, nil
	})

	require.Equal(t, 0, ResolveMaxDigestLength(proc))
	configured = 19
	require.Equal(t, 0, ResolveMaxDigestLength(proc), "one statement must keep its first snapshot")
	require.Equal(t, 0, ResolveMaxDigestLength(proc.NewNoContextChildProc(0)), "child processes share the statement snapshot")
	require.Equal(t, int32(1), calls.Load())

	proc.ResetMaxDigestLengthSnapshot()
	require.Equal(t, 19, ResolveMaxDigestLength(proc.NewNoContextChildProc(0)))
	require.Equal(t, int32(2), calls.Load())
}

func TestRemoteMaxDigestLengthSnapshotWithoutResolver(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     int64
		set       bool
		want      int
		wantSet   bool
		wantValue int64
	}{
		{name: "legacy absent zero", value: 0, set: false, want: DefaultMaxDigestLength, wantSet: true},
		{name: "explicit zero", value: 0, set: true, want: 0, wantSet: true},
		{name: "custom", value: 41, set: true, want: 41, wantSet: true},
		{name: "invalid negative", value: -1, set: true, want: DefaultMaxDigestLength, wantSet: true, wantValue: -1},
		{name: "invalid too large", value: MaximumMaxDigestLength + 1, set: true, want: DefaultMaxDigestLength, wantSet: true, wantValue: MaximumMaxDigestLength + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{
				MaxDigestLength: test.value, MaxDigestLengthSet: test.set,
			}}}
			require.Equal(t, test.want, ResolveMaxDigestLength(proc))
			require.Equal(t, test.wantSet, proc.Base.SessionInfo.MaxDigestLengthSet)
			if test.wantSet {
				wantValue := test.wantValue
				if wantValue == 0 {
					wantValue = int64(test.want)
				}
				require.Equal(t, wantValue, proc.Base.SessionInfo.MaxDigestLength)
			} else {
				require.Zero(t, proc.Base.SessionInfo.MaxDigestLength)
			}
		})
	}
}
