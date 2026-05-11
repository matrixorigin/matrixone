// Copyright 2024 Matrix Origin
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

package malloc

import (
	"bytes"
	"testing"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
)

func TestHeapProfilerWriteParsesAsPprof(t *testing.T) {
	profiler := NewProfiler[HeapSampleValues]()

	s := profiler.Sample(0, 1)
	s.Objects.Allocated.Add(2)
	s.Bytes.Allocated.Add(64)
	s.Objects.Inuse.Add(1)
	s.Bytes.Inuse.Add(32)

	var buf bytes.Buffer
	require.NoError(t, profiler.Write(&buf))

	p, err := profile.Parse(&buf)
	require.NoError(t, err)
	require.NoError(t, p.CheckValid())

	require.NotNil(t, p.PeriodType)
	require.Equal(t, "space", p.PeriodType.Type)
	require.Equal(t, "bytes", p.PeriodType.Unit)
	require.Equal(t, int64(512*1024), p.Period)
	require.NotZero(t, p.TimeNanos)

	require.Equal(t, "inuse_space", p.DefaultSampleType)
	require.Len(t, p.SampleType, 4)
	require.Equal(t, "alloc_objects", p.SampleType[0].Type)
	require.Equal(t, "alloc_space", p.SampleType[1].Type)
	require.Equal(t, "inuse_objects", p.SampleType[2].Type)
	require.Equal(t, "inuse_space", p.SampleType[3].Type)

	require.Len(t, p.Mapping, 1)
	require.True(t, p.Mapping[0].HasFunctions)
	for _, loc := range p.Location {
		require.NotNil(t, loc.Mapping)
		require.Equal(t, p.Mapping[0].ID, loc.Mapping.ID)
	}

	for _, s := range p.Sample {
		require.Len(t, s.Value, len(p.SampleType), "sample value length must match SampleType count")
	}
}
