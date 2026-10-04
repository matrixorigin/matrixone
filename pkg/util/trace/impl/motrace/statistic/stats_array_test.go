// Copyright 2023 Matrix Origin
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

package statistic

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
)

func TestNewStatsArray(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
			},
			wantString: []byte(`[1,2,3.000,4,5]`),
		},
		{
			name: "random",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString: []byte(`[1,65,6.000,78,3494]`),
		},
		{
			name: "random_with_0",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       0,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[1,65,0,7834,0]`),
		},
		{
			name: "scale_3",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       0.001,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[1,65,0.001,7834,0]`),
		},
		{
			name: "scale_0.00049",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       0.0001,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[1,65,0.000,7834,0]`),
		},
		{
			name: "scale_0.0005",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       0.0005,
				s3in:         7834,
				s3out:        0,
			},
			wantString: []byte(`[1,65,0.001,7834,0]`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStatsArrayV1().
				WithVersion(tt.field.version).
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out)
			got := s.ToJsonString()
			require.Equal(t, tt.wantString, got)
			require.Equal(t, tt.field.version, s.GetVersion())
			require.Equal(t, tt.field.timeConsumed, s.GetTimeConsumed())
			require.Equal(t, tt.field.memory, s.GetMemorySize())
			require.Equal(t, tt.field.s3in, s.GetS3IOInputCount())
			require.Equal(t, tt.field.s3out, s.GetS3IOOutputCount())
		})
	}
}

func TestStatsArray_Add(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
			},
			wantString: []byte(`[1,3,5.000,7,9]`),
		},
		{
			name: "random",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString: []byte(`[1,66,8.000,81,3498]`),
		},
		{
			name: "1/8192_io_input",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         float64(1) / 8192,
				s3out:        0,
			},
			wantString: []byte(`[1,3,5.000,3.000122,4]`),
		},
	}

	getInitStatsArray := func() *StatsArray {
		return NewStatsArrayV1().WithTimeConsumed(1).WithMemorySize(2).WithS3IOInputCount(3).WithS3IOOutputCount(4)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := getInitStatsArray()
			s := NewStatsArrayV1().
				WithVersion(tt.field.version).
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out)
			got := dst.Add(s).ToJsonString()
			require.Equal(t, tt.wantString, got)
		})
	}
}

func TestDecodeStatsArray(t *testing.T) {
	tests := []struct {
		name string
		json string
		ok   bool
	}{
		{name: "v0", json: `[0,1,2,3,4,5,1,6,7,8,9]`, ok: true},
		{name: "v1", json: `[1,1,2,3,4]`, ok: true},
		{name: "v2", json: `[2,1,2,3,4,5]`, ok: true},
		{name: "v3", json: `[3,1,2,3,4,5,1]`, ok: true},
		{name: "v4", json: `[4,1,2,3,4,5,1,6,7]`, ok: true},
		{name: "v5", json: `[5,1,2,3,4,5,1,6,7,8,9]`, ok: true},
		{name: "v6", json: `[6,1,2,3,4,5,1,6,7,8,9,128,10,11,12,13,14]`, ok: true},
		{name: "object", json: `{\"version\":5}`, ok: false},
		{name: "non-integral-version", json: `[5.5,1,2,3,4,5,1,6,7,8,9]`, ok: false},
		{name: "unsupported-version", json: `[7,1,2,3,4,5,1,6,7,8,9,128,10,11,12,13,14]`, ok: false},
		{name: "wrong-length", json: `[6,1,2,3,4,5,1,6,7,8,9]`, ok: false},
		{name: "non-finite", json: `[5,1,2,3,4,5,1,6,7,8,NaN]`, ok: false},
		{name: "null", json: `[5,1,2,3,4,5,1,6,7,8,null]`, ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeStatsArray([]byte(tt.json))
			if tt.ok {
				require.NoError(t, err)
				require.Equal(t, int(got.GetVersion()), int(got[StatsArrayIndexVersion]))
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestStatsArrayAddMixedV5V6IsCommutativeAndAssociative(t *testing.T) {
	v5a := NewStatsArray().WithVersion(StatsArrayVersion5).WithTimeConsumed(3).WithMemorySize(100).
		WithS3IOInputCount(4).WithS3IOOutputCount(5).WithOutTrafficBytes(6).
		WithConnType(ConnTypeExternal).WithOutPacketCount(7).WithCU(8).
		WithS3IOListCount(9).WithS3IODeleteCount(10)
	v5b := NewStatsArray().WithVersion(StatsArrayVersion5).WithTimeConsumed(11).WithMemorySize(200).
		WithS3IOInputCount(12).WithS3IOOutputCount(13).WithOutTrafficBytes(14).
		WithConnType(ConnTypeExternal).WithOutPacketCount(15).WithCU(16).
		WithS3IOListCount(17).WithS3IODeleteCount(18)
	v6 := NewStatsArrayV6().WithTimeConsumed(21).WithMemorySize(300).
		WithS3IOInputCount(22).WithS3IOOutputCount(23).WithOutTrafficBytes(24).
		WithConnType(ConnTypeExternal).WithOutPacketCount(25).WithCU(26).
		WithS3IOListCount(27).WithS3IODeleteCount(28).
		WithQualityFlags(0).WithS3ReadBytes(29).WithS3WriteBytes(30).
		WithSpillBytes(31).WithTotalWaitNS(32).WithAttemptCount(33)

	operands := []*StatsArray{v5a, v5b, v6}
	permutations := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	var want StatsArray
	for idx, permutation := range permutations {
		got := *operands[permutation[0]]
		got.Add(operands[permutation[1]]).Add(operands[permutation[2]])
		if idx == 0 {
			want = got
		} else {
			require.Equal(t, want, got, "permutation %v", permutation)
		}
	}
	require.Equal(t, float64(StatsArrayVersion6), want.GetVersion())
	require.Equal(t, float64(300), want.GetMemorySize())
	require.Equal(t, resource.QualityPartial|resource.QualityMissingMemoryDomain|resource.QualityAggregated, want.GetQualityFlags())
}

func TestStatsArrayAddPromotesEveryLegacyVersion(t *testing.T) {
	legacyJSON := map[int]string{
		StatsArrayVersion0: `[0,3,100,4,5,6,2,7,8,9,10]`,
		StatsArrayVersion1: `[1,3,100,4,5]`,
		StatsArrayVersion2: `[2,3,100,4,5,6]`,
		StatsArrayVersion3: `[3,3,100,4,5,6,2]`,
		StatsArrayVersion4: `[4,3,100,4,5,6,2,7,8]`,
		StatsArrayVersion5: `[5,3,100,4,5,6,2,7,8,9,10]`,
	}
	for version, encoded := range legacyJSON {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			legacy, err := DecodeStatsArray([]byte(encoded))
			require.NoError(t, err)
			fresh := NewStatsArrayV6().WithTimeConsumed(20).WithMemorySize(300).
				WithS3IOInputCount(21).WithS3IOOutputCount(22).
				WithConnType(ConnTypeExternal).WithQualityFlags(0)

			left := legacy
			left.Add(fresh)
			right := *fresh
			right.Add(&legacy)

			require.Equal(t, left, right)
			require.Equal(t, float64(StatsArrayVersion6), left.GetVersion())
			require.Equal(t, float64(23), left.GetTimeConsumed())
			require.Equal(t, float64(300), left.GetMemorySize())
			require.Equal(t, float64(25), left.GetS3IOInputCount())
			require.Equal(t, float64(27), left.GetS3IOOutputCount())
			require.Equal(t,
				resource.QualityPartial|resource.QualityMissingMemoryDomain|resource.QualityAggregated,
				left.GetQualityFlags())
		})
	}
}

func TestStatsArray_AddV2(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
		traffic      float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
				traffic:      6,
			},
			wantString: []byte(`[2,3,5.000,7,9,11]`),
		},
		{
			name: "random",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
				traffic:      456,
			},
			wantString: []byte(`[2,66,8.000,81,3498,461]`),
		},
	}

	getInitStatsArray := func() *StatsArray {
		return NewStatsArrayV2().WithTimeConsumed(1).WithMemorySize(2).WithS3IOInputCount(3).WithS3IOOutputCount(4).WithOutTrafficBytes(5)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := getInitStatsArray()
			s := NewStatsArrayV2().
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out).
				WithOutTrafficBytes(tt.field.traffic)
			got := dst.Add(s).ToJsonString()
			require.Equal(t, tt.wantString, got)
		})
	}
}

func TestStatsArray_AddV3(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
		traffic      float64
		connType     ConnType
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
				traffic:      6,
				connType:     ConnTypeInternal,
			},
			wantString: []byte(`[3,3,5.000,7,9,11,1]`),
		},
		{
			name: "random",
			field: field{
				version:      1,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
				traffic:      456,
				connType:     ConnTypeExternal,
			},
			wantString: []byte(`[3,66,8.000,81,3498,461,1]`),
		},
	}

	getInitStatsArray := func() *StatsArray {
		return NewStatsArrayV3().WithTimeConsumed(1).WithMemorySize(2).WithS3IOInputCount(3).WithS3IOOutputCount(4).WithOutTrafficBytes(5).WithConnType(ConnTypeInternal)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := getInitStatsArray()
			s := NewStatsArrayV3().
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out).
				WithOutTrafficBytes(tt.field.traffic).
				WithConnType(tt.field.connType)
			got := dst.Add(s).ToJsonString()
			require.Equal(t, tt.wantString, got)
		})
	}
}

func TestStatsArray_InitIfEmpty(t *testing.T) {
	tests := []struct {
		name string
		s    StatsArray
		want *StatsArray
	}{
		{
			name: "normal",
			s:    StatsArray{1, 2, 3, 4},
			want: &StatsArray{1, 2, 3, 4},
		},
		{
			name: "empty",
			s:    StatsArray{},
			want: &StatsArray{StatsArrayVersion},
		},
		{
			name: "valid",
			s:    StatsArray{0, 1},
			want: &StatsArray{0, 1, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.s.InitIfEmpty()
			require.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkStatsInfo(b *testing.B) {
	s := StatsInfo{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ParseStage.ParseStartTime = time.Now()
		s.ParseStage.ParseDuration = time.Since(s.ParseStage.ParseStartTime)
		s.PlanStart()
		s.PlanEnd()
		s.CompileStart()
		s.CompileEnd()
		s.ExecutionStart()
		s.ExecutionEnd()
	}
}

// BenchmarkStatsInfo_ToJsonString
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic
// cpu: Apple M1 Pro
// BenchmarkStatsInfo_ToJsonString
// BenchmarkStatsInfo_ToJsonString/v_check_value
// BenchmarkStatsInfo_ToJsonString/v_check_value-10         	1000000000	         0.0000026 ns/op
// BenchmarkStatsInfo_ToJsonString/v_no_check
// BenchmarkStatsInfo_ToJsonString/v_no_check-10            	1000000000	         0.0000017 ns/op
//
// SO, keep the check op, reduce the byte ToJsonString used.
func BenchmarkStatsInfo_ToJsonString(b *testing.B) {
	s := NewStatsArrayV4().
		WithTimeConsumed(1).
		WithMemorySize(123.45678).
		WithS3IOInputCount(float64(1) / 8192).
		WithS3IOOutputCount(123).
		WithOutTrafficBytes(48475).
		WithConnType(1).
		WithCU(234.759347)

	b.Run("v_check_value", func(bm *testing.B) {
		bm.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.ToJsonString()
		}
	})
	b.Run("v_no_check", func(bm *testing.B) {
		bm.ResetTimer()
		for i := 0; i < b.N; i++ {
			StatsArrayToJsonString_noCheck((*s)[:StatsArrayLengthV4])
		}
	})
}

func StatsArrayToJsonString_noCheck(arr []float64) []byte {
	// len([1,184467440737095516161,18446744073709551616,18446744073709551616,18446744073709551616]") = 88
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	for idx, v := range arr {
		if idx > 0 {
			buf = append(buf, ',')
		}
		if v == 0.0 {
			buf = append(buf, '0')
		} else if idx == StatsArrayIndexMemorySize {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForMemorySize, 64)
		} else if idx == StatsArrayIndexCU {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForCU, 64)
		} else if idx == StatsArrayIndexS3IOInputCount {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForIOInput, 64)
		} else {
			buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
		}
	}
	buf = append(buf, ']')
	return buf
}
