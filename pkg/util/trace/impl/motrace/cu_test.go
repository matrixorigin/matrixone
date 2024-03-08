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

package motrace

import (
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCalculateFloat64(t *testing.T) {
	type fields struct {
		dividend float64
		divisor  float64
	}

	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{
			name: "cu",
			fields: fields{
				dividend: 6.79e-24,
				divisor:  1.0026988039e-06,
			},
			want: 6.77172444366172e-18,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.dividend / tt.fields.divisor
			require.Equal(t, tt.want, got)
		})
	}
}

var dummyOBConfig = config.OBCUConfig{
	CUUnit:        1.0026988039e-06,
	CpuPrice:      7.43e-14,
	MemPrice:      6.79e-24,
	IoInPrice:     1e-06,
	IoOutPrice:    1e-06,
	TrafficPrice0: 8.94e-10,
	TrafficPrice1: 0,
	TrafficPrice2: 8.94e-10,
}

func TestCalculateCUMemBigNumber(t *testing.T) {
	type args struct {
		memByte    int64
		durationNS int64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "case1",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
			},
			//     177359275896974822700044.0
			want: 1.773592758969748e+23,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := float64(tt.args.memByte) * float64(tt.args.durationNS)
			assert.Equalf(t, tt.want, got, "float64 * float64: %v, %v", tt.args.memByte, tt.args.durationNS)
		})
	}
}

func TestCalculateCUMem(t *testing.T) {
	type args struct {
		memByte    int64
		durationNS int64
		cfg        *config.OBCUConfig
	}

	defaultCUConfig := config.NewOBCUConfig()

	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "500GiB * 300s",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
				cfg:        &dummyOBConfig,
			},
			// want: 1.201028143901687e+06,
			want: 1.2010281439016873e+06,
		},
		{
			name: "default_cfg",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
				cfg:        defaultCUConfig,
			},
			// want: 806598.2280355261,
			want: 806598.2280355261,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateCUMem(tt.args.memByte, tt.args.durationNS, tt.args.cfg)
			assert.Equalf(t, tt.want, got, "CalculateCUMem(%v, %v, %v)", tt.args.memByte, tt.args.durationNS, tt.args.cfg)
		})
	}
}

func TestCalculateCUMemDecimal(t *testing.T) {
	type args struct {
		memByte    int64
		durationNS int64
		cfg        *config.OBCUConfig
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "573,384,797,164(byte)*309(sec)",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
				cfg:        &dummyOBConfig,
			},
			want: 1.2010281439016873e+06,
		},
		{
			name: "5,733(G-byte)*86400(sec)",
			args: args{
				memByte:    5733847971640,
				durationNS: 86400e9,
				cfg:        &dummyOBConfig,
			},
			//    3,354,742,523.444667
			want: 3.354742523444667e+09,
		},
		{
			name: "128(GB)*7*86400(sec)",
			args: args{
				memByte:    128 << 30,
				durationNS: 7 * 86400e9,
				cfg:        &dummyOBConfig,
			},
			//  562,886,586.3021176
			want: 5.628865863021175e+08,
		},
		{
			name: "573384797164(byte)*1e9(ns)",
			args: args{
				memByte:    573384797164,
				durationNS: 1e9,
				cfg:        &dummyOBConfig,
			},
			//    3882.803846579476
			want: 3882.803846579476,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CalculateCUMemDecimal(tt.args.memByte, tt.args.durationNS, tt.args.cfg.MemPrice, tt.args.cfg.CUUnit)
			require.Nil(t, err)
			assert.Equalf(t, tt.want, got, "CalculateCUMemDecimal(%v, %v, %v, %v)", tt.args.memByte, tt.args.durationNS, tt.args.cfg.MemPrice, tt.args.cfg.CUUnit)
		})
	}
}

func TestCalculateDiff(t *testing.T) {
	type args struct {
		memByte    int64
		durationNS int64
		cfg        *config.OBCUConfig
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "case1",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
			},
			want: 1.773592758969748e+23,
		},
	}

	cfg := &dummyOBConfig
	func1 := func(bytes, ns, price, unit float64) float64 { return bytes * ns * price / unit }
	func2 := func(bytes, ns, price, unit float64) float64 { return bytes * price / unit * ns }
	func3 := func(bytes, ns, price, unit float64) float64 { return ns / unit * price * bytes }

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1 := func1(float64(tt.args.memByte), float64(tt.args.durationNS), cfg.MemPrice, cfg.CUUnit)
			got2 := func2(float64(tt.args.memByte), float64(tt.args.durationNS), cfg.MemPrice, cfg.CUUnit)
			got3 := func3(float64(tt.args.memByte), float64(tt.args.durationNS), cfg.MemPrice, cfg.CUUnit)
			t.Logf("got1 = %v", got1)
			t.Logf("got2 = %v", got2)
			t.Logf("got3 = %v", got3)
		})
	}
}
