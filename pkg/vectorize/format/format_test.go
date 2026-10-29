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

package format

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForamtENUS(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12,332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12,332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12,332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12,332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20,000,000.0000",
		},
		{
			name:   "negative_rounding",
			number: "-1.2350000",
			scale:  "2",
			want:   "-1.24",
		},
		{
			name:   "negative_grouped_rounding",
			number: "-1234.5678901",
			scale:  "2",
			want:   "-1,234.57",
		},
		{
			name:   "negative_without_rounding",
			number: "-1234.5612345",
			scale:  "2",
			want:   "-1,234.56",
		},
		{
			name:   "negative_integer_carry",
			number: "-999.9950000",
			scale:  "2",
			want:   "-1,000.00",
		},
		{
			name:   "positive_rounding_control",
			number: "1234.5678901",
			scale:  "2",
			want:   "1,234.57",
		},
		{
			name:   "negative_scientific_rounding",
			number: "-1.235e0",
			scale:  "2",
			want:   "-1.24",
		},
		{
			name:   "negative_without_integer_part",
			number: "-.567",
			scale:  "1",
			want:   "-0.6",
		},
		{
			name:   "negative_padding_control",
			number: "-1.2",
			scale:  "3",
			want:   "-1.200",
		},
		{
			name:   "negative_zero_scale_control",
			number: "-1.2",
			scale:  "0",
			want:   "-1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := formatENUS(c.number, c.scale)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtARSA(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20000000.0000",
		},
		{
			name:   "negative_rounding_without_grouping",
			number: "-1234.5678901",
			scale:  "2",
			want:   "-1234.57",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := formatARSA(c.number, c.scale)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtBEBY(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12.332,20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12.332,1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12.332,1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12.332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0,12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20.000.000,0000",
		},
		{
			name:   "negative_rounding_with_decimal_comma",
			number: "-1234.5678901",
			scale:  "2",
			want:   "-1.234,57",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := formatBEBY(c.number, c.scale)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtBGBG(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12 332,20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12 332,1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12 332,1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12 332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0,12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20 000 000,0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatBGBG(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtDECH(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12'332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12'332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12'332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12'332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20'000'000.0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatDECH(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}
