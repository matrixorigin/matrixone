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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatENUS(c.number, c.scale)
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatARSA(c.number, c.scale)
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatBEBY(c.number, c.scale)
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
