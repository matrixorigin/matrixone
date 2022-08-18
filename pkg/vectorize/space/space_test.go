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

package space

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFillSpacesUint8(t *testing.T) {
	cases := []uint8{0, 1, 2, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}

func TestFillSpacesUint32(t *testing.T) {
	cases := []uint32{0, 1, 2, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}

func TestFillSpacesInt16(t *testing.T) {
	cases := []int16{0, 1, 2, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}

func TestFillSpacesInt64(t *testing.T) {
	cases := []int64{0, 1, 2, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}

func TestFillSpacesFloat32(t *testing.T) {
	cases := []float32{0, 1.1, 1.5, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}

func TestFillSpacesFloat64(t *testing.T) {
	cases := []float64{0, 1.1, 1.5, 3}
	result := make([]string, len(cases))
	FillSpacesNumber(cases, result)
	for i, s := range result {
		require.Equal(t, int(cases[i]), len(s))
	}
}
