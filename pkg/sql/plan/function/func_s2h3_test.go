// Copyright 2021 - 2024 Matrix Origin
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

package function

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
	h3 "github.com/uber/h3-go/v4"
)

func TestValidLatLng(t *testing.T) {
	// in range
	require.NoError(t, validLatLng(0, 0))
	require.NoError(t, validLatLng(90, 180))
	require.NoError(t, validLatLng(-90, -180))
	require.NoError(t, validLatLng(39.9087, 116.3975))
	// out of range
	require.Error(t, validLatLng(91, 0))
	require.Error(t, validLatLng(-91, 0))
	require.Error(t, validLatLng(0, 181))
	require.Error(t, validLatLng(0, -181))
	// non-finite must be rejected by the choke point itself (NaN compares false)
	require.Error(t, validLatLng(math.NaN(), 0))
	require.Error(t, validLatLng(0, math.NaN()))
	require.Error(t, validLatLng(math.Inf(1), 0))
	require.Error(t, validLatLng(0, math.Inf(-1)))
}

func TestUint64ListToJSONPreservesPrecision(t *testing.T) {
	// Values above 2^53 must round-trip exactly: a JSON float would corrupt them.
	ids := []uint64{0, 3886697461225194355, 18446744073709551615, 1 << 53, (1 << 53) + 1}
	bj, err := uint64ListToJSON(ids)
	require.NoError(t, err)
	s := bj.String()
	for _, id := range ids {
		require.Contains(t, s, strconv.FormatUint(id, 10), "id %d not preserved in %s", id, s)
	}
	// empty list encodes as an empty array
	bj, err = uint64ListToJSON(nil)
	require.NoError(t, err)
	require.Equal(t, "[]", strings.ReplaceAll(bj.String(), " ", ""))
}

func TestS2RequireValid(t *testing.T) {
	_, err := s2RequireValid(0)
	require.Error(t, err)

	leaf := uint64(s2.CellIDFromLatLng(s2.LatLngFromDegrees(39.9087, 116.3975)))
	cid, err := s2RequireValid(leaf)
	require.NoError(t, err)
	require.Equal(t, 30, cid.Level())

	// a coarser ancestor is also valid
	parent := uint64(cid.Parent(10))
	pcid, err := s2RequireValid(parent)
	require.NoError(t, err)
	require.Equal(t, 10, pcid.Level())
}

func TestH3RequireValid(t *testing.T) {
	_, err := h3RequireValid(0)
	require.Error(t, err)

	cell, err := h3.LatLngToCell(h3.NewLatLng(39.9087, 116.3975), 9)
	require.NoError(t, err)
	c, err := h3RequireValid(uint64(cell))
	require.NoError(t, err)
	require.Equal(t, 9, c.Resolution())
}
