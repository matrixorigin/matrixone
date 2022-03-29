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

package lpad

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestLpadVarchar(t *testing.T) {
	//Test values
	// origins := []string{"hi", "hi","hishjajsa","hish&sa*(#jsa","hish&sa*(#jsa","hishjajsa"}

	origins := &types.Bytes{
		Data:    []byte("hihihishjajsahish&sa*(#jsahish&sa*(#jsahishjajsa"),
		Offsets: []uint32{0, 2, 4, 13, 26, 39},
		Lengths: []uint32{2, 2, 9, 13, 13, 9},
	}

	origins_int64 := []int64{4, 1, 7, 5, 17, 47}

	// origins_padd := []string{"??", "??", "??saso","?^^%$so","?^^%$so","^%so"}
	origins_padd := &types.Bytes{
		Data:    []byte("??????saso?^^%$so?^^%$so^%so"),
		Offsets: []uint32{0, 2, 4, 10, 17, 24},
		Lengths: []uint32{2, 2, 6, 7, 7, 4},
	}
	//Predefined Correct Values
	results := []string{"??hi", "h", "hishjaj", "hish&", "?^^%hish&sa*(#jsa", "^%so^%so^%so^%so^%so^%so^%so^%so^%so^%hishjajsa"}

	or := LpadVarchar(origins, origins_int64, origins_padd)

	for i, _ := range results {
		require.Equal(t, []byte(results[i]), or.Data[or.Offsets[i]:or.Offsets[i]+or.Lengths[i]])
	}
}
