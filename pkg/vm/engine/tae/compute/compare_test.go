// Copyright 2021 Matrix Origin
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

package compute

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestComapreGeneric(t *testing.T) {
	defer testutils.AfterTest(t)()
	x := types.Decimal256{
		B0_63: 0, B64_127: 0,
		B128_191: 0, B192_255: 0,
	}
	y := types.Decimal256{
		B0_63: ^x.B0_63, B64_127: ^x.B64_127,
		B128_191: ^x.B128_191, B192_255: ^x.B192_255,
	}
	assert.True(t, CompareGeneric(x, y, types.T_decimal256.ToType()) == 1)

	t1 := types.TimestampToTS(timestamp.Timestamp{
		PhysicalTime: 100,
		LogicalTime:  10,
	})
	t2 := t1.Next()
	assert.True(t, CompareGeneric(t1, t2, types.T_TS.ToType()) == -1)
}
