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

package model

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
)

func TestEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var zeroV types.TS
	view := NewBlockView(zeroV.Next().Next())
	colTypes := types.MockColTypes(14)
	rows := 64
	bat := containers.MockBatch(colTypes, rows, 3, nil)

	view.SetBatch(bat)
	defer view.Close()
	view.SetUpdates(1, roaring.BitmapOf(3), map[uint32]any{3: int16(7)})
	view.SetUpdates(13, roaring.BitmapOf(4), map[uint32]any{4: []byte("testEval")})

	_ = view.Eval(true)

	assert.Equal(t, any(int16(7)), view.GetColumnData(1).Get(3))
	assert.Equal(t, any([]byte("testEval")), view.GetColumnData(13).Get(4))
}
