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

package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var dedupFunctions = map[types.T]any{
	types.T_int8:  dedupOrderedFunc[int8],
	types.T_int16: dedupOrderedFunc[int16],
	types.T_int32: dedupOrderedFunc[int32],
	types.T_int64: dedupOrderedFunc[int64],
}

func dedupOrderedFunc[T types.OrderedT](args ...any) func(T, bool, int) error {
	vec := args[0].(containers.Vector).GetDownstreamVector()
	vs := vector.MustFixedCol[T](vec)
	var mask *roaring.Bitmap
	var def *catalog.ColDef
	if args[1] != nil {
		mask = args[1].(*roaring.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	return func(v T, _ bool, row int) (err error) {
		// logutil.Infof("row=%d,v=%v", row, v)
		if _, existed := compute.GetOffsetOfOrdered2(
			vs,
			v,
			mask,
		); existed {
			entry := common.TypeStringValue(*vec.GetType(), any(v))
			return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
		}
		return
	}
}

func dedupClosure(
	vec containers.Vector,
	ts types.TS,
	mask *roaring.Bitmap,
	def *catalog.ColDef) func(any, bool, int) error {
	return func(v any, _ bool, _ int) (err error) {
		if _, existed := compute.GetOffsetByVal(vec, v, mask); existed {
			entry := common.TypeStringValue(vec.GetType(), v)
			return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
		}
		return nil
	}
}
