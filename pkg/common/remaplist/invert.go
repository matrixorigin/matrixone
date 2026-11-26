// Copyright 2026 Matrix Origin
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

package remaplist

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// InvertInto writes the inverse of r into dst: dst gets srcRows entries, and
// entry s is the output row that takes source row s, or -1 when no output
// row does. r must not repeat a source row, and every source row must be in
// [0, srcRows). dst is a List its caller owns (Init'ed with a pool); its
// previous rows and buckets are dropped. On error dst is left empty.
//
// This is the old row → new row map a merge hands to transfer: r lists, in
// output order, the rows that survived, and the dropped rows come back -1.
func (r *List) InvertInto(srcRows int, dst *List) error {
	if dst == r {
		return moerr.NewInternalErrorNoCtx("remaplist: InvertInto into itself")
	}
	if srcRows < 0 {
		return moerr.NewInternalErrorNoCtxf("remaplist: InvertInto(%d)", srcRows)
	}
	dst.rows = dst.rows[:0]
	dst.offs = dst.offs[:0]
	if err := dst.reserve(srcRows); err != nil {
		return err
	}
	inv := dst.rows[:srcRows]
	for i := range inv {
		inv[i] = -1
	}
	for out, src := range r.rows {
		if src < 0 || src >= int64(srcRows) {
			return moerr.NewInternalErrorNoCtxf("remaplist: InvertInto: source row %d out of %d", src, srcRows)
		}
		if inv[src] != -1 {
			return moerr.NewInternalErrorNoCtxf("remaplist: InvertInto: source row %d repeats", src)
		}
		inv[src] = int64(out)
	}
	dst.rows = inv
	return nil
}
