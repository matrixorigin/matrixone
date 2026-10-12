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

package bytejson

// Depth returns the MySQL JSON depth of a valid ByteJSON document. A scalar,
// JSON null, and an empty container have depth one. The traversal keeps only
// one frame per open container, so its auxiliary space is O(depth) and it
// never materializes the document through encoding/json.
func (bj ByteJson) Depth() int {
	depth, _ := bj.DepthWithCheck(nil)
	return depth
}

// DepthWithCheck is Depth with an optional cancellation hook. The hook is
// called while walking the document and may stop a large traversal without
// making this container package depend on process.Context.
func (bj ByteJson) DepthWithCheck(check func() error) (int, error) {
	if bj.Type != TpCodeObject && bj.Type != TpCodeArray {
		if check != nil {
			if err := check(); err != nil {
				return 0, err
			}
		}
		return 1, nil
	}

	type frame struct {
		value ByteJson
		index int
		depth int
	}

	maxDepth := 1
	stack := []frame{{value: bj, depth: 1}}
	for len(stack) > 0 {
		if check != nil {
			if err := check(); err != nil {
				return 0, err
			}
		}
		last := &stack[len(stack)-1]
		if last.index >= last.value.GetElemCnt() {
			stack = stack[:len(stack)-1]
			continue
		}

		index := last.index
		last.index++
		var child ByteJson
		if last.value.Type == TpCodeObject {
			child = last.value.GetObjectVal(index)
		} else {
			child = last.value.GetArrayElem(index)
		}

		childDepth := last.depth + 1
		if childDepth > maxDepth {
			maxDepth = childDepth
		}
		if child.Type == TpCodeObject || child.Type == TpCodeArray {
			stack = append(stack, frame{value: child, depth: childDepth})
		}
	}
	return maxDepth, nil
}
