// Copyright 2026 Matrix Origin
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

package bytejson

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// PathIterator walks one value selected by a JSON path at a time.
//
// The iterator keeps traversal state instead of collecting selected values in
// a slice. Values returned by Next are views into the root document supplied
// to Reset or NewPathIterator and must not outlive that document. The stack
// grows with the active path/container depth; a wildcard's children are
// visited one at a time.
type PathIterator struct {
	stack   []pathMatchFrame
	initErr error
}

// PathMatchIterator is retained as a descriptive alias for callers that used
// the name from the initial Foundation design.
type PathMatchIterator = PathIterator

type pathMatchFrame struct {
	value ByteJson
	path  Path

	sub       subPath
	remaining Path
	phase     pathMatchPhase
	index     int
	end       int
}

type pathMatchPhase uint8

const (
	pathMatchEnter pathMatchPhase = iota
	pathMatchDoubleStarDescend
	pathMatchObjectWildcard
	pathMatchArrayWildcard
	pathMatchArrayRange
)

func (it *PathIterator) popFrame() {
	if len(it.stack) == 0 {
		return
	}
	last := len(it.stack) - 1
	it.stack[last] = pathMatchFrame{}
	it.stack = it.stack[:last]
}

// NewPathIterator returns an iterator for matches in root. A nil path is an
// invalid construction and is reported by the first call to Next; an empty
// (but non-nil) Path is the valid root path.
func NewPathIterator(root ByteJson, path *Path) *PathIterator {
	it := &PathIterator{}
	it.Reset(root, path)
	return it
}

// NewPathMatchIterator is an alias for NewPathIterator kept for compatibility
// with the original Foundation API name.
func NewPathMatchIterator(root ByteJson, path *Path) *PathIterator {
	return NewPathIterator(root, path)
}

// Reset reuses it for a new root/path pair. The path is treated as immutable
// for the lifetime of the iterator; ParseJsonPath returns such a value.
func (it *PathIterator) Reset(root ByteJson, path *Path) {
	if it == nil {
		return
	}
	// A reslice alone would leave old ByteJson views in the backing array and
	// keep the previous source document alive across Reset. Clear before reuse
	// so borrowed ownership ends at the reset boundary.
	clear(it.stack)
	it.stack = it.stack[:0]
	it.initErr = nil
	if path == nil {
		it.initErr = moerr.NewInvalidInputNoCtx("JSON path iterator requires a path")
		return
	}
	it.stack = append(it.stack, pathMatchFrame{value: root, path: *path})
}

// Close releases traversal state and all borrowed document views. Reset may
// be called after Close to reuse the iterator.
func (it *PathIterator) Close() {
	if it == nil {
		return
	}
	clear(it.stack)
	it.stack = nil
	it.initErr = nil
}

// Next returns the next path match. A JSON null value is returned with ok=true;
// no match is represented by ok=false. It is equivalent to NextContext with a
// context that cannot be cancelled.
func (it *PathIterator) Next() (value ByteJson, ok bool, err error) {
	return it.NextContext(context.Background())
}

// NextContext is the cancellation-aware form of Next. Cancellation is checked
// before every bounded traversal step. A cancelled call leaves the cursor in
// place, so a caller may retry with a live context; no goroutine or unbounded
// wait is introduced by the iterator.
func (it *PathIterator) NextContext(ctx context.Context) (value ByteJson, ok bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			// A manually assembled or corrupted ByteJson must fail closed. The
			// iterator is borrowed-view based, so retaining a partially walked
			// stack after a malformed offset would make a later retry unsafe.
			if it != nil {
				clear(it.stack)
				it.stack = nil
			}
			value, ok = ByteJson{}, false
			err = moerr.NewInvalidInputNoCtxf("invalid JSON document: %v", recovered)
		}
	}()
	if it == nil {
		return ByteJson{}, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if it.initErr != nil {
		return ByteJson{}, false, it.initErr
	}

	for len(it.stack) > 0 {
		if err := ctx.Err(); err != nil {
			return ByteJson{}, false, err
		}

		top := &it.stack[len(it.stack)-1]
		if top.path.empty() {
			value = top.value
			it.popFrame()
			return value, true, nil
		}

		switch top.phase {
		case pathMatchEnter:
			sub, remaining := top.path.step()
			top.sub = sub
			top.remaining = remaining
			top.phase = pathMatchDoubleStarDescend

			switch sub.tp {
			case subPathDoubleStar:
				// A double-star first yields the suffix at the current value,
				// matching ByteJson.query's recursive-descent order.
				it.stack = append(it.stack, pathMatchFrame{value: top.value, path: remaining})
				continue
			case subPathKey:
				if top.value.Type != TpCodeObject {
					it.popFrame()
					continue
				}
				child, exists := top.value.queryValByKeyExists([]byte(sub.key))
				if !exists {
					it.popFrame()
					continue
				}
				it.popFrame()
				it.stack = append(it.stack, pathMatchFrame{value: child, path: remaining})
				continue
			case subPathIdx:
				if top.value.Type == TpCodeObject {
					idx, _, _ := sub.idx.genIndex(1)
					if idx != 0 {
						it.popFrame()
						continue
					}
					// JSON path [0] autowraps an object/scalar as the value
					// itself, preserving the existing Query contract.
					value := top.value
					it.popFrame()
					it.stack = append(it.stack, pathMatchFrame{value: value, path: remaining})
					continue
				}
				if top.value.Type != TpCodeArray {
					idx, _, _ := sub.idx.genIndex(1)
					if idx != 0 {
						it.popFrame()
						continue
					}
					value := top.value
					it.popFrame()
					it.stack = append(it.stack, pathMatchFrame{value: value, path: remaining})
					continue
				}
				count := top.value.GetElemCnt()
				idx, _, last := sub.idx.genIndex(count)
				if (last && idx < 0) || count <= idx {
					it.popFrame()
					continue
				}
				if idx == subPathIdxALL {
					top.phase = pathMatchArrayWildcard
					top.index = 0
					top.end = count
					continue
				}
				child := top.value.GetArrayElem(idx)
				it.popFrame()
				it.stack = append(it.stack, pathMatchFrame{value: child, path: remaining})
				continue
			case subPathRange:
				if top.value.Type == TpCodeObject {
					if !sub.iRange.matchesIndex(0, 1) {
						it.popFrame()
						continue
					}
					value := top.value
					it.popFrame()
					it.stack = append(it.stack, pathMatchFrame{value: value, path: remaining})
					continue
				}
				if top.value.Type != TpCodeArray {
					if !sub.iRange.matchesIndex(0, 1) {
						it.popFrame()
						continue
					}
					value := top.value
					it.popFrame()
					it.stack = append(it.stack, pathMatchFrame{value: value, path: remaining})
					continue
				}
				rng := sub.iRange.genRange(top.value.GetElemCnt())
				start, end := rng[0], rng[1]
				if start < 0 || end < 0 {
					it.popFrame()
					continue
				}
				top.phase = pathMatchArrayRange
				top.index, top.end = start, end+1
				continue
			case subPathKeyWildcard:
				if top.value.Type != TpCodeObject {
					it.popFrame()
					continue
				}
				top.phase = pathMatchObjectWildcard
				top.index, top.end = 0, top.value.GetElemCnt()
				continue
			default:
				it.popFrame()
				continue
			}

		case pathMatchDoubleStarDescend:
			// The suffix frame above is visited first. This frame then walks
			// each child with the complete double-star path, one at a time.
			if top.sub.tp != subPathDoubleStar {
				it.popFrame()
				continue
			}
			switch top.value.Type {
			case TpCodeObject:
				if top.index >= top.value.GetElemCnt() {
					it.popFrame()
					continue
				}
				child := top.value.GetObjectVal(top.index)
				top.index++
				it.stack = append(it.stack, pathMatchFrame{value: child, path: top.path})
			case TpCodeArray:
				if top.index >= top.value.GetElemCnt() {
					it.popFrame()
					continue
				}
				child := top.value.GetArrayElem(top.index)
				top.index++
				it.stack = append(it.stack, pathMatchFrame{value: child, path: top.path})
			default:
				it.popFrame()
			}

		case pathMatchObjectWildcard:
			if top.index >= top.end {
				it.popFrame()
				continue
			}
			child := top.value.GetObjectVal(top.index)
			top.index++
			it.stack = append(it.stack, pathMatchFrame{value: child, path: top.remaining})

		case pathMatchArrayWildcard:
			if top.index >= top.end {
				it.popFrame()
				continue
			}
			child := top.value.GetArrayElem(top.index)
			top.index++
			it.stack = append(it.stack, pathMatchFrame{value: child, path: top.remaining})

		case pathMatchArrayRange:
			if top.index >= top.end {
				it.popFrame()
				continue
			}
			child := top.value.GetArrayElem(top.index)
			top.index++
			it.stack = append(it.stack, pathMatchFrame{value: child, path: top.remaining})
		}
	}

	return ByteJson{}, false, nil
}
