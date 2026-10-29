// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// NewRecursiveSearchPath returns the implicit JSON_SEARCH filter that walks
// every value in the document. The public path parser intentionally rejects a
// bare double-star token, so this package-owned constructor keeps the
// internal traversal contract explicit.
func NewRecursiveSearchPath() *Path {
	return &Path{paths: []subPath{{tp: subPathDoubleStar}}}
}

// SearchStringPaths returns the complete locations of string values selected
// by paths whose value satisfies match. Overlapping filters are deduplicated.
// The callback receives a view into the source document which is valid only
// during the call.
//
// A non-positive maxResults or maxBytes means that the corresponding traversal
// bound is disabled. JSON_SEARCH callers should supply both limits.
func (bj ByteJson) SearchStringPaths(
	ctx context.Context,
	paths []*Path,
	match func([]byte) (bool, error),
	stopAfterFirst bool,
	maxResults int,
	maxBytes int,
) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result := make([]string, 0, min(len(paths), 8))
	seen := make(map[string]struct{})
	totalBytes := 0

	var walk func(ByteJson, *Path, string) (bool, error)
	walk = func(current ByteJson, filter *Path, location string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if filter == nil {
			return false, moerr.NewInvalidInputNoCtx("JSON search path is nil")
		}
		if filter.empty() {
			if current.Type != TpCodeString {
				return false, nil
			}
			matched, err := match(current.GetString())
			if err != nil || !matched {
				return false, err
			}
			if _, ok := seen[location]; ok {
				return false, nil
			}
			if maxResults > 0 && len(result) >= maxResults {
				return false, moerr.NewInvalidInputNoCtx("JSON_SEARCH result exceeds the path-count limit")
			}
			if maxBytes > 0 && len(location) > maxBytes-totalBytes {
				return false, moerr.NewInvalidInputNoCtx("JSON_SEARCH result exceeds the byte limit")
			}
			seen[location] = struct{}{}
			result = append(result, location)
			totalBytes += len(location)
			return stopAfterFirst, nil
		}

		sub, remaining := filter.step()
		if sub.tp == subPathDoubleStar {
			done, err := walk(current, &remaining, location)
			if err != nil || done {
				return done, err
			}
			switch current.Type {
			case TpCodeObject:
				for i := 0; i < current.GetElemCnt(); i++ {
					childLocation := appendSearchKey(location, string(current.getObjectKey(i)))
					done, err = walk(current.GetObjectVal(i), filter, childLocation)
					if err != nil || done {
						return done, err
					}
				}
			case TpCodeArray:
				for i := 0; i < current.GetElemCnt(); i++ {
					childLocation := appendSearchIndex(location, i)
					done, err = walk(current.GetArrayElem(i), filter, childLocation)
					if err != nil || done {
						return done, err
					}
				}
			}
			return false, nil
		}

		switch current.Type {
		case TpCodeObject:
			switch sub.tp {
			case subPathIdx:
				idx, _, _ := sub.idx.genIndex(1)
				if idx == 0 {
					return walk(current, &remaining, appendSearchIndex(location, idx))
				}
			case subPathRange:
				if sub.iRange.matchesIndex(0, 1) {
					return walk(current, &remaining, appendSearchIndex(location, 0))
				}
			case subPathKey:
				child, exists := current.queryValByKeyExists([]byte(sub.key))
				if exists {
					return walk(child, &remaining, appendSearchKey(location, sub.key))
				}
			case subPathKeyWildcard:
				for i := 0; i < current.GetElemCnt(); i++ {
					done, err := walk(
						current.GetObjectVal(i),
						&remaining,
						appendSearchKey(location, string(current.getObjectKey(i))),
					)
					if err != nil || done {
						return done, err
					}
				}
			}

		case TpCodeArray:
			count := current.GetElemCnt()
			switch sub.tp {
			case subPathIdx:
				idx, _, last := sub.idx.genIndex(count)
				if last && idx < 0 || count <= idx {
					return false, nil
				}
				if idx == subPathIdxALL {
					for i := 0; i < count; i++ {
						done, err := walk(current.GetArrayElem(i), &remaining, appendSearchIndex(location, i))
						if err != nil || done {
							return done, err
						}
					}
					return false, nil
				}
				return walk(current.GetArrayElem(idx), &remaining, appendSearchIndex(location, idx))
			case subPathRange:
				if count == 0 {
					return false, nil
				}
				bounds := sub.iRange.genRange(count)
				if bounds[0] < 0 || bounds[1] < 0 {
					return false, nil
				}
				for i := bounds[0]; i <= bounds[1]; i++ {
					done, err := walk(current.GetArrayElem(i), &remaining, appendSearchIndex(location, i))
					if err != nil || done {
						return done, err
					}
				}
			}

		default:
			if sub.tp == subPathIdx {
				idx, _, _ := sub.idx.genIndex(1)
				if idx == 0 {
					return walk(current, &remaining, appendSearchIndex(location, idx))
				}
			}
			if sub.tp == subPathRange && sub.iRange.matchesIndex(0, 1) {
				return walk(current, &remaining, appendSearchIndex(location, 0))
			}
		}
		return false, nil
	}

	for _, path := range paths {
		if path == nil {
			return nil, moerr.NewInvalidInputNoCtx("JSON search path is nil")
		}
		done, err := walk(bj, path, "$")
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
	}
	return result, nil
}

func appendSearchKey(location, key string) string {
	if isIdentifier(key) {
		return location + "." + key
	}
	return location + "." + strconv.Quote(key)
}

func appendSearchIndex(location string, index int) string {
	return location + "[" + strconv.Itoa(index) + "]"
}
