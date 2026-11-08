// Copyright 2024 Matrix Origin
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

package external

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// jsonLinePending owns the bytes between CSV reads. Its scanner only decides
// when another full JSON parse is useful; bytejson decides validity.
type jsonLinePending struct {
	data                              []byte
	depth                             int
	inString, escaped, closed, sealed bool
}

func (p *jsonLinePending) append(fragment string, limit int64) error {
	n := int64(len(p.data))
	maxInt := int64(^uint(0) >> 1)
	if limit <= 0 || n > limit-1 || int64(len(fragment)) > limit-1-n || int64(len(fragment)) > maxInt-n {
		return moerr.NewInvalidInputNoCtxf("json record size exceeds max record size %d (buffered %d bytes, incoming %d bytes), tunable by cn config 'largest-entry-limit'", limit, n, len(fragment))
	}
	required := len(p.data) + len(fragment)
	if required > cap(p.data) {
		capacity := required
		maxCapacity := limit - 1
		if maxCapacity > maxInt {
			maxCapacity = maxInt
		}
		grown := maxCapacity
		if int64(cap(p.data)) <= maxCapacity/2 {
			grown = int64(cap(p.data)) * 2
		}
		if grown > int64(capacity) {
			capacity = int(grown)
		}
		data := make([]byte, len(p.data), capacity)
		copy(data, p.data)
		p.data = data
	}
	p.data = append(p.data, fragment...)
	if p.sealed {
		return nil
	}
	for i := 0; i < len(fragment); i++ {
		c := fragment[i]
		if p.inString {
			if p.escaped {
				p.escaped = false
			} else if c == '\\' {
				p.escaped = true
			} else if c == '"' {
				p.inString = false
			}
			continue
		}
		switch c {
		case '"':
			p.inString = true
		case '{', '[':
			p.depth++
		case '}', ']':
			p.depth--
			if p.depth == 0 {
				p.closed = true
				return nil
			}
		}
	}
	return nil
}
