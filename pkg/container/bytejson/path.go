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

package bytejson

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/util"
)

func (p *Path) init(subs []subPath) {
	p.paths = subs
	for _, sub := range subs {
		if sub.tp == subPathDoubleStar {
			p.flag |= pathFlagDoubleStar
		}
		if sub.tp == subPathKey && sub.key == "*" {
			p.flag |= pathFlagSingleStar
		}
		if sub.tp == subPathIdx && sub.idx.num == subPathIdxALL && sub.idx.tp == numberIndices {
			p.flag |= pathFlagSingleStar
		}
	}
}

func (p *Path) empty() bool {
	return len(p.paths) == 0
}

func (p *Path) step() (sub subPath, newP Path) {
	sub = p.paths[0]
	newP.init(p.paths[1:])
	return
}

func (p *Path) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, sub := range p.paths {
		switch sub.tp {
		case subPathIdx:
			s.WriteString("[")
			sub.idx.toString(&s)
			s.WriteString("]")
		case subPathRange:
			s.WriteString("[")
			sub.iRange.toString(&s)
			s.WriteString("]")
		case subPathKey:
			s.WriteString(".")
			//TODO check here is ok
			s.WriteString(strconv.Quote(sub.key))
		case subPathDoubleStar:
			s.WriteString("**")
		}
	}
	return s.String()
}

func (pi subPathIndices) toString(s *strings.Builder) {
	switch pi.tp {
	case numberIndices:
		if pi.num == subPathIdxALL {
			s.WriteString("*")
		} else {
			s.WriteString(strconv.Itoa(pi.num))
		}
	case lastIndices:
		s.WriteString(lastKey)
		if pi.num > 0 {
			s.WriteString(" - ")
			s.WriteString(strconv.Itoa(pi.num))
		}
	default:
		panic("invalid index type")
	}
}
func (pe subPathRangeExpr) toString(s *strings.Builder) {
	pe.start.toString(s)
	s.WriteString(" to ")
	pe.end.toString(s)
}

func newPathGenerator(path string) *pathGenerator {
	return &pathGenerator{
		pathStr: path,
		pos:     0,
	}
}

func (pg *pathGenerator) trimSpace() {
	for ; pg.pos < len(pg.pathStr); pg.pos++ {
		if !unicode.IsSpace(rune(pg.pathStr[pg.pos])) {
			break
		}
	}
}

func (pg *pathGenerator) hasNext() bool {
	return pg.pos < len(pg.pathStr)
}

func (pg *pathGenerator) next() byte {
	ret := pg.pathStr[pg.pos]
	pg.pos++
	return ret
}
func (pg *pathGenerator) front() byte {
	return pg.pathStr[pg.pos]
}
func (pg *pathGenerator) tryNext(inc int) string {
	if pg.pos+inc > len(pg.pathStr) {
		return ""
	}
	return pg.pathStr[pg.pos : pg.pos+inc]
}
func (pg *pathGenerator) skip(inc int) {
	pg.pos += inc
}

func (pg *pathGenerator) nextUtil(f func(byte) bool) (string, bool) {
	start := pg.pos
	isEnd := true
	for ; pg.hasNext(); pg.next() {
		if !f(pg.front()) {
			isEnd = false
			break
		}
	}
	return pg.pathStr[start:pg.pos], isEnd
}

func (pg *pathGenerator) generateDoubleStar(legs []subPath) ([]subPath, bool) {
	pg.next()
	if !pg.hasNext() || pg.next() != '*' {
		return nil, false
	}
	if !pg.hasNext() || pg.front() == '*' { //check if it is ***
		return nil, false
	}

	legs = append(legs, subPath{
		tp: subPathDoubleStar,
	})
	return legs, true
}

func (pg *pathGenerator) tryIndices(rs *subPathIndices) bool {
	rs.num = 0
	if pg.tryNext(lastKeyLen) == lastKey {
		rs.tp = lastIndices
		pg.skip(lastKeyLen)
		pg.trimSpace()
		if !pg.hasNext() {
			return false
		}
		if pg.front() == '-' {
			pg.next()
			pg.trimSpace()
			if !pg.hasNext() {
				return false
			}
			if idx, ok := pg.tryNumberIndex(); ok {
				rs.num = idx
				return true
			}
			return false
		}
		return true
	}
	if idx, ok := pg.tryNumberIndex(); ok {
		rs.tp = numberIndices
		rs.num = idx
		return true
	}
	return false
}

func (pg *pathGenerator) tryNumberIndex() (int, bool) {
	str, isEnd := pg.nextUtil(func(b byte) bool { // now only support non-negative integer
		return b >= '0' && b <= '9'
	})
	if isEnd {
		return 0, false
	}
	index, err := strconv.Atoi(str)
	if err != nil || index > math.MaxUint32 {
		return 0, false
	}
	return index, true
}

func (pg *pathGenerator) generateIndex(legs []subPath) ([]subPath, bool) {
	pg.next()
	pg.trimSpace()
	if !pg.hasNext() {
		return nil, false
	}
	if pg.front() == '*' {
		pg.next()
		legs = append(legs, subPath{
			tp: subPathIdx,
			idx: &subPathIndices{
				tp:  numberIndices,
				num: subPathIdxALL,
			},
		})
		pg.trimSpace()
		if !pg.hasNext() || pg.next() != ']' {
			return nil, false
		}
		return legs, true
	}
	i1 := &subPathIndices{}
	ok := pg.tryIndices(i1)
	if !ok {
		return nil, false
	}
	if !pg.hasNext() {
		return nil, false
	}
	pg.trimSpace()
	if pg.tryNext(toKeyLen) == toKey {
		if pg.pathStr[pg.pos-1] != ' ' {
			return nil, false
		}
		pg.skip(toKeyLen)
		if !pg.hasNext() {
			return nil, false
		}
		if pg.front() != ' ' {
			return nil, false
		}
		pg.trimSpace()
		i2 := &subPathIndices{}
		ok = pg.tryIndices(i2)
		if !ok {
			return nil, false
		}
		if i1.tp == lastIndices && i2.tp == lastIndices && i1.num < i2.num {
			return nil, false
		}
		legs = append(legs, subPath{
			tp: subPathRange,
			iRange: &subPathRangeExpr{
				start: i1,
				end:   i2,
			},
		})
	} else {
		legs = append(legs, subPath{
			tp:  subPathIdx,
			idx: i1,
		})
	}

	pg.trimSpace()
	if !pg.hasNext() || pg.next() != ']' {
		return nil, false
	}
	return legs, true
}

func (pg *pathGenerator) generateKey(legs []subPath) ([]subPath, bool) {
	pg.next()
	pg.trimSpace()
	if !pg.hasNext() {
		return nil, false
	}
	if pg.front() == '*' {
		pg.next()
		legs = append(legs, subPath{
			tp:  subPathKey,
			key: "*",
		})
	} else {
		var quoted bool
		var key string
		if pg.front() == '"' {
			pg.next()
			str, isEnd := pg.nextUtil(func(b byte) bool {
				if b == '\\' {
					pg.next()
					return true
				}
				return b != '"'
			})
			if isEnd {
				return nil, false
			}
			pg.next()
			key = str
			quoted = true
		} else {
			key, _ = pg.nextUtil(func(b byte) bool {
				return !(unicode.IsSpace(rune(b)) || b == '.' || b == '[' || b == '*')
			})
		}
		key = "\"" + key + "\""
		if !json.Valid(util.UnsafeStringToBytes(key)) {
			return nil, false
		}
		key, err := strconv.Unquote(key)
		if err != nil {
			return nil, false
		}
		if !quoted && !isIdentifier(key) {
			return nil, false
		}
		legs = append(legs, subPath{
			tp:  subPathKey,
			key: key,
		})
	}
	return legs, true
}

// genIndex returns originVal,modifiedVal,ok
func (pi subPathIndices) genIndex(cnt int) (int, int, bool) {
	switch pi.tp {
	case numberIndices:
		if pi.num >= cnt {
			return pi.num, cnt - 1, false
		}
		return pi.num, pi.num, false
	case lastIndices:
		idx := cnt - pi.num - 1
		if idx < 0 {
			return idx, 0, true
		}
		return idx, idx, true
	}
	return subPathIdxErr, subPathIdxErr, false
}

func (pe subPathRangeExpr) genRange(cnt int) (ret [2]int) {
	orig1, mdf1, _ := pe.start.genIndex(cnt)
	orig2, mdf2, _ := pe.end.genIndex(cnt)
	if orig1 > orig2 {
		ret[0], ret[1] = subPathIdxErr, subPathIdxErr
		return
	}
	ret[0], ret[1] = mdf1, mdf2
	return
}
