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
		if sub.tp == subPathIdx && sub.idx == subPathIdxALL {
			p.flag |= pathFlagSingleStar
		}
	}
}

func (p Path) empty() bool {
	return len(p.paths) == 0
}

func (p Path) step() (sub subPath, newP Path) {
	sub = p.paths[0]
	newP.init(p.paths[1:])
	return
}

func (p Path) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, sub := range p.paths {
		switch sub.tp {
		case subPathIdx:
			if sub.idx == subPathIdxALL {
				s.WriteString("[*]")
			} else {
				s.WriteString("[")
				s.WriteString(strconv.Itoa(sub.idx))
				s.WriteString("]")
			}
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

func NewPathGenerator(path string) *pathGenerator {
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

func (pg pathGenerator) hasNext() bool {
	return pg.pos < len(pg.pathStr)
}

func (pg *pathGenerator) next() byte {
	ret := pg.pathStr[pg.pos]
	pg.pos++
	return ret
}
func (pg pathGenerator) front() byte {
	return pg.pathStr[pg.pos]
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
func (pg *pathGenerator) generateIndex(legs []subPath) ([]subPath, bool) {
	pg.next()
	pg.trimSpace()
	if !pg.hasNext() {
		return nil, false
	}
	if pg.front() == '*' {
		pg.next()
		legs = append(legs, subPath{
			tp:  subPathIdx,
			idx: subPathIdxALL,
		})
	} else {
		str, isEnd := pg.nextUtil(func(b byte) bool { // now only support non-negative integer
			return b >= '0' && b <= '9'
		})
		if isEnd {
			return nil, false
		}
		index, err := strconv.Atoi(str)
		if err != nil || index > math.MaxUint32 {
			return nil, false
		}
		legs = append(legs, subPath{
			tp:  subPathIdx,
			idx: index,
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
		if !json.Valid(string2Slice(key)) {
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
