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
	"strconv"
	"strings"
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
