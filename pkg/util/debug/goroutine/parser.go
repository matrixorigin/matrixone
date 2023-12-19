// Copyright 2023 Matrix Origin
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

package goroutine

import (
	"bufio"
	"bytes"
	"strings"

	"github.com/fagongzi/util/format"
)

var (
	goroutineTokenLen      = len("goroutine")
	blockedMinutesTokenLen = len(" minutes]:")
)

func parse(data []byte) []Goroutine {
	var gs []Goroutine
	var g Goroutine
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "goroutine") {
			if !g.IsEmpty() {
				gs = append(gs, g)
			}
			g = Goroutine{}
			readGoroutineLine(line, &g)
		} else if line == "" {
			continue
		} else if strings.Contains(line, ":") {
			g.files = append(g.files, line)
		} else {
			g.methods = append(g.methods, line)
		}
	}
	if !g.IsEmpty() {
		gs = append(gs, g)
	}
	return gs
}

// format: goroutine 628 [IO wait, 8 minutes]:
func readGoroutineLine(
	line string,
	g *Goroutine) {
	g.rawState = line

	// goroutine 628 [IO wait, 8 minutes]:
	//           ^ offset
	offset := goroutineTokenLen + 1

	idx := strings.Index(line[offset:], " ")
	g.ID = format.MustParseStringInt(line[offset : offset+idx])

	// goroutine 628 [IO wait, 8 minutes]:
	//                ^ offset
	offset += idx + 2
	idx = strings.Index(line[offset:], ",")
	if idx == -1 {
		g.State = line[offset : len(line)-2]
		return
	}

	g.State = line[offset : offset+idx]

	// goroutine 628 [IO wait, 8 minutes]:
	//                         ^ offset
	offset += idx + 2
	g.BlockedMinutes = format.MustParseStringInt(line[offset : len(line)-blockedMinutesTokenLen])
}
