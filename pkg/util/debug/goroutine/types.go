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
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

// Config leak check config
type Config struct {
	// EnableLeakCheck enable goroutine leak check
	EnableLeakCheck bool `toml:"enable-leak-check"`
	// SuspectLeakCount if goroutine count > suspect-leak-count, maybe leak. Default is 10000.
	SuspectLeakCount int `toml:"suspect-leak-count"`
	// CheckDuration check goroutine duration. Default is 1 hour.
	CheckDuration toml.Duration `toml:"check-duration"`
}

type Analyzer interface {
	ParseSystem() ([]Goroutine, error)
	Parse(stack []byte) []Goroutine
	GroupAnalyze([]Goroutine) AnalyzeResult
}

// AnalyzeResult analyze result
type AnalyzeResult struct {
	Goroutines        []Goroutine
	createGroups      [][]int
	firstMethodGroups [][][]int
}

// Goroutine goroutine
type Goroutine struct {
	ID             int
	State          string
	BlockedMinutes int
	rawState       string
	methods        []string
	files          []string
	realFuncLevel  int
}

func (g Goroutine) IsEmpty() bool {
	return g.ID == 0
}

func (g Goroutine) CreateBy() (string, string) {
	idx := len(g.methods) - 1 - g.realFuncLevel
	if idx == -1 && g.realFuncLevel > 0 {
		idx += g.realFuncLevel
	}
	return g.methods[idx], g.files[idx]
}

func (g Goroutine) Last() string {
	idx := len(g.methods) - 1
	return g.methods[idx]
}

func (g Goroutine) String() string {
	m, f := g.CreateBy()
	var buf bytes.Buffer
	buf.WriteString(g.rawState)
	buf.WriteString("\n")

	buf.WriteString(g.methods[0])
	buf.WriteString("\n")

	buf.WriteString("\t")
	buf.WriteString(g.files[0])
	buf.WriteString("\n")

	buf.WriteString(m)
	buf.WriteString("\t")
	buf.WriteString(f)
	buf.WriteString("\n")
	return buf.String()
}

func (g Goroutine) Has(value string) bool {
	if strings.Contains(strings.ToLower(g.rawState), value) {
		return true
	}

	for _, v := range g.methods {
		if strings.Contains(strings.ToLower(v), value) {
			return true
		}
	}
	for _, v := range g.files {
		if strings.Contains(strings.ToLower(v), value) {
			return true
		}
	}
	return false
}

func (res AnalyzeResult) GroupCount() int {
	return len(res.createGroups)
}

func (res AnalyzeResult) read(
	groupFunc func(int, Goroutine, int),
	groupDetailFunc func(int, int, Goroutine, int)) {
	for i, values := range res.createGroups {
		g := res.Goroutines[values[0]]
		groupFunc(i, g, len(values))

		for j, groups := range res.firstMethodGroups[i] {
			g := res.Goroutines[groups[0]]
			groupDetailFunc(i, j, g, len(groups))
		}
	}
}

func (res AnalyzeResult) Display(
	top int,
	filter func(i, j int) (bool, bool)) string {
	if top > len(res.createGroups) {
		top = len(res.createGroups)
	}

	var buf bytes.Buffer
	indent := 0
	write(
		fmt.Sprintf("Total goroutines: %d, display top %d", len(res.Goroutines), top),
		indent,
		&buf)
	for i, values := range res.createGroups[:top] {
		indent = 0
		g := res.Goroutines[values[0]]
		m, f := g.CreateBy()
		write(m, indent, &buf)
		write(f, indent+1, &buf)
		write(fmt.Sprintf("Count: %d", len(values)), indent, &buf)
		indent = 1
		for j, groups := range res.firstMethodGroups[i] {
			skip, printAllStack := filter(i, j)
			if skip {
				write("skipped", indent, &buf)
				continue
			}

			g := res.Goroutines[groups[0]]
			write(fmt.Sprintf("%s, %d minutes", g.State, g.BlockedMinutes), indent, &buf)

			if printAllStack {
				for k := range g.files {
					write(g.methods[k], indent, &buf)
					write(g.files[k], indent+1, &buf)
				}
			} else {
				write(g.methods[0], indent, &buf)
				write(g.files[0], indent+1, &buf)
			}

			write(fmt.Sprintf("Count: %d", len(groups)), indent, &buf)
		}
		write("\n", indent, &buf)
	}
	return buf.String()
}

func (res AnalyzeResult) String() string {
	return res.Display(len(res.createGroups),
		func(i, j int) (bool, bool) {
			return false, true
		})
}

func write(
	v string,
	indent int,
	buf *bytes.Buffer) {
	writeIndent := func() {
		for i := 0; i < indent; i++ {
			buf.WriteString("\t")
		}
	}
	writeIndent()
	buf.WriteString(v)
	buf.WriteString("\n")
}
