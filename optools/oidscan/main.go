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

// Command oidscan reports the switch statements over type ids (types.T) in
// MatrixOne's pkg/ tree, as input for docs/design/20261126-type-vft.md.
//
// It is a report, not a check: TestTypeSwitchFamilies in pkg/container/types
// is the check.
//
//	cd optools/oidscan
//	go run . -root ../..                  # summary
//	go run . -root ../.. -loops           # switches inside a loop, TSV
//	go run . -root ../.. -loops -invariant # only those whose type is loop-invariant
//	go run . -root ../.. -list -dir pkg/container/vector   # every switch in one package, TSV
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
)

func main() {
	root := flag.String("root", ".", "repository root (the directory that holds pkg/)")
	loops := flag.Bool("loops", false, "list switches inside a loop instead of the summary")
	invariant := flag.Bool("invariant", false, "with -loops, list only switches whose subject does not change in the innermost loop")
	list := flag.Bool("list", false, "list every switch instead of the summary")
	dir := flag.String("dir", "", "only report switches in files directly in this slash-separated directory, relative to -root (e.g. pkg/container/vector)")
	flag.Parse()

	switches, err := Scan(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "oidscan:", err)
		os.Exit(1)
	}
	switches = underDir(switches, *dir)
	if *list {
		writeList(os.Stdout, switches)
		return
	}
	if *loops {
		writeLoops(os.Stdout, switches, *invariant)
		return
	}
	writeSummary(os.Stdout, switches)
}

// underDir keeps the switches in files directly in dir; "" keeps all.
func underDir(switches []Switch, dir string) []Switch {
	if dir == "" {
		return switches
	}
	dir = strings.TrimSuffix(dir, "/")
	var out []Switch
	for _, s := range switches {
		if path.Dir(s.File) == dir {
			out = append(out, s)
		}
	}
	return out
}

// writeList writes one TSV line per switch: file:line, function, oid count,
// "default" if it has a default branch, and "loop" if it is inside a loop.
func writeList(w io.Writer, switches []Switch) {
	for _, s := range switches {
		def, loop := "-", "-"
		if s.HasDefault {
			def = "default"
		}
		if len(s.Loops) > 0 {
			loop = "loop"
		}
		fmt.Fprintf(w, "%s:%d\t%s\t%d\t%s\t%s\n", s.File, s.Line, s.Func, s.Oids, def, loop)
	}
}

// writeLoops writes one TSV line per switch inside a loop:
// file:line, function, oid count, invariant, loops (innermost first).
func writeLoops(w io.Writer, switches []Switch, invariantOnly bool) {
	for _, s := range switches {
		if len(s.Loops) == 0 || (invariantOnly && !s.Invariant) {
			continue
		}
		inv := "-"
		if s.Invariant {
			inv = "invariant"
		}
		fmt.Fprintf(w, "%s:%d\t%s\t%d\t%s\t%s\n",
			s.File, s.Line, s.Func, s.Oids, inv, strings.Join(s.Loops, " | "))
	}
}

// bucket names the size class a switch with n type ids falls in.
func bucket(n int) string {
	switch {
	case n >= 20:
		return "20+"
	case n >= 10:
		return "10-19"
	case n >= 4:
		return "4-9"
	default:
		return "1-3"
	}
}

func writeSummary(w io.Writer, switches []Switch) {
	var withDefault, inLoop, invariant int
	buckets := map[string]int{}
	bigByPkg := map[string]int{}
	for _, s := range switches {
		if s.HasDefault {
			withDefault++
		}
		if len(s.Loops) > 0 {
			inLoop++
			if s.Invariant {
				invariant++
			}
		}
		b := bucket(s.Oids)
		buckets[b]++
		if b == "20+" {
			bigByPkg[path.Dir(s.File)]++
		}
	}
	fmt.Fprintf(w, "type switches:            %d\n", len(switches))
	fmt.Fprintf(w, "  with a default branch:  %d\n", withDefault)
	fmt.Fprintf(w, "  inside a loop:          %d\n", inLoop)
	fmt.Fprintf(w, "    loop-invariant type:  %d\n", invariant)
	fmt.Fprintln(w, "by number of type ids named:")
	for _, b := range []string{"20+", "10-19", "4-9", "1-3"} {
		fmt.Fprintf(w, "  %-6s %d\n", b, buckets[b])
	}
	fmt.Fprintln(w, "packages with the most 20+ switches:")
	pkgs := make([]string, 0, len(bigByPkg))
	for p := range bigByPkg {
		pkgs = append(pkgs, p)
	}
	sort.Slice(pkgs, func(i, j int) bool {
		if bigByPkg[pkgs[i]] != bigByPkg[pkgs[j]] {
			return bigByPkg[pkgs[i]] > bigByPkg[pkgs[j]]
		}
		return pkgs[i] < pkgs[j]
	})
	if len(pkgs) > 10 {
		pkgs = pkgs[:10]
	}
	for _, p := range pkgs {
		fmt.Fprintf(w, "  %4d  %s\n", bigByPkg[p], p)
	}
}
