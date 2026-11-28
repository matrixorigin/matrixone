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

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixture = `package p

import "types"

func notInLoop(t types.T) {
	switch t {
	case types.T_int8, types.T_int16:
	default:
	}
}

func notATypeSwitch(x int) {
	for i := 0; i < x; i++ {
		switch x {
		case 1:
		}
	}
}

// the type is fixed for the loop: invariant
func perRowInvariant(vec *Vector) {
	for row := 0; row < vec.Length(); row++ {
		switch vec.GetType().Oid {
		case types.T_date:
		case types.T_datetime:
		}
	}
}

// the switch subject changes per iteration: not invariant
func perColumn(vecs []*Vector) {
	for _, vec := range vecs {
		switch vec.GetType().Oid {
		case types.T_int32:
		}
	}
}

// assigned in the loop body: not invariant
func assignedInBody(vecs []*Vector) {
	var typ types.T
	for i := range vecs {
		typ = vecs[i].GetType().Oid
		switch typ {
		case types.T_int32:
		}
	}
}

// the switch's own init does not make it variant
func ownInit(vec *Vector, n int) {
	for i := 0; i < n; i++ {
		switch t := vec.GetType().Oid; t {
		case types.T_int32, types.T_int64:
		}
	}
}

// tagless: the case expressions decide
func tagless(vecs []*Vector) {
	for _, v := range vecs {
		switch {
		case v.GetType().Oid == types.T_json:
		}
	}
}

// a closure is a function boundary: the outer loop is not reported
func closure(vecs []*Vector) {
	for range vecs {
		f := func(t types.T) {
			switch t {
			case types.T_bool:
			}
		}
		_ = f
	}
}
`

func writeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "pkg", "p")
	for _, d := range []string{dir, filepath.Join(dir, "testdata")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		filepath.Join(dir, "p.go"):             fixture,
		filepath.Join(dir, "p_test.go"):        fixture,
		filepath.Join(dir, "testdata", "x.go"): fixture,
	}
	for name, src := range files {
		if err := os.WriteFile(name, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestScan(t *testing.T) {
	switches, err := Scan(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		oids       int
		hasDefault bool
		loops      int
		invariant  bool
	}
	wants := map[string]want{
		"notInLoop":       {2, true, 0, false},
		"perRowInvariant": {2, false, 1, true},
		"perColumn":       {1, false, 1, false},
		"assignedInBody":  {1, false, 1, false},
		"ownInit":         {2, false, 1, true},
		"tagless":         {1, false, 1, false},
		"closure":         {1, false, 0, false},
	}
	got := map[string]Switch{}
	for _, s := range switches {
		if s.File != "pkg/p/p.go" {
			t.Errorf("scanned %s: test and testdata files must be skipped", s.File)
		}
		got[s.Func] = s
	}
	if len(got) != len(wants) {
		t.Errorf("found switches in %d functions, want %d: %+v", len(got), len(wants), switches)
	}
	for fn, w := range wants {
		s, ok := got[fn]
		if !ok {
			t.Errorf("%s: switch not found", fn)
			continue
		}
		if s.Oids != w.oids || s.HasDefault != w.hasDefault || len(s.Loops) != w.loops || s.Invariant != w.invariant {
			t.Errorf("%s: got oids=%d default=%v loops=%v invariant=%v, want %+v",
				fn, s.Oids, s.HasDefault, s.Loops, s.Invariant, w)
		}
	}
}

func TestReports(t *testing.T) {
	switches, err := Scan(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer
	writeSummary(&b, switches)
	for _, line := range []string{
		"type switches:            7",
		"  with a default branch:  1",
		"  inside a loop:          5",
		"    loop-invariant type:  2",
		"  1-3    7",
	} {
		if !strings.Contains(b.String(), line+"\n") {
			t.Errorf("summary lacks %q:\n%s", line, b.String())
		}
	}

	b.Reset()
	writeLoops(&b, switches, true)
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 2 ||
		!strings.Contains(lines[0], "\tperRowInvariant\t2\tinvariant\tfor row < vec.Length()") ||
		!strings.Contains(lines[1], "\townInit\t2\tinvariant\tfor i < n") {
		t.Errorf("unexpected -loops -invariant output:\n%s", b.String())
	}
}

func TestListAndDir(t *testing.T) {
	switches, err := Scan(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := underDir(switches, "pkg/other"); len(got) != 0 {
		t.Errorf("underDir(pkg/other) kept %d switches", len(got))
	}
	kept := underDir(switches, "pkg/p/")
	if len(kept) != len(switches) {
		t.Errorf("underDir(pkg/p/) kept %d of %d switches", len(kept), len(switches))
	}
	var b bytes.Buffer
	writeList(&b, kept)
	if !strings.Contains(b.String(), "\tnotInLoop\t2\tdefault\t-\n") ||
		!strings.Contains(b.String(), "\tperRowInvariant\t2\t-\tloop\n") {
		t.Errorf("unexpected -list output:\n%s", b.String())
	}
}
