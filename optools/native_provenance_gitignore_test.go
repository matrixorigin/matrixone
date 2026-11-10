// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The thirdparties Makefile extracts and compiles inside scratch trees that
// live only while a native build runs (see the header comment in
// thirdparties/Makefile). cgo/mo-native-provenance classifies native *inputs*
// with `git status --porcelain --untracked-files=all -- Makefile cgo
// thirdparties` at prepare/begin/record/verify time, so any scratch tree that
// is not ignored turns an in-flight, interrupted, or leftover native build
// into a bogus "native inputs changed" failure (#30235). Keep every scratch
// tree covered by .gitignore.
func TestNativeBuildScratchDirsStayIgnored(t *testing.T) {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	repo := strings.TrimSpace(string(root))

	for _, dir := range []string{
		"thirdparties/_croaring_build",
		"thirdparties/_fp16",
		"thirdparties/_jemalloc_build",
		"thirdparties/_simsimd",
		"thirdparties/_stringzilla",
		"thirdparties/_usearch_build",
		"thirdparties/_xxhash_build",
	} {
		probe := filepath.Join(repo, dir, "provenance-probe-file")
		rel, err := filepath.Rel(repo, probe)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		check := exec.Command("git", "check-ignore", rel)
		check.Dir = repo
		if out, err := check.CombinedOutput(); err != nil {
			t.Errorf("%s is not covered by git ignore rules: %v\n%s", dir, err, out)
		}
	}
}
