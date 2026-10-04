// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUTNativePreparationOwner(t *testing.T) {
	helper, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ name, prepared, verifyStatus, want string }{
		{"direct script", "", "0", "build"},
		{"valid preparation", "cpu:release:0", "0", "verify"},
		{"stale preparation", "cpu:release:0", "1", "verify\nbuild"},
		{"invalid indication", "cpu:release:0:extra", "0", "build"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "cgo"), 0700); err != nil {
				t.Fatal(err)
			}
			verifier := "#!/bin/bash\necho verify >> \"$NATIVE_TEST_LOG\"\nexit \"$VERIFY_STATUS\"\n"
			if err := os.WriteFile(filepath.Join(dir, "cgo", "mo-native-provenance"), []byte(verifier), 0700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(dir, "calls")
			cmd := exec.Command("bash", "-c", `set -eu
source "$1"
go() { printf 'linux\namd64\nlinux\namd64\n'; }
make() { echo build >> "$NATIVE_TEST_LOG"; }
prepare_ut_native`, "bash", helper)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "UT_NATIVE_PREPARED="+tt.prepared, "VERIFY_STATUS="+tt.verifyStatus, "NATIVE_TEST_LOG="+log)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("prepare: %v\n%s", err, out)
			}
			calls, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(string(calls)) != tt.want {
				t.Fatalf("calls=%q want=%q", calls, tt.want)
			}
		})
	}
}
