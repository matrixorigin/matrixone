//go:build mo_m0

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

package m0admission

import (
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestBirthAdmissionExecChild(t *testing.T) {
	if os.Getenv("M0_ADMISSION_TEST_CHILD") != "1" {
		t.Skip("exec child only")
	}
	close, err := Initialize(os.Args[1:])
	require.NoError(t, err)
	defer close()
	require.True(t, Enabled())
	t.Log("BIRTH_EXEC_LOCK_VERIFIED")
}

func TestBirthAdmissionSurvivesExec(t *testing.T) {
	manifest, path, lock := admissionFixture(t)
	manifest.Args = []string{"-test.run=^TestBirthAdmissionExecChild$", "-test.v=true", "-test.count=1"}
	writeAdmissionJSON(t, path, manifest)
	child := exec.Command(os.Args[0], manifest.Args...)
	child.ExtraFiles = []*os.File{lock}
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, LockFDEnv+"=") {
			child.Env = append(child.Env, item)
		}
	}
	child.Env = append(child.Env, LockFDEnv+"=3", "M0_ADMISSION_TEST_CHILD=1")
	output, err := child.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "BIRTH_EXEC_LOCK_VERIFIED")
	require.False(t, Enabled(), "child admission must not enable the parent")
}
