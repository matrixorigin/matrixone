// Copyright 2021 Matrix Origin
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

package fz

import "os/exec"

func mount(target string, mountPoint string, fsType string, options string) error {
	output, err := exec.Command(
		"mount",
		"-t", "9p",
		"-o", options,
		target,
		mountPoint,
	).CombinedOutput()
	if err != nil {
		pt("%s\n", output)
		return err
	}
	return nil
}
