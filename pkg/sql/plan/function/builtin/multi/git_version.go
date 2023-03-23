// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"runtime/debug"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var gitVersion = func() string {
	revision := "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return revision
	}
	for _, kv := range info.Settings {
		if kv.Key == "vcs.revision" {
			revision = kv.Value
		}
	}
	return revision
}()

func GitVersion(_ []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_varchar.ToType()
	return vector.NewConstBytes(rtyp, []byte(gitVersion), 1, proc.Mp()), nil
}
