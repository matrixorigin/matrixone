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

package bootstrap

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_0"
)

var (
	handles = []VersionHandle{}
)

// All versions need create a upgrade handle in pkg/bootstrap/versions
// package. And register the upgrade logic into handles.
func init() {
	handles = append(handles, v1_2_0.Handler)
}

func getFinalVersionHandle() VersionHandle {
	return handles[len(handles)-1]
}

func getVersionHandle(version string) VersionHandle {
	for _, h := range handles {
		if h.Metadata().Version == version {
			return h
		}
	}
	panic("missing upgrade handle for version: " + version)
}
