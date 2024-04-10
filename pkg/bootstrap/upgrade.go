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

// initUpgrade all versions need create a upgrade handle in pkg/bootstrap/versions
// package. And register the upgrade logic into handles.
func (s *service) initUpgrade() {
	s.handles = append(s.handles, v1_2_0.Handler)
	//s.handles = append(s.handles, v1_2_1.Handler)
}

func (s *service) getFinalVersionHandle() VersionHandle {
	return s.handles[len(s.handles)-1]
}

// GetFinalVersion Get mo final version
func (s *service) GetFinalVersion() string {
	return s.handles[len(s.handles)-1].Metadata().Version
}

func (s *service) GetFinalVersionOffset() int32 {
	return int32(s.handles[len(s.handles)-1].Metadata().VersionOffset)
}

func (s *service) getVersionHandle(version string) VersionHandle {
	for _, h := range s.handles {
		if h.Metadata().Version == version {
			return h
		}
	}
	panic("missing upgrade handle for version: " + version)
}
