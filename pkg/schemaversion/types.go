// Copyright 2025 Matrix Origin
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

package schemaversion

var (
	FinalVersion       string
	FinalVersionOffset int32
)

type VersionInfo struct {
	FinalVersion          string // schema version of current binary code
	FinalVersionOffset    int32  // schema versionOffset of current binary code
	FinalVersionCompleted bool   // if the system has been upgraded to current binary version, it's true, (refer to mo_version)
	Cluster               struct {
		Version        string
		VersionOffset  int32
		IsFinalVersion bool
	}
	Account struct {
		Version       string
		VersionOffset int32
	}
}

func NewVersionInfo() *VersionInfo {
	return &VersionInfo{
		FinalVersion:       FinalVersion,
		FinalVersionOffset: FinalVersionOffset,
	}
}
