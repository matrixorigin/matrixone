// Copyright 2022 Matrix Origin
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

package main

import (
	"fmt"
	"os"

	_version "github.com/matrixorigin/matrixone/pkg/version"
)

func maybePrintVersion() {
	if !*version {
		return
	}

	fmt.Println("MatrixOne build info:")
	fmt.Printf("  The golang version used to build this binary: %s\n", _version.GoVersion)
	fmt.Printf("  Git branch name: %s\n", _version.BranchName)
	fmt.Printf("  Git commit ID: %s\n", _version.CommitID)
	fmt.Printf("  Buildtime: %s\n", _version.BuildTime)
	fmt.Printf("  Version: %s\n", _version.Version)
	os.Exit(0)
}
