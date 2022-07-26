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
)

var (
	// GoVersion go version, setup by makefile
	GoVersion = ""
	// BranchName git branch, setup by makefile
	BranchName = ""
	// CommitID git commit id, setup by makefile
	CommitID = ""
	// BuildTime build time, setup by makefile
	BuildTime = ""
	// Version version, setup by makefile
	Version = ""
)

func maybePrintVersion() {
	if !*version {
		return
	}

	fmt.Println("MatrixOne build info:")
	fmt.Printf("  The golang version used to build this binary: %s\n", GoVersion)
	fmt.Printf("  Git branch name: %s\n", BranchName)
	fmt.Printf("  Git commit ID: %s\n", CommitID)
	fmt.Printf("  Buildtime: %s\n", BuildTime)
	fmt.Printf("  Version: %s\n", Version)
	os.Exit(0)
}
