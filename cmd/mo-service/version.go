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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

	logutil.Info("MatrixOne build info:")
	logutil.Infof("  The golang version used to build this binary: %s", GoVersion)
	logutil.Infof("  Git branch name: %s", BranchName)
	logutil.Infof("  Git commit ID: %s", CommitID)
	logutil.Infof("  Buildtime: %s", BuildTime)
	logutil.Infof("  Version: %s", Version)
	os.Exit(0)
}
