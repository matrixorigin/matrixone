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

package version

// MySQLCompatibilityVersion is the fixed SQL dialect baseline. A configured
// server-version display prefix and the MatrixOne release do not change it.
const MySQLCompatibilityVersion = 80030

// MySQLCompatibilityVersionString is also used by the default handshake prefix.
const MySQLCompatibilityVersionString = "8.0.30"

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
