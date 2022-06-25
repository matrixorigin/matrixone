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

package db

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
)

var (
	// defaultDBPath            = "aoedb" // Unused
	moduleName = "DB"
	// defaultDBName            = "default" // Unused
	// emptyDBName              = "" // Unused
	// defaultTestBlockRows     = uint64(2000) // Unused
	// defaultTestSegmentBlocks = uint64(2) // Unused
)

// Unused
// func getTestPath(t *testing.T) string {
// 	return testutils.GetDefaultTestPath(moduleName, t)
// }

func initTestEnv(t *testing.T) string {
	testutils.RemoveDefaultTestPath(moduleName, t)
	return testutils.MakeDefaultTestPath(moduleName, t)
}
