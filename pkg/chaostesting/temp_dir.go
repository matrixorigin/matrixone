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

import (
	"os"
)

type TempDir string

func (_ Def) TempDir(
	logger Logger,
) (
	dir TempDir,
	cleanup Cleanup,
) {
	d, err := os.MkdirTemp(os.TempDir(), "testcube-*")
	ce(err)
	dir = TempDir(d)
	return dir, func() {
		logger.Info("remove temp dir")
		os.RemoveAll(d)
	}
}
